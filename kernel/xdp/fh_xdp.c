// SPDX-License-Identifier: GPL-2.0 OR BSD-2-Clause
#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/in.h>
#include <linux/ip.h>
#include <linux/ipv6.h>
#include <linux/tcp.h>
#include "include/bpf_helpers.h"

#ifndef __constant_htons
#define __constant_htons(x) ((__u16)__builtin_bswap16((__u16)(x)))
#endif

#ifndef ETH_P_8021Q
#define ETH_P_8021Q 0x8100
#endif
#ifndef ETH_P_8021AD
#define ETH_P_8021AD 0x88A8
#endif

struct fh_vlan_hdr {
    __be16 tci;
    __be16 encapsulated_proto;
};

struct rate_config {
    __u64 rate_per_second;
    __u64 burst;
};

struct rate_state {
    struct bpf_spin_lock lock;
    __u32 reserved;
    __u64 updated_ns;
    __u64 tokens;
};

struct ipv6_key {
    __u8 addr[16];
};

struct xdp_stats {
    __u64 passed;
    __u64 dropped;
    __u64 malformed;
    __u64 blocked;
    __u64 rate_limited;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 64);
    __type(key, __u16);
    __type(value, __u8);
} fh_ports SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 65536);
    __type(key, __u32);
    __type(value, __u8);
} fh_block_v4 SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 65536);
    __type(key, struct ipv6_key);
    __type(value, __u8);
} fh_block_v6 SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 262144);
    __uint(map_flags, BPF_F_NO_PREALLOC);
    __type(key, __u32);
    __type(value, struct rate_state);
} fh_rate_v4 SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct rate_config);
} fh_config SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct xdp_stats);
} fh_stats SEC(".maps");

static __always_inline struct xdp_stats *stats(void)
{
    __u32 zero = 0;
    return bpf_map_lookup_elem(&fh_stats, &zero);
}

static __always_inline int pass_packet(void)
{
    struct xdp_stats *s = stats();
    if (s)
        s->passed++;
    return XDP_PASS;
}

static __always_inline int drop_packet(int reason)
{
    struct xdp_stats *s = stats();
    if (s) {
        s->dropped++;
        if (reason == 1)
            s->malformed++;
        else if (reason == 2)
            s->blocked++;
        else if (reason == 3)
            s->rate_limited++;
    }
    return XDP_DROP;
}

static __always_inline int protected_port(__be16 destination)
{
    __u8 *enabled = bpf_map_lookup_elem(&fh_ports, &destination);
    return enabled && *enabled;
}

// Returns 1 to allow, 0 for an address block, and -1 for rate limiting.
static __always_inline int allow_ipv4(__u32 source)
{
    __u8 *blocked = bpf_map_lookup_elem(&fh_block_v4, &source);
    if (blocked && *blocked)
        return 0;

    __u32 zero = 0;
    struct rate_config *cfg = bpf_map_lookup_elem(&fh_config, &zero);
    if (!cfg || cfg->rate_per_second == 0)
        return 1;

    __u64 capacity = cfg->burst;
    if (capacity == 0)
        capacity = cfg->rate_per_second;
    if (capacity == 0)
        return 1;

    __u64 now = bpf_ktime_get_ns();
    struct rate_state *state = bpf_map_lookup_elem(&fh_rate_v4, &source);
    if (!state) {
        struct rate_state initial = {
            .updated_ns = now,
            .tokens = capacity > 0 ? capacity - 1 : 0,
        };
        bpf_map_update_elem(&fh_rate_v4, &source, &initial, BPF_ANY);
        return 1;
    }

    bpf_spin_lock(&state->lock);
    __u64 elapsed = now - state->updated_ns;
    __u64 whole = elapsed / 1000000000ULL;
    __u64 rem = elapsed % 1000000000ULL;
    __u64 refill = 0;
    // The userspace control plane caps rate and burst at 1e9. Saturate
    // long-idle entries before multiplication to keep arithmetic bounded.
    if (whole > 2)
        refill = capacity;
    else {
        refill = whole * cfg->rate_per_second;
        refill += (rem * cfg->rate_per_second) / 1000000000ULL;
    }
    __u64 tokens = state->tokens;
    if (refill > 0) {
        if (tokens + refill < tokens || tokens + refill > capacity)
            tokens = capacity;
        else
            tokens += refill;
        state->updated_ns = now;
    }
    if (tokens == 0) {
        bpf_spin_unlock(&state->lock);
        return -1;
    }
    state->tokens = tokens - 1;
    bpf_spin_unlock(&state->lock);
    return 1;
}


struct ipv6_opt_hdr_fh {
    __u8 nexthdr;
    __u8 hdrlen;
};

struct ipv6_frag_hdr_fh {
    __u8 nexthdr;
    __u8 reserved;
    __be16 frag_off;
    __be32 identification;
};

static __always_inline int find_ipv6_tcp(struct ipv6hdr *ip6, void *data_end, void **transport)
{
    __u8 next = ip6->nexthdr;
    void *cursor = ip6 + 1;

#pragma unroll
    for (int i = 0; i < 6; i++) {
        if (next == IPPROTO_TCP) {
            *transport = cursor;
            return 1;
        }
        if (next == IPPROTO_HOPOPTS || next == IPPROTO_ROUTING || next == IPPROTO_DSTOPTS) {
            struct ipv6_opt_hdr_fh *hdr = cursor;
            if ((void *)(hdr + 1) > data_end)
                return 0;
            __u64 length = ((__u64)hdr->hdrlen + 1) * 8;
            if (cursor + length > data_end)
                return 0;
            next = hdr->nexthdr;
            cursor += length;
            continue;
        }
        if (next == IPPROTO_FRAGMENT) {
            struct ipv6_frag_hdr_fh *frag = cursor;
            if ((void *)(frag + 1) > data_end)
                return 0;
            // Only the first fragment can contain the transport header.
            if (frag->frag_off & __constant_htons(0xFFF8))
                return -1;
            next = frag->nexthdr;
            cursor = frag + 1;
            continue;
        }
        if (next == IPPROTO_AH) {
            struct ipv6_opt_hdr_fh *ah = cursor;
            if ((void *)(ah + 1) > data_end)
                return 0;
            __u64 length = ((__u64)ah->hdrlen + 2) * 4;
            if (cursor + length > data_end)
                return 0;
            next = ah->nexthdr;
            cursor += length;
            continue;
        }
        return -1;
    }
    return -1;
}

static __always_inline int parse_tcp(void *cursor, void *data_end, __be16 *destination)
{
    struct tcphdr *tcp = cursor;
    if ((void *)(tcp + 1) > data_end)
        return 0;
    if (tcp->doff < 5 || (void *)tcp + ((__u64)tcp->doff * 4) > data_end)
        return 0;
    *destination = tcp->dest;
    return 1;
}

SEC("xdp")
int fh_xdp(struct xdp_md *ctx)
{
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end)
        return drop_packet(1);

    __u16 proto = eth->h_proto;
    void *cursor = eth + 1;

    // Support up to two VLAN tags without unbounded parser loops.
#pragma unroll
    for (int i = 0; i < 2; i++) {
        if (proto != __constant_htons(ETH_P_8021Q) && proto != __constant_htons(ETH_P_8021AD))
            break;
        struct fh_vlan_hdr *vlan = cursor;
        if ((void *)(vlan + 1) > data_end)
            return drop_packet(1);
        proto = vlan->encapsulated_proto;
        cursor = vlan + 1;
    }

    if (proto == __constant_htons(ETH_P_IP)) {
        struct iphdr *ip = cursor;
        if ((void *)(ip + 1) > data_end || ip->ihl < 5)
            return drop_packet(1);
        void *transport = (void *)ip + ((__u64)ip->ihl * 4);
        if (transport > data_end)
            return drop_packet(1);
        if (ip->protocol != IPPROTO_TCP)
            return pass_packet();
        // Non-initial fragments do not contain a TCP header; leave reassembly
        // to the kernel stack instead of affecting unrelated traffic.
        if (ip->frag_off & __constant_htons(0x1FFF))
            return pass_packet();
        __be16 destination = 0;
        if (!parse_tcp(transport, data_end, &destination))
            return drop_packet(1);
        if (!protected_port(destination))
            return pass_packet();
        int decision = allow_ipv4(ip->saddr);
        if (decision == 0)
            return drop_packet(2);
        if (decision < 0)
            return drop_packet(3);
        return pass_packet();
    }

    if (proto == __constant_htons(ETH_P_IPV6)) {
        struct ipv6hdr *ip6 = cursor;
        if ((void *)(ip6 + 1) > data_end)
            return drop_packet(1);
        void *transport = 0;
        int found = find_ipv6_tcp(ip6, data_end, &transport);
        if (found < 0)
            return pass_packet();
        if (found == 0)
            return drop_packet(1);
        __be16 destination = 0;
        if (!parse_tcp(transport, data_end, &destination))
            return drop_packet(1);
        if (!protected_port(destination))
            return pass_packet();
        struct ipv6_key key = {};
        __builtin_memcpy(key.addr, &ip6->saddr, sizeof(key.addr));
        __u8 *blocked = bpf_map_lookup_elem(&fh_block_v6, &key);
        return blocked && *blocked ? drop_packet(2) : pass_packet();
    }

    return pass_packet();
}

char LICENSE[] SEC("license") = "Dual BSD/GPL";
