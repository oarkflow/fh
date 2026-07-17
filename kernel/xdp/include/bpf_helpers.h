#ifndef FH_BPF_HELPERS_H
#define FH_BPF_HELPERS_H

#include <linux/bpf.h>
#include <linux/types.h>

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, val) int (*name)[val]
#define __type(name, val) val *name
#define __array(name, val) val *name[]
#ifndef __always_inline
#define __always_inline inline __attribute__((always_inline))
#endif

static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)BPF_FUNC_map_lookup_elem;
static long (*bpf_map_update_elem)(void *map, const void *key, const void *value, __u64 flags) = (void *)BPF_FUNC_map_update_elem;
static __u64 (*bpf_ktime_get_ns)(void) = (void *)BPF_FUNC_ktime_get_ns;
static long (*bpf_spin_lock)(struct bpf_spin_lock *lock) = (void *)BPF_FUNC_spin_lock;
static long (*bpf_spin_unlock)(struct bpf_spin_lock *lock) = (void *)BPF_FUNC_spin_unlock;

#endif
