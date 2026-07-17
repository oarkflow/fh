CLANG ?= clang
CFLAGS ?= -O2 -g -Wall -Werror -target bpf
ARCH ?= x86
OUTPUT ?= fh_xdp.o

.PHONY: all clean
all: $(OUTPUT)

$(OUTPUT): fh_xdp.c include/bpf_helpers.h
	$(CLANG) $(CFLAGS) -D__TARGET_ARCH_$(ARCH) -I. -c fh_xdp.c -o $(OUTPUT)

clean:
	rm -f $(OUTPUT)
