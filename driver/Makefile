obj-m = raizn.o
ccflags-y := -std=gnu99 -Wimplicit-fallthrough=0
all:
	make -C /lib/modules/$(shell uname -r)/build/ M=$(PWD) modules EXTRA_CFLAGS="-DPROFILING" # EXTRA_CFLAGS="-DDEBUG"

clean:
	make -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
