SOURCES := $(wildcard *.cc *.hh)
TARGETS := ybinlogp.so ybinlogp

CFLAGS += -Wall -Wextra -Werror -g
CXXFLAGS += -Wall -Wextra -Werror -g 

all: $(TARGETS)

ybinlogp.so: ybinlogp.o
	$(AR) $< -o $@ $(LDFLAGS)

ybinlogp: ybinlogp.o
	$(LD) $< -o $@ $(LDFLAGS)

force:: clean all

clean::
	rm -f $(TARGETS) *.o

ybinlogp.o: ybinlogp.cc ybinlogp.hh
