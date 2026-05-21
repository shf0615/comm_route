CC = gcc
CFLAGS = -Wall -Wextra -std=gnu11 -I.. -I../common -I../router -I../frag -I../reliability -I../transaction -D_GNU_SOURCE
LDFLAGS = -lpthread

ROUTE_SRC = ../common/route_pool.c ../common/route_crc.c ../common/route_queue.c \
            ../router/route_router.c ../frag/route_frag.c \
            ../reliability/route_reliability.c ../transaction/route_transaction.c \
            ../route_instance.c

all: example_mq

example_mq: example_mq_transport.c $(ROUTE_SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

run: example_mq
	./example_mq

clean:
	rm -f example_mq

.PHONY: all run clean
