CC=javac
SOURCES = DSClient.java

all: DSClient run
build: DSClient
test: DSClient test

DSClient: $(SOURCES)
	$(CC) $^

run:
	java DSClient

test:
	./S1Tests-wk6.sh -n DSClient.class