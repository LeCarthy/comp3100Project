# for the xml library, use 'sudo apt-get install libxml2-dev'
CC=javac
SOURCES = DSClient.java

all: DSClient run
build: DSClient

DSClient: $(SOURCES)
	$(CC) $^

run:
	java DSClient