CC = g++
CFLAGS = -Werror -Wall
#include path for the JSON header files
IJSON = /path/to/jsonccp/includes
#location of the JSON library
JSON = json
#the main storm library
STORM = Storm.cpp
STORMLIB = storm.o

storm:
		$(CC) $(CFLAGS) -fPIC -I$(IJSON) -c $(STORM) -o $(STORMLIB) 
		
split_sentence:
		$(CC) $(CFLAGS) -I$(IJSON) SplitSentenceTest.cpp $(STORMLIB) -o split_sentence.o -l$(JSON)

clean:
		rm -f *.o