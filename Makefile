CC = g++
CFLAGS = -Werror -Wall
#include path for the JSON header files
IJSON = path/to/jsoncpp/includes
#location of the JSON library
JSON = json
#the main storm library
STORM = Storm.cpp

		
split_sentence:
		$(CC) $(CFLAGS) -I$(IJSON) SplitSentenceTest.cpp $(STORM) -L/usr/local/lib -o split_sentence -l$(JSON)

clean:
		rm -f split_sentence