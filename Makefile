CC = g++
CFLAGS = -Werror -Wall
#include path for the JSON header files
IJSON = /home/iain/git/jsoncpp-src-0.6.0-rc2/include
#location of the JSON library
JSON = json
#the main storm library
STORM = Storm.cpp

		
split_sentence:
		$(CC) $(CFLAGS) -I$(IJSON) SplitSentenceTest.cpp $(STORM) -o split_sentence -l$(JSON)

clean:
		rm -f split_sentence