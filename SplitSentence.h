/*
Author: Sasa Petrovic (montyphyton@gmail.com)
Copyright (c) 2012, University of Edinburgh
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the author nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#ifndef SPLIT_SENTENCE_H
#define SPLIT_SENTENCE_H

#include <string>
#include <vector>

#include "Storm.h"
#include "json/json.h"

namespace storm
{

// A simple function that splits the input string into words based on
// whitespaces.
void splitString(
		const std::string &text,
		std::vector<std::string> &parts,
		const std::string &delimiter = " ")
{
	parts.clear();
	size_t delimiterPos = text.find(delimiter);
	size_t lastPos = 0;
	if (delimiterPos == std::string::npos)
	{
		parts.push_back(text);
		return;
	}

	while(delimiterPos != std::string::npos)
	{
		parts.push_back(text.substr(lastPos, delimiterPos - lastPos));
		lastPos = delimiterPos + delimiter.size();
		delimiterPos = text.find(delimiter, lastPos);
	}
	parts.push_back(text.substr(lastPos));
}

class SplitSentence : public BasicBolt
{
	public:
		void Initialize(Json::Value conf, Json::Value context) { }
		void Process(Tuple &tuple)
		{
			std::string s = tuple.GetValues()[2].asString();
			std::vector<std::string> tokens;
			splitString(s, tokens, " ");
			for (unsigned int i = 0; i < tokens.size(); ++i)
			{
				Json::Value j_token;
				j_token.append(tokens[i]);
				Tuple t(j_token);
				Emit(t);
			}
		}
};

}

#endif
