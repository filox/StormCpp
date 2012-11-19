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

#ifndef STORM_H
#define STORM_H

#include <string>
#include <vector>
#include <deque>
#include <sstream>
#include <fstream>
#include <utility>
#include <iostream>
#include <stdexcept>
#include <sstream>

#include <sys/types.h>
#include <unistd.h>

#include "json/json.h"

namespace storm
{
	inline void Log(const std::string &msg);

	class Tuple
	{
		public:
			// values should be a Json::Value which is an array.
			Tuple(std::string id, std::string component, std::string stream,
					int task, const Json::Value &values) :
				_id(id), _component(component), _stream(stream),
				_task(task), _values(values)
			{
				if (!values.isArray())
					throw std::runtime_error("Cannot construct tuple, values is not an array");
			}
			Tuple(const Json::Value &values) :
				_id(""), _component(""), _stream(""),
				_task(-1), _values(values)
			{
			}
			// Simple getters.
			std::string GetID() const { return _id; }
			std::string GetComponent() const { return _component; }
			std::string GetStream() const { return _stream; }
			int GetTask() const { return _task; }
			const Json::Value &GetValues() const { return _values; }
			// Converts Tuple to JSON object.
			Json::Value ToJSON() const
			{
				return _values;
			}
		private:
			std::string _id, _component, _stream;
			int _task;
			Json::Value _values;
	};

	enum ModeEnum {
		NONE,
		BOLT,
		SPOUT
	};

	// Global variables.
	extern std::deque<Json::Value> pending_taskids;
	extern std::deque<Json::Value> pending_commands;
	extern Tuple *Anchor_tuple;
	extern ModeEnum Mode;

	// Reads lines from stdin until it reads "end".  When it reads "end", it
	// returns a JSON object read from stdin.
	inline Json::Value ReadMsg()
	{
		std::string msg, line;
		bool read_line = false;
		while(1)
		{
			read_line = getline(std::cin, line).good();
			if (line == "end")
				break;
			else if (read_line)
				msg += line + "\n";
		}
		Json::Value root;
		Json::Reader reader;
		bool parsing_successful = reader.parse(msg, root);
		if (!parsing_successful)
		{
			Log("Failed to parse JSON " + msg);
		}
		return root;
	}

	// Prints the message to stdout.  Using endl will automatically flush.
	inline void SendMsgToParent(Json::Value &v)
	{
		static Json::FastWriter writer;
		std::cout << writer.write(v);
		std::cout << "end" << std::endl;
	}

	// Reads an array of task IDs either from the queue or from stdin.  This
	// array of task IDs is returned when we do a non-direct emit from a spout,
	// and it indicates the task IDs to which the tuple was emitted.
	inline Json::Value ReadTaskIDs()
	{
		if (!pending_taskids.empty())
		{
			Json::Value v = pending_taskids.front();
			pending_taskids.pop_front();
			return v;
		}
		else
		{
			Json::Value msg = ReadMsg();
			while (!msg.isArray())
			{
				pending_commands.push_back(msg);
				msg = ReadMsg();
			}
			return msg;
		}
	}

	// Reads a new command from the queue or from stdin if the queue is empty.
	inline Json::Value ReadCommand()
	{
		if (!pending_commands.empty())
		{
			Json::Value v = pending_commands.front();
			pending_commands.pop_front();
			return v;
		}
		else
		{
			Json::Value msg = ReadMsg();
			while (msg.isArray())
			{
				pending_taskids.push_back(msg);
				msg = ReadMsg();
			}
			return msg;
		}
	}

	// Reads a tuple.  All the function does is call ReadCommand and then call
	// the tuple constructor with the appropriate parameters.
	inline Tuple ReadTuple()
	{
		Json::Value msg = ReadCommand();
		return Tuple(msg["id"].asString(), msg["comp"].asString(),
				msg["stream"].asString(), msg["task"].asInt(),
				msg["tuple"]);
	}

	// Send sync message to stdout.  It is called from spouts after processing
	// every ack, next, or fail command.
	inline void Sync()
	{
		Json::Value v;
		v["command"] = "sync";
		SendMsgToParent(v);
	}

	inline void Ack(const std::string &id)
	{
		Json::Value v;
		v["command"] = "ack";
		v["id"] = id;
		SendMsgToParent(v);
	}

	inline void Fail(const std::string &id)
	{
		Json::Value v;
		v["command"] = "fail";
		v["id"] = id;
		SendMsgToParent(v);
	}

	inline void Log(const std::string &msg)
	{
		Json::Value v;
		v["command"] = "log";
		v["msg"] = msg;
		SendMsgToParent(v);
	}

	// Writes an empty file in dir_name whose name is the PID of the current
	// process and sends a message to parent.  Getting the PID of the current
	// process is platform-specific, and for now only work on unix systems.
	inline void SendPid(const std::string &dir_name)
	{
		pid_t current_pid = getpid();
		Json::Value v;
		v["pid"] = current_pid;
		SendMsgToParent(v);
		std::stringstream full_name;
		// TODO: In the future, we might consider doing this properly.
		full_name << dir_name << "/" << current_pid;
		std::ofstream temp(full_name.str().c_str());
		temp.close();
	}

	inline void EmitSpout(Tuple &tuple, const std::string &stream,
			int task, std::string id = "")
	{
		Json::Value v;
		v["command"] = "emit";
		if (!stream.empty())
			v["stream"] = stream;
		if (!id.empty())
			v["id"] = id;
		if (task != -1)
			v["task"] = task;
		v["tuple"] = tuple.ToJSON();
		SendMsgToParent(v);
	}

	inline void EmitBolt(Tuple &tuple,
			const std::string &stream, int task,
			std::vector<Tuple> anchors = std::vector<Tuple>())
	{
		if (Anchor_tuple != NULL)
		{
			anchors.clear();
			anchors.push_back(*Anchor_tuple);
		}
		Json::Value v;
		v["command"] = "emit";
		if (!stream.empty())
			v["stream"] = stream;
		Json::Value json_anchors;
		for (int i = 0; i < anchors.size(); ++i)
			json_anchors.append(anchors[i].GetID());
		v["anchors"] = json_anchors;
		if (task != -1)
			v["task"] = task;
		v["tuple"] = tuple.ToJSON();
		SendMsgToParent(v);
	}

	// Internal emit.  Spouts and bolts should not call this function directly,
	// but instead use either Emit or EmitDirect.
	inline void Emit_(Tuple &tuple, std::string stream = "", int task = -1)
	{
		if (Mode == BOLT)
			EmitBolt(tuple, stream, task);
		else if (Mode == SPOUT)
			EmitSpout(tuple, stream, task);
	}

	// Various overloads.
	inline void Emit_(Tuple &tuple, const std::string &stream, int task, std::string &id)
	{
		if (Mode == SPOUT)
			EmitSpout(tuple, stream, task, id);
	}

	inline void Emit_(Tuple &tuple, const std::string &stream, int task, std::vector<Tuple> anchors)
	{
		if (Mode == BOLT)
			EmitBolt(tuple, stream, task, anchors);
	}

	// Emitting directly to a task.
	inline void EmitDirect(int task, Tuple &tuple, std::string stream = "")
	{
		Emit_(tuple, stream, task);
	}

	inline void EmitDirect(int task, Tuple &tuple, const std::string &stream, std::string &id)
	{
		Emit_(tuple, stream, task, id);
	}

	inline void EmitDirect(int task, Tuple &tuple, const std::string &stream, std::vector<Tuple> anchors)
	{
		Emit_(tuple, stream, task, anchors);
	}

	// Emit is called from the process method of bolts and NextTuple method of spouts.
	inline Json::Value Emit(Tuple &tuple, std::string stream = "", int task = -1)
	{
		Emit_(tuple, stream, task);
		return ReadTaskIDs();
	}

	inline Json::Value Emit(Tuple &tuple, const std::string &stream, int task, std::string &id)
	{
		Emit_(tuple, stream, task, id);
		return ReadTaskIDs();
	}

	inline Json::Value Emit(Tuple &tuple, const std::string &stream, int task, std::vector<Tuple> anchors)
	{
		Emit_(tuple, stream, task, anchors);
		return ReadTaskIDs();
	}

	inline std::pair<Json::Value, Json::Value> InitComponent()
	{
		Json::Value setupInfo = ReadMsg();
		SendPid(setupInfo["pidDir"].asString());
		std::pair<Json::Value, Json::Value> ret =
			std::make_pair(setupInfo["conf"], setupInfo["context"]);
		return ret;
	}

	// Base spout class.  All C++ spouts should inherit from this class.
	class Spout
	{
		public:
			Spout();
			virtual void Initialize(Json::Value conf, Json::Value context) = 0;
			// Read the next tuple and write it to stdout.
			virtual void NextTuple() = 0;
			virtual void Ack(Json::Value &msg)
			{
				storm::Ack(msg["id"].asString());
			}
			virtual void Fail(Json::Value &msg)
			{
				storm::Fail(msg["id"].asString());
			}
			void Run()
			{
				Mode = SPOUT;
				std::pair<Json::Value, Json::Value> conf_context = InitComponent();
				Initialize(conf_context.first, conf_context.second);
				Json::Value msg;
				std::string command;
				while(1)
				{
					msg = ReadCommand();
					command = msg["command"].asString();
					if (command == "next")
						NextTuple();
					else if (command == "ack")
						this->Ack(msg);
					else if (command == "fail")
						this->Fail(msg);
					Sync();
				}
			}
	};

	// Base bolt class.
	class Bolt
	{
		public:
			virtual void Initialize(Json::Value conf, Json::Value context) = 0;
			// This method performs the actual work.  All subclasses should
			// implement this method.
			virtual void Process(Tuple &tuple) = 0;
			// This method does exactly the same thing as the one in BasicBolt,
			// but without setting the anchor tuple.
			void Run()
			{
				Mode = BOLT;
				std::pair<Json::Value, Json::Value> conf_context = InitComponent();
				Initialize(conf_context.first, conf_context.second);
				while(1)
				{
					Tuple tuple = ReadTuple();
					Process(tuple);
					Ack(tuple.GetID());
				}
			}
	};

	// Basic bolt class.
	class BasicBolt
	{
		public:
			virtual void Initialize(Json::Value conf, Json::Value context) = 0;
			// This method performs the actual work.  All subclasses should
			// implement this method.
			virtual void Process(Tuple &tuple) = 0;
			void Run()
			{
				Mode = BOLT;
				std::pair<Json::Value, Json::Value> conf_context = InitComponent();
				Initialize(conf_context.first, conf_context.second);
				while(1)
				{
					Tuple tuple = ReadTuple();
					Anchor_tuple = &tuple;
					Process(tuple);
					Ack(tuple.GetID());
				}
			}
	};
}

#endif
