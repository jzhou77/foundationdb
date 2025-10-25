/*
 * ActorCompiler.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ACTORCOMPILER_ACTORCOMPILER_H
#define ACTORCOMPILER_ACTORCOMPILER_H

#include "ParseTree.h"
#include "Function.h"
#include "Context.h"
#include "Error.h"
#include <string>
#include <set>
#include <map>
#include <cstdint>
#include <ostream>

namespace actorcompiler {

// Descriptor compiler for DESCR declarations
class DescrCompiler {
private:
	const Descr& descr;
	std::string memberIndentStr;

public:
	DescrCompiler(const Descr& descr, int braceDepth);
	void write(std::ostream& writer, int& lines);
};

// Main actor compiler - generates C++ code from actor AST
class ActorCompiler {
private:
	const Actor& actor;
	std::string sourceFile;
	bool isTopLevel;
	bool lineNumbersEnabled;
	bool generateProbes;

	// Generated class/function names
	std::string className;
	std::string fullClassName;
	std::string stateClassName;

	// State variables and function registry
	std::set<std::string> stateVariables;
	std::map<std::pair<uint64_t, uint64_t>, std::string> uidObjects;
	std::map<std::string, Function*> functions; // label -> Function mapping
	int labelIndex; // Counter for generating unique continuation labels

public:
	ActorCompiler(const Actor& actor,
	              const std::string& sourceFile,
	              bool isTopLevel,
	              bool lineNumbersEnabled,
	              bool generateProbes);

	~ActorCompiler();

	// Write generated actor code
	void write(std::ostream& writer);

	// Get UID mappings
	const std::map<std::pair<uint64_t, uint64_t>, std::string>& getUidObjects() const { return uidObjects; }

	// Get discovered state variables
	const std::set<std::string>& getStateVariables() const { return stateVariables; }

#ifdef ACTORCOMPILER_TEST_BUILD
	// Test-only interface for validating function registry
	Function* testGetFunction(const std::string& label) { return getFunction(label); }
	std::string testGenerateLabel() { return generateLabel(); }
	size_t testGetFunctionCount() const { return functions.size(); }
#endif

private:
	// State discovery - traverse AST to find state variables
	void findState(Statement* stmt);

	// Function registry - get or create continuation function by label
	Function* getFunction(const std::string& label);

	// Generate a unique continuation label
	std::string generateLabel();

	// Code generation methods (to be implemented in Step 5)
	void writeActorFunction(std::ostream& writer, const std::string& fullReturnType);
	void writeActorClass(std::ostream& writer, const std::string& fullStateClassName);
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_ACTORCOMPILER_H
