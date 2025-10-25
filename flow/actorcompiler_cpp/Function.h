/*
 * Function.h
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

#ifndef ACTORCOMPILER_FUNCTION_H
#define ACTORCOMPILER_FUNCTION_H

#include <string>
#include <vector>
#include <memory>
#include <sstream>

namespace actorcompiler {

// Represents a generated C++ function with accumulated body text
class Function {
public:
	std::string name;
	std::string returnType;
	std::vector<std::string> formalParameters;
	bool endIsUnreachable = false;
	std::string exceptionParameterIs;
	bool publicName = false;
	std::string specifiers;

	Function() = default;

	// Write methods (to be implemented in Step 5)
	void indent(int change);
	void writeLine(const std::string& line);
	void writeLineUnindented(const std::string& line);

	// Get accumulated body text
	std::string getBodyText() const { return body.str(); }

	// Check if function was called
	bool wasCalled() const { return called; }
	void markCalled() { called = true; }

	// Function call generation
	std::string call(const std::vector<std::string>& parameters);

private:
	std::ostringstream body;
	std::string indentation;
	bool called = false;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_FUNCTION_H
