/*
 * Error.h
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

#ifndef ACTORCOMPILER_ERROR_H
#define ACTORCOMPILER_ERROR_H

#include <stdexcept>
#include <string>
#include <sstream>

namespace actorcompiler {

// Custom exception class for parsing and compilation errors
// Tracks the source line number where the error occurred
class Error : public std::runtime_error {
private:
	int sourceLine;

public:
	// Constructor with line number and formatted message
	template <typename... Args>
	Error(int line, const char* format, Args&&... args)
	  : std::runtime_error(formatMessage(format, std::forward<Args>(args)...)), sourceLine(line) {}

	// Constructor with line number and plain message
	Error(int line, const std::string& message) : std::runtime_error(message), sourceLine(line) {}

	int getSourceLine() const noexcept { return sourceLine; }

private:
	// Helper to format error messages
	template <typename... Args>
	static std::string formatMessage(const char* format, Args&&... args) {
		// Simple sprintf-style formatting
		char buffer[4096];
		snprintf(buffer, sizeof(buffer), format, std::forward<Args>(args)...);
		return std::string(buffer);
	}
};

// Policy class for error message handling
class ErrorMessagePolicy {
public:
	bool disableDiagnostics = false;

	void handleActorWithoutWait(const std::string& sourceFile, const class Actor& actor);

	bool actorsNoDiscardByDefault() const { return !disableDiagnostics; }
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_ERROR_H
