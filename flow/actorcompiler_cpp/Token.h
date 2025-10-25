/*
 * Token.h
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

#ifndef ACTORCOMPILER_TOKEN_H
#define ACTORCOMPILER_TOKEN_H

#include <string>
#include <functional>
#include <stdexcept>
#include "Error.h"

namespace actorcompiler {

class TokenRange; // Forward declaration

// Represents a single lexical token
struct Token {
	std::string value;
	int position = 0; // Position in token array
	int sourceLine = 0; // Line number in source file
	int braceDepth = 0; // Nesting depth of braces
	int parenDepth = 0; // Nesting depth of parentheses

	// Check if token is whitespace or comment
	bool isWhitespace() const {
		return value == " " || value == "\n" || value == "\r" || value == "\r\n" || value == "\t" ||
		       value.find("//") == 0 || value.find("/*") == 0;
	}

	// Assert a condition with an error message
	template <typename Predicate>
	const Token& assert(const std::string& error, Predicate pred) const {
		if (!pred(*this)) {
			throw Error(sourceLine, "%s", error.c_str());
		}
		return *this;
	}

	// Find matching bracket/paren in a range
	TokenRange getMatchingRangeIn(const TokenRange& range) const;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_TOKEN_H
