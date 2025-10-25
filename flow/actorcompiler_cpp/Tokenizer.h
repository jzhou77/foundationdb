/*
 * Tokenizer.h
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

#ifndef ACTORCOMPILER_TOKENIZER_H
#define ACTORCOMPILER_TOKENIZER_H

#include "Token.h"
#include <string>
#include <vector>
#include <regex>

namespace actorcompiler {

// Tokenizer - Lexical analysis for actor compiler
class Tokenizer {
public:
	// Tokenize input text into tokens
	static std::vector<Token> tokenize(const std::string& text);

	// Count parentheses and brace depth for each token
	static void countParens(std::vector<Token>& tokens);

private:
	// Regex patterns for token matching
	static const std::vector<std::regex>& getTokenPatterns();
};

// Helper functions for filtering tokens outside brackets/angles
namespace AngleBracketParser {
std::vector<Token> notInsideAngleBrackets(const std::vector<Token>& tokens);
}

namespace BracketParser {
std::vector<Token> notInsideBrackets(const std::vector<Token>& tokens);
}

} // namespace actorcompiler

#endif // ACTORCOMPILER_TOKENIZER_H
