/*
 * Tokenizer.cpp
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

#include "Tokenizer.h"
#include "TokenRange.h"
#include "Error.h"
#include <algorithm>
#include <vector>
#include <regex>
#include <string>
#include <cstddef>

namespace actorcompiler {

// Build and cache regex patterns equivalent to the C# tokenExpressions
static std::vector<std::regex> buildPatterns() {
	// Each pattern is anchored at start (^) to match at current position.
	// Order matters and mirrors the C# implementation.
	const char* raw[] = {
		"^\\{",
		"^\\}",
		"^\\(",
		"^\\)",
		"^\\[",
		"^\\]",
		"^//[^\\n]*",
		"^/[*]([*][^/]|[^*])*[*]/",
		"^'(\\\\.|[^\\'\\n])*'",
		"^\"(\\\\.|[^\"\\n])*\"",
		"^[a-zA-Z_][a-zA-Z_0-9]*",
		"^\r\n",
		"^\n",
		"^::",
		"^:",
		"^#[a-z]*",
		"^.",
	};
	std::vector<std::regex> v;
	v.reserve(sizeof(raw) / sizeof(raw[0]));
	for (auto* pat : raw) {
		v.emplace_back(pat, std::regex_constants::ECMAScript);
	}
	return v;
}

const std::vector<std::regex>& Tokenizer::getTokenPatterns() {
	static const std::vector<std::regex> patterns = buildPatterns();
	return patterns;
}

std::vector<Token> Tokenizer::tokenize(const std::string& text) {
	std::vector<Token> out;
	const auto& patterns = getTokenPatterns();
	size_t pos = 0;
	while (pos < text.size()) {
		bool matched = false;
		for (const auto& re : patterns) {
			std::cmatch m;
			const char* start = text.c_str() + pos;
			if (std::regex_search(start, text.c_str() + text.size(), m, re) && m.position() == 0) {
				Token t;
				t.value = m.str(0);
				out.emplace_back(std::move(t));
				pos += m.length(0);
				matched = true;
				break;
			}
		}
		if (!matched) {
			// Compute approximate line for diagnostics
			int line = 1 + static_cast<int>(std::count(text.begin(), text.begin() + pos, '\n'));
			throw Error(line, "Can't tokenize! %d", static_cast<int>(pos));
		}
	}
	return out;
}

void Tokenizer::countParens(std::vector<Token>& tokens) {
	int braceDepth = 0;
	int parenDepth = 0;
	int lineCount = 1;
	int lastParenLine = 1;
	int lastBraceLine = 1;

	for (size_t i = 0; i < tokens.size(); ++i) {
		const std::string& v = tokens[i].value;
		if (v == "}") {
			--braceDepth;
		} else if (v == ")") {
			--parenDepth;
		} else if (v == "\r\n" || v == "\n") {
			++lineCount;
		}

		if (braceDepth < 0)
			throw Error(lineCount, "Mismatched braces");
		if (parenDepth < 0)
			throw Error(lineCount, "Mismatched parenthesis");

		tokens[i].position = static_cast<int>(i);
		tokens[i].sourceLine = lineCount;
		tokens[i].braceDepth = braceDepth;
		tokens[i].parenDepth = parenDepth;

		if (v.rfind("/*", 0) == 0) {
			lineCount += static_cast<int>(std::count(v.begin(), v.end(), '\n'));
		}

		if (v == "{") {
			++braceDepth;
			if (braceDepth == 1)
				lastBraceLine = tokens[i].sourceLine;
		} else if (v == "(") {
			++parenDepth;
			if (parenDepth == 1)
				lastParenLine = tokens[i].sourceLine;
		}
	}

	if (braceDepth != 0)
		throw Error(lastBraceLine, "Unmatched brace");
	if (parenDepth != 0)
		throw Error(lastParenLine, "Unmatched parenthesis");
}

namespace AngleBracketParser {
std::vector<Token> notInsideAngleBrackets(const TokenRange& range) {
	std::vector<Token> result;
	int angleDepth = 0;
	int basePD = 0;
	bool baseInit = false;
	for (auto it = range.cbegin(); it != range.cend(); ++it) {
		const Token& tok = *it;
		if (!baseInit) {
			basePD = tok.parenDepth;
			baseInit = true;
		}
		if (tok.parenDepth == basePD && tok.value == ">")
			angleDepth--;
		if (angleDepth == 0)
			result.push_back(tok);
		if (tok.parenDepth == basePD && tok.value == "<")
			angleDepth++;
	}
	return result;
}
} // namespace AngleBracketParser

namespace BracketParser {
std::vector<Token> notInsideBrackets(const TokenRange& range) {
	std::vector<Token> result;
	int bracketDepth = 0;
	int basePD = 0;
	bool baseInit = false;
	for (auto it = range.cbegin(); it != range.cend(); ++it) {
		const Token& tok = *it;
		if (!baseInit) {
			basePD = tok.parenDepth;
			baseInit = true;
		}
		if (tok.parenDepth == basePD && tok.value == "]")
			bracketDepth--;
		if (bracketDepth == 0)
			result.push_back(tok);
		if (tok.parenDepth == basePD && tok.value == "[")
			bracketDepth++;
	}
	return result;
}
} // namespace BracketParser

} // namespace actorcompiler
