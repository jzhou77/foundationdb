/*
 * Token.cpp
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

#include "Token.h"
#include "TokenRange.h"
#include "Tokenizer.h"
#include "Error.h"
#include <cstddef>
#include <string>

namespace actorcompiler {

TokenRange Token::getMatchingRangeIn(const TokenRange& range) const {
	const auto& all = range.getAllTokens();

	if (value == "(") {
		// Scan forward for matching ')' at same parenDepth
		for (size_t i = static_cast<size_t>(position) + 1; i < range.endIndex(); ++i) {
			const Token& t = all[i];
			if (t.value == ")" && t.parenDepth == parenDepth) {
				return TokenRange(all, static_cast<size_t>(position) + 1, t.position);
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched (");
	}
	if (value == ")") {
		// Scan backward for matching '(' at same parenDepth
		for (size_t i = static_cast<size_t>(position); i-- > range.beginIndex();) {
			const Token& t = all[i];
			if (t.value == "(" && t.parenDepth == parenDepth) {
				return TokenRange(all, i + 1, static_cast<size_t>(position));
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched )");
	}
	if (value == "{") {
		// Forward to matching '}' at same braceDepth
		for (size_t i = static_cast<size_t>(position) + 1; i < range.endIndex(); ++i) {
			const Token& t = all[i];
			if (t.value == "}" && t.braceDepth == braceDepth) {
				return TokenRange(all, static_cast<size_t>(position) + 1, t.position);
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched {");
	}
	if (value == "}") {
		// Backward to matching '{' at same braceDepth
		for (size_t i = static_cast<size_t>(position); i-- > range.beginIndex();) {
			const Token& t = all[i];
			if (t.value == "{" && t.braceDepth == braceDepth) {
				return TokenRange(all, i + 1, static_cast<size_t>(position));
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched }");
	}
	if (value == "<") {
		// Inside angle brackets at same base paren depth
		int basePD = parenDepth;
		int depth = 0;
		for (size_t i = static_cast<size_t>(position) + 1; i < range.endIndex(); ++i) {
			const Token& t = all[i];
			if (t.parenDepth != basePD)
				continue;
			if (t.value == "<") {
				++depth;
			} else if (t.value == ">") {
				if (depth == 0) {
					return TokenRange(all, static_cast<size_t>(position) + 1, t.position);
				}
				--depth;
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched <");
	}
	if (value == "[") {
		// Inside brackets at same base paren depth
		int basePD = parenDepth;
		int depth = 0;
		for (size_t i = static_cast<size_t>(position) + 1; i < range.endIndex(); ++i) {
			const Token& t = all[i];
			if (t.parenDepth != basePD)
				continue;
			if (t.value == "[") {
				++depth;
			} else if (t.value == "]") {
				if (depth == 0) {
					return TokenRange(all, static_cast<size_t>(position) + 1, t.position);
				}
				--depth;
			}
		}
		throw Error(sourceLine, "Syntax error: Unmatched [");
	}

	throw Error(sourceLine, "Can't match this token!");
}

} // namespace actorcompiler
