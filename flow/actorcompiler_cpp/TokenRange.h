/*
 * TokenRange.h
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

#ifndef ACTORCOMPILER_TOKENRANGE_H
#define ACTORCOMPILER_TOKENRANGE_H

#include "Token.h"
#include <vector>
#include <functional>
#include <optional>
#include <stdexcept>

namespace actorcompiler {

// Iterator-like wrapper for a range of tokens
// Provides LINQ-like operations for token manipulation
class TokenRange {
private:
	const std::vector<Token>* tokens;
	size_t beginPos;
	size_t endPos;

public:
	TokenRange(const std::vector<Token>& tokens, size_t begin, size_t end)
	  : tokens(&tokens), beginPos(begin), endPos(end) {
		if (begin > end) {
			throw std::invalid_argument("Invalid TokenRange: begin > end");
		}
	}

	// Range properties
	bool isEmpty() const { return beginPos == endPos; }
	size_t begin() const { return beginPos; }
	size_t end() const { return endPos; }
	size_t length() const { return endPos - beginPos; }

	// Access tokens
	const Token& first() const;
	const Token& last() const;
	const Token& operator[](size_t index) const { return (*tokens)[beginPos + index]; }

	// Find operations
	template <typename Predicate>
	std::optional<Token> firstOrDefault(Predicate pred) const;

	template <typename Predicate>
	const Token& last(Predicate pred) const;

	// Range manipulation
	TokenRange skip(size_t count) const;
	TokenRange consume(const std::string& value) const;

	template <typename Predicate>
	TokenRange consume(const std::string& error, Predicate pred) const;

	template <typename Predicate>
	TokenRange skipWhile(Predicate pred) const;

	template <typename Predicate>
	TokenRange takeWhile(Predicate pred) const;

	template <typename Predicate>
	TokenRange revTakeWhile(Predicate pred) const;

	template <typename Predicate>
	TokenRange revSkipWhile(Predicate pred) const;

	// Iteration support
	using const_iterator = std::vector<Token>::const_iterator;
	const_iterator cbegin() const { return tokens->begin() + beginPos; }
	const_iterator cend() const { return tokens->begin() + endPos; }

	// Check if all tokens satisfy predicate
	template <typename Predicate>
	bool all(Predicate pred) const;

	// Check if any token satisfies predicate
	template <typename Predicate>
	bool any(Predicate pred) const;

	// Get underlying token vector
	const std::vector<Token>& getAllTokens() const { return *tokens; }
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_TOKENRANGE_H
