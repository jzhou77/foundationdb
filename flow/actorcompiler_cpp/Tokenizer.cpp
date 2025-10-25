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
#include "Error.h"

namespace actorcompiler {

// Stub implementation - to be completed in Step 3
std::vector<Token> Tokenizer::tokenize(const std::string& text) {
	// TODO: Implement in Step 3
	return {};
}

void Tokenizer::countParens(std::vector<Token>& tokens) {
	// TODO: Implement in Step 3
}

} // namespace actorcompiler
