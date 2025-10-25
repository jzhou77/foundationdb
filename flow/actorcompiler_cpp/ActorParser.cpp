/*
 * ActorParser.cpp
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

#include "ActorParser.h"
#include "Tokenizer.h"

namespace actorcompiler {

ActorParser::ActorParser(const std::string& text,
                         const std::string& sourceFile,
                         const ErrorMessagePolicy& errorMessagePolicy,
                         bool generateProbes)
  : sourceFile(sourceFile), errorMessagePolicy(errorMessagePolicy), generateProbes(generateProbes) {
	// TODO: Implement in Step 4
	tokens = Tokenizer::tokenize(text);
	Tokenizer::countParens(tokens);
}

void ActorParser::write(std::ostream& /* writer */, const std::string& /* destFileName */) {
	// TODO: Implement in Step 4
}

TokenRange ActorParser::range(size_t begin, size_t end) const {
	return TokenRange(tokens, begin, end);
}

std::string ActorParser::str(const TokenRange& /* range */) const {
	// TODO: Implement in Step 4
	return "";
}

} // namespace actorcompiler
