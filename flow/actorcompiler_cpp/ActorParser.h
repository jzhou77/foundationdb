/*
 * ActorParser.h
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

#ifndef ACTORCOMPILER_ACTORPARSER_H
#define ACTORCOMPILER_ACTORPARSER_H

#include "Token.h"
#include "TokenRange.h"
#include "ParseTree.h"
#include "Error.h"
#include <string>
#include <vector>
#include <map>

namespace actorcompiler {

// Parser for Flow actor syntax
class ActorParser {
private:
	std::vector<Token> tokens;
	std::string sourceFile;
	ErrorMessagePolicy errorMessagePolicy;
	bool generateProbes;
	bool lineNumbersEnabled = true;
	std::map<std::pair<uint64_t, uint64_t>, std::string> uidObjects;

public:
	ActorParser(const std::string& text,
	            const std::string& sourceFile,
	            const ErrorMessagePolicy& errorMessagePolicy,
	            bool generateProbes);

	// Write processed output to stream
	void write(std::ostream& writer, const std::string& destFileName);

	// Get UID mappings for actors
	const std::map<std::pair<uint64_t, uint64_t>, std::string>& getUidObjects() const { return uidObjects; }

private:
	// Parsing methods (to be implemented in Step 4)
	Actor parseActor(size_t pos, size_t& end);
	Descr parseDescr(size_t pos, size_t& end);

	// Helper methods
	TokenRange range(size_t begin, size_t end) const;
	std::string str(const TokenRange& range) const;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_ACTORPARSER_H
