/*
 * ActorCompiler.cpp
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

#include "ActorCompiler.h"
#include <iostream>

namespace actorcompiler {

ActorCompiler::ActorCompiler(const Actor& actor,
                             const std::string& sourceFile,
                             bool isTopLevel,
                             bool lineNumbersEnabled,
                             bool generateProbes)
  : actor(actor), sourceFile(sourceFile), isTopLevel(isTopLevel), lineNumbersEnabled(lineNumbersEnabled),
    generateProbes(generateProbes) {
	// TODO: Implement in Step 5
}

void ActorCompiler::write(std::ostream& /* writer */) {
	// TODO: Implement in Step 5
}

DescrCompiler::DescrCompiler(const Descr& descr, int braceDepth) : descr(descr) {
	memberIndentStr = std::string(braceDepth, '\t');
}

void DescrCompiler::write(std::ostream& /* writer */, int& lines) {
	// TODO: Implement in Step 5
	lines = 0;
}

void ErrorMessagePolicy::handleActorWithoutWait(const std::string& sourceFile, const Actor& actor) {
	if (!disableDiagnostics && !actor.isTestCase) {
		std::cerr << sourceFile << ":" << actor.sourceLine << ": warning: ACTOR " << actor.name
		          << " does not contain a wait() statement\n";
	}
}

} // namespace actorcompiler
