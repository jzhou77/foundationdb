/*
 * Context.h
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

#ifndef ACTORCOMPILER_CONTEXT_H
#define ACTORCOMPILER_CONTEXT_H

#include <string>

namespace actorcompiler {

// Compilation context - tracks current code generation state
struct Context {
	std::string targetLabel; // Current target label for gotos
	std::string breakLabel; // Break target label
	std::string continueLabel; // Continue target label
	std::string catchHandler; // Catch handler label
	std::string errorVarName; // Error variable name
	std::string errorCodeVarName; // Error code variable name
	int continuationIndex = 0; // Continuation index for naming (shared with catch handler)

	// Loop context
	int loopDepth = 0; // Nesting level of loops (0 = not in loop)
	std::string loopBodyPrefix; // Prefix for loop body methods (e.g., "a_body1cont1loopBody1")

	// Create an unreachable context
	static Context createUnreachable() {
		Context ctx;
		ctx.targetLabel = "UNREACHABLE";
		return ctx;
	}

	// Create context with new target
	Context withTarget(const std::string& newTarget) const;

	// Create context for loop
	Context loopContext(const std::string& breakLbl, const std::string& continueLbl) const;

	// Create context for loop body with depth tracking
	Context loopBodyContext(int depth, const std::string& bodyPrefix, const std::string& breakLbl, const std::string& continueLbl) const;

	// Create context with new catch handler
	Context withCatch(const std::string& errVar, const std::string& errCode, const std::string& handler) const;

	// Clone context
	Context clone() const;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_CONTEXT_H
