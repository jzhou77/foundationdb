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

#include "Function.h"
#include <memory>

namespace actorcompiler {

// Compilation context - tracks current code generation state
struct Context {
	Function* target = nullptr; // Current function being written to
	Function* next = nullptr; // Next continuation function
	Function* breakF = nullptr; // Break target function
	Function* continueF = nullptr; // Continue target function
	Function* catchFErr = nullptr; // Error handler function
	int tryLoopDepth = 0; // Loop depth inside try block

	// Mark target as unreachable
	void unreachable() { target = nullptr; }

	// Create context with new target
	Context withTarget(Function* newTarget) const;

	// Create context for loop
	Context loopContext(Function* newTarget, Function* breakF, Function* continueF, int deltaLoopDepth) const;

	// Create context with new catch handler
	Context withCatch(Function* newCatchFErr) const;

	// Clone context
	Context clone() const;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_CONTEXT_H
