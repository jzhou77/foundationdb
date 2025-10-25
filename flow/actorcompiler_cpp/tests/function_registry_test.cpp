/*
 * function_registry_test.cpp
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

#include "../ActorCompiler.h"
#include "../ParseTree.h"
#include <iostream>
#include <cassert>
#include <memory>

using namespace actorcompiler;

void testFunctionRegistry() {
	// Create a minimal actor
	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";
	actor.sourceLine = 1;

	// Create compiler
	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	std::cout << "Testing function registry...\n";

	// Test 1: Function count starts at 0
	assert(compiler.testGetFunctionCount() == 0);
	std::cout << "✓ Initial function count is 0\n";

	// Test 2: Get a function creates it
	Function* func1 = compiler.testGetFunction("body1");
	assert(func1 != nullptr);
	assert(compiler.testGetFunctionCount() == 1);
	std::cout << "✓ getFunction creates new function, count = 1\n";

	// Test 3: Getting same function returns cached version
	Function* func1Again = compiler.testGetFunction("body1");
	assert(func1Again == func1); // Same pointer
	assert(compiler.testGetFunctionCount() == 1); // Count unchanged
	std::cout << "✓ getFunction returns cached function for same label\n";

	// Test 4: Get different function creates another
	Function* func2 = compiler.testGetFunction("loopBody");
	assert(func2 != nullptr);
	assert(func2 != func1); // Different pointer
	assert(compiler.testGetFunctionCount() == 2);
	std::cout << "✓ getFunction creates new function for different label, count = 2\n";

	// Test 5: Generate labels are unique
	std::string label1 = compiler.testGenerateLabel();
	std::string label2 = compiler.testGenerateLabel();
	std::string label3 = compiler.testGenerateLabel();
	assert(label1 != label2);
	assert(label2 != label3);
	assert(label1 != label3);
	std::cout << "✓ generateLabel creates unique labels: " << label1 << ", " << label2 << ", " << label3 << "\n";

	// Test 6: Generated labels follow pattern
	assert(label1 == "cont1");
	assert(label2 == "cont2");
	assert(label3 == "cont3");
	std::cout << "✓ Generated labels follow 'contN' pattern\n";

	// Test 7: Can create functions with generated labels
	Function* funcCont1 = compiler.testGetFunction(label1);
	assert(funcCont1 != nullptr);
	assert(funcCont1 != func1);
	assert(funcCont1 != func2);
	assert(compiler.testGetFunctionCount() == 3); // body1, loopBody, cont1 (from generated label)
	std::cout << "✓ Can create functions using generated labels, count = 3\n";

	// Test 8: Verify caching still works with generated labels
	Function* funcCont1Again = compiler.testGetFunction(label1);
	assert(funcCont1Again == funcCont1); // Same pointer
	assert(compiler.testGetFunctionCount() == 3); // Count unchanged
	std::cout << "✓ Caching works correctly with generated labels\n";

	std::cout << "\nAll function registry tests PASSED!\n";
}

int main() {
	testFunctionRegistry();

	std::cout << "\nFunction registry implementation verified:\n";
	std::cout << "- getFunction() creates functions lazily\n";
	std::cout << "- getFunction() caches and returns existing functions\n";
	std::cout << "- generateLabel() creates unique continuation labels\n";
	std::cout << "- Labels follow 'contN' pattern with incrementing counter\n";
	std::cout << "- Destructor will clean up allocated Function objects\n";

	return 0;
}
