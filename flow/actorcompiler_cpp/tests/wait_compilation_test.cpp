/*
 * wait_compilation_test.cpp
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
#include "../Context.h"
#include <iostream>
#include <sstream>
#include <cassert>

using namespace actorcompiler;

void testSimpleWaitCompilation() {
	std::cout << "Test 1: Simple wait statement with state result\n";

	// Create a minimal actor with one wait statement
	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";

	// Create actor compiler
	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create a wait statement: state int x = wait(getFuture());
	auto waitStmt = std::make_unique<WaitStatement>();
	waitStmt->result.type = "int";
	waitStmt->result.name = "x";
	waitStmt->futureExpression = "getFuture()";
	waitStmt->resultIsState = true;
	waitStmt->isWaitNext = false;

	// Get the entry function
	Function* entryFunc = compiler.testGetFunction("entry");

	// Create a basic context (no error handling, no loops)
	Context ctx = Context::createUnreachable();

	// Compile the wait statement
	compiler.testCompile(entryFunc, waitStmt.get(), ctx);

	// Verify the generated code
	std::ostringstream output;
	entryFunc->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify key patterns exist
	assert(code.find("StrictFuture<int> __when_expr = getFuture();") != std::string::npos);
	assert(code.find("if (__when_expr.isReady())") != std::string::npos);
	assert(code.find("if (__when_expr.isError())") != std::string::npos);
	assert(code.find("x = __when_expr.get();") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos);
	assert(code.find("cont1:") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testWaitWithLocalResult() {
	std::cout << "Test 2: Wait statement with local (non-state) result\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create wait statement: int y = wait(getOtherFuture());
	auto waitStmt = std::make_unique<WaitStatement>();
	waitStmt->result.type = "int";
	waitStmt->result.name = "y";
	waitStmt->futureExpression = "getOtherFuture()";
	waitStmt->resultIsState = false; // Local variable
	waitStmt->isWaitNext = false;

	Function* entryFunc = compiler.testGetFunction("entry");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(entryFunc, waitStmt.get(), ctx);

	std::ostringstream output;
	entryFunc->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify patterns
	assert(code.find("StrictFuture<int> __when_expr = getOtherFuture();") != std::string::npos);
	assert(code.find("int y = __when_expr.get();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testWaitWithErrorHandler() {
	std::cout << "Test 3: Wait statement with error handler\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto waitStmt = std::make_unique<WaitStatement>();
	waitStmt->result.type = "int";
	waitStmt->result.name = "z";
	waitStmt->futureExpression = "getFuture()";
	waitStmt->resultIsState = true;
	waitStmt->isWaitNext = false;

	Function* entryFunc = compiler.testGetFunction("entry");

	// Create context with error handler
	// Parameters: errorVarName, errorCodeVarName, catchHandlerLabel
	Context ctx = Context::createUnreachable().withCatch("error", "errorCode", "catchLabel");

	compiler.testCompile(entryFunc, waitStmt.get(), ctx);

	std::ostringstream output;
	entryFunc->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify error handling
	assert(code.find("error = __when_expr.getError();") != std::string::npos);
	assert(code.find("goto catchLabel;") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testMultipleWaits() {
	std::cout << "Test 4: Multiple sequential waits\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create first wait
	auto wait1 = std::make_unique<WaitStatement>();
	wait1->result.type = "int";
	wait1->result.name = "a";
	wait1->futureExpression = "getFirst()";
	wait1->resultIsState = true;

	// Create second wait
	auto wait2 = std::make_unique<WaitStatement>();
	wait2->result.type = "int";
	wait2->result.name = "b";
	wait2->futureExpression = "getSecond()";
	wait2->resultIsState = true;

	Function* entryFunc = compiler.testGetFunction("entry");
	Context ctx = Context::createUnreachable();

	// Compile both waits sequentially
	compiler.testCompile(entryFunc, wait1.get(), ctx);
	compiler.testCompile(entryFunc, wait2.get(), ctx);

	std::ostringstream output;
	entryFunc->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify both continuations exist
	assert(code.find("cont1:") != std::string::npos);
	assert(code.find("cont2:") != std::string::npos);
	assert(code.find("a = __when_expr.get();") != std::string::npos);
	assert(code.find("b = __when_expr.get();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

int main() {
	std::cout << "=== Wait Compilation Tests ===\n\n";

	try {
		testSimpleWaitCompilation();
		testWaitWithLocalResult();
		testWaitWithErrorHandler();
		testMultipleWaits();

		std::cout << "All tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
