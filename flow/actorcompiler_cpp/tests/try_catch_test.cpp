/*
 * try_catch_test.cpp
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

void testSimpleTryCatch() {
	std::cout << "Test 1: Simple try/catch with named error variable\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create try/catch: try { risky(); } catch (Error& e) { handle(e); }
	auto tryStmt = std::make_unique<TryStatement>();

	tryStmt->tryBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(tryStmt->tryBody.get())->code = "risky();";

	TryStatement::Catch catchClause;
	catchClause.expression = "Error& e";
	catchClause.body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(catchClause.body.get())->code = "handle(e);";

	tryStmt->catches.push_back(std::move(catchClause));

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, tryStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify structure
	assert(code.find("BEGIN try block") != std::string::npos);
	assert(code.find("try {") != std::string::npos);
	assert(code.find("risky();") != std::string::npos);
	assert(code.find("catch (Error& e)") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos);
	assert(code.find("catch (...)") != std::string::npos);
	assert(code.find("e = unknown_error()") != std::string::npos);
	assert(code.find("handle(e);") != std::string::npos);
	assert(code.find("END try block") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testTryCatchEllipsis() {
	std::cout << "Test 2: Try/catch with ellipsis (...)\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto tryStmt = std::make_unique<TryStatement>();

	tryStmt->tryBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(tryStmt->tryBody.get())->code = "doWork();";

	TryStatement::Catch catchClause;
	catchClause.expression = "...";
	catchClause.body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(catchClause.body.get())->code = "cleanup();";

	tryStmt->catches.push_back(std::move(catchClause));

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, tryStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify default error variable name used
	assert(code.find("catch (Error& __current_error)") != std::string::npos);
	assert(code.find("doWork();") != std::string::npos);
	assert(code.find("cleanup();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testThrowStatement() {
	std::cout << "Test 3: Throw statement with expression\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create throw statement
	auto throwStmt = std::make_unique<ThrowStatement>();
	throwStmt->expression = "operation_failed()";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, throwStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Without catch context, should use C++ throw
	assert(code.find("throw operation_failed()") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testThrowWithCatchContext() {
	std::cout << "Test 4: Throw statement within catch context\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto throwStmt = std::make_unique<ThrowStatement>();
	throwStmt->expression = "my_error()";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable().withCatch("err", "errCode", "errorHandler");

	compiler.testCompile(func, throwStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// With catch context, should assign error and goto handler
	assert(code.find("err = my_error()") != std::string::npos);
	assert(code.find("goto errorHandler") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testRethrow() {
	std::cout << "Test 5: Re-throw (throw with no expression)\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto throwStmt = std::make_unique<ThrowStatement>();
	throwStmt->expression = ""; // Empty means re-throw

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable().withCatch("currentErr", "errCode", "catchLabel");

	compiler.testCompile(func, throwStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Re-throw should just goto catch handler
	assert(code.find("goto catchLabel") != std::string::npos);
	assert(code.find("re-throw") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testTryCatchWithWait() {
	std::cout << "Test 6: Try/catch with wait statement inside\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto tryStmt = std::make_unique<TryStatement>();

	// Try body contains a wait statement
	auto waitStmt = std::make_unique<WaitStatement>();
	waitStmt->result.type = "int";
	waitStmt->result.name = "x";
	waitStmt->futureExpression = "getFuture()";
	waitStmt->resultIsState = true;

	tryStmt->tryBody = std::move(waitStmt);

	TryStatement::Catch catchClause;
	catchClause.expression = "Error& err";
	catchClause.body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(catchClause.body.get())->code = "logError(err);";

	tryStmt->catches.push_back(std::move(catchClause));

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, tryStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify try block contains wait logic and catch handler
	assert(code.find("try {") != std::string::npos);
	assert(code.find("StrictFuture<int> __when_expr = getFuture()") != std::string::npos);
	assert(code.find("catch (Error& err)") != std::string::npos);
	assert(code.find("logError(err);") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testNestedTryCatch() {
	std::cout << "Test 7: Nested try/catch blocks\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Outer try/catch
	auto outerTry = std::make_unique<TryStatement>();

	// Inner try/catch as part of outer try body
	auto innerTry = std::make_unique<TryStatement>();
	innerTry->tryBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(innerTry->tryBody.get())->code = "innerRisky();";

	TryStatement::Catch innerCatch;
	innerCatch.expression = "Error& innerErr";
	innerCatch.body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(innerCatch.body.get())->code = "handleInner(innerErr);";
	innerTry->catches.push_back(std::move(innerCatch));

	outerTry->tryBody = std::move(innerTry);

	TryStatement::Catch outerCatch;
	outerCatch.expression = "Error& outerErr";
	outerCatch.body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(outerCatch.body.get())->code = "handleOuter(outerErr);";
	outerTry->catches.push_back(std::move(outerCatch));

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, outerTry.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify nested structure
	assert(code.find("innerRisky();") != std::string::npos);
	assert(code.find("catch (Error& innerErr)") != std::string::npos);
	assert(code.find("handleInner(innerErr);") != std::string::npos);
	assert(code.find("catch (Error& outerErr)") != std::string::npos);
	assert(code.find("handleOuter(outerErr);") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

int main() {
	std::cout << "=== Try/Catch Compilation Tests ===\n\n";

	try {
		testSimpleTryCatch();
		testTryCatchEllipsis();
		testThrowStatement();
		testThrowWithCatchContext();
		testRethrow();
		testTryCatchWithWait();
		testNestedTryCatch();

		std::cout << "All tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
