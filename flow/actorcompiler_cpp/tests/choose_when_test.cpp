/*
 * choose_when_test.cpp
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

void testSimpleChoose() {
	std::cout << "Test 1: Simple choose with two when clauses\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create choose statement with two when clauses
	auto chooseStmt = std::make_unique<ChooseStatement>();
	auto codeBlock = std::make_unique<CodeBlock>();

	// First when: when (int x = wait(getFuture1())) { process1(x); }
	auto when1 = std::make_unique<WhenStatement>();
	when1->wait = std::make_unique<WaitStatement>();
	when1->wait->result.type = "int";
	when1->wait->result.name = "x";
	when1->wait->futureExpression = "getFuture1()";
	when1->wait->resultIsState = true;
	when1->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(when1->body.get())->code = "process1(x);";

	// Second when: when (std::string y = wait(getFuture2())) { process2(y); }
	auto when2 = std::make_unique<WhenStatement>();
	when2->wait = std::make_unique<WaitStatement>();
	when2->wait->result.type = "std::string";
	when2->wait->result.name = "y";
	when2->wait->futureExpression = "getFuture2()";
	when2->wait->resultIsState = false;
	when2->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(when2->body.get())->code = "process2(y);";

	codeBlock->statements.push_back(std::move(when1));
	codeBlock->statements.push_back(std::move(when2));
	chooseStmt->body = std::move(codeBlock);

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, chooseStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify structure
	assert(code.find("BEGIN choose block") != std::string::npos);
	assert(code.find("__when_expr_0") != std::string::npos);
	assert(code.find("__when_expr_1") != std::string::npos);
	assert(code.find("StrictFuture<int> __when_expr_0 = getFuture1()") != std::string::npos);
	assert(code.find("StrictFuture<std::string> __when_expr_1 = getFuture2()") != std::string::npos);
	assert(code.find("if (__when_expr_0.isReady())") != std::string::npos);
	assert(code.find("if (__when_expr_1.isReady())") != std::string::npos);
	assert(code.find("process1(x);") != std::string::npos);
	assert(code.find("process2(y);") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testChooseWithErrorHandler() {
	std::cout << "Test 2: Choose with error handler\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create choose with one when clause
	auto chooseStmt = std::make_unique<ChooseStatement>();
	auto codeBlock = std::make_unique<CodeBlock>();

	auto when1 = std::make_unique<WhenStatement>();
	when1->wait = std::make_unique<WaitStatement>();
	when1->wait->result.type = "int";
	when1->wait->result.name = "result";
	when1->wait->futureExpression = "getFuture()";
	when1->wait->resultIsState = true;
	when1->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(when1->body.get())->code = "handleResult(result);";

	codeBlock->statements.push_back(std::move(when1));
	chooseStmt->body = std::move(codeBlock);

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable().withCatch("err", "errCode", "catchHandler");

	compiler.testCompile(func, chooseStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify error handling
	assert(code.find("err = __when_expr_0.getError()") != std::string::npos);
	assert(code.find("goto catchHandler") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testChooseWithThreeWhen() {
	std::cout << "Test 3: Choose with three when clauses\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto chooseStmt = std::make_unique<ChooseStatement>();
	auto codeBlock = std::make_unique<CodeBlock>();

	// Create three when clauses
	for (int i = 0; i < 3; ++i) {
		auto when = std::make_unique<WhenStatement>();
		when->wait = std::make_unique<WaitStatement>();
		when->wait->result.type = "int";
		when->wait->result.name = "val" + std::to_string(i);
		when->wait->futureExpression = "getFuture" + std::to_string(i) + "()";
		when->wait->resultIsState = true;
		when->body = std::make_unique<PlainOldCodeStatement>();
		static_cast<PlainOldCodeStatement*>(when->body.get())->code =
		    "handle" + std::to_string(i) + "(val" + std::to_string(i) + ");";
		codeBlock->statements.push_back(std::move(when));
	}

	chooseStmt->body = std::move(codeBlock);

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, chooseStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Verify all three when clauses present
	assert(code.find("__when_expr_0") != std::string::npos);
	assert(code.find("__when_expr_1") != std::string::npos);
	assert(code.find("__when_expr_2") != std::string::npos);
	assert(code.find("handle0(val0)") != std::string::npos);
	assert(code.find("handle1(val1)") != std::string::npos);
	assert(code.find("handle2(val2)") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testChooseWithEmptyWhenBody() {
	std::cout << "Test 4: Choose with empty when body\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto chooseStmt = std::make_unique<ChooseStatement>();
	auto codeBlock = std::make_unique<CodeBlock>();

	// When with no body (just waits for the future)
	auto when1 = std::make_unique<WhenStatement>();
	when1->wait = std::make_unique<WaitStatement>();
	when1->wait->result.type = "Void";
	when1->wait->result.name = "v";
	when1->wait->futureExpression = "signal.getFuture()";
	when1->wait->resultIsState = true;
	when1->body = nullptr; // No body

	codeBlock->statements.push_back(std::move(when1));
	chooseStmt->body = std::move(codeBlock);

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, chooseStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Should compile without body
	assert(code.find("StrictFuture<Void> __when_expr_0 = signal.getFuture()") != std::string::npos);
	assert(code.find("v = __when_expr_0.get()") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

int main() {
	std::cout << "=== Choose/When Compilation Tests ===\n\n";

	try {
		testSimpleChoose();
		testChooseWithErrorHandler();
		testChooseWithThreeWhen();
		testChooseWithEmptyWhenBody();

		std::cout << "All tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
