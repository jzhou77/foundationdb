/*
 * loop_compilation_test.cpp
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

void testIfStatement() {
	std::cout << "Test 1: Simple if statement\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create if statement: if (x > 0) { doSomething(); }
	auto ifStmt = std::make_unique<IfStatement>();
	ifStmt->expression = "x > 0";
	ifStmt->ifBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(ifStmt->ifBody.get())->code = "doSomething();";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, ifStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("if (x > 0)") != std::string::npos);
	assert(code.find("doSomething();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testIfElseStatement() {
	std::cout << "Test 2: If-else statement\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create if-else: if (x > 0) { positive(); } else { negative(); }
	auto ifStmt = std::make_unique<IfStatement>();
	ifStmt->expression = "x > 0";
	ifStmt->ifBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(ifStmt->ifBody.get())->code = "positive();";
	ifStmt->elseBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(ifStmt->elseBody.get())->code = "negative();";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, ifStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("if (x > 0)") != std::string::npos);
	assert(code.find("positive();") != std::string::npos);
	assert(code.find("else") != std::string::npos);
	assert(code.find("negative();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testConstexprIf() {
	std::cout << "Test 3: Constexpr if statement\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	auto ifStmt = std::make_unique<IfStatement>();
	ifStmt->expression = "sizeof(T) > 4";
	ifStmt->constexpr_ = true;
	ifStmt->ifBody = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(ifStmt->ifBody.get())->code = "bigType();";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, ifStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("if constexpr (sizeof(T) > 4)") != std::string::npos);
	assert(code.find("bigType();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testForLoop() {
	std::cout << "Test 4: For loop\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create for loop: for (int i = 0; i < 10; ++i) { process(i); }
	auto forStmt = std::make_unique<ForStatement>();
	forStmt->initExpression = "int i = 0";
	forStmt->condExpression = "i < 10";
	forStmt->nextExpression = "++i";
	forStmt->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(forStmt->body.get())->code = "process(i);";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, forStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("int i = 0;") != std::string::npos);
	assert(code.find("if (!(i < 10))") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos); // break label
	assert(code.find("process(i);") != std::string::npos);
	assert(code.find("++i;") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testWhileLoop() {
	std::cout << "Test 5: While loop\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create while loop: while (hasMore()) { processNext(); }
	auto whileStmt = std::make_unique<WhileStatement>();
	whileStmt->expression = "hasMore()";
	whileStmt->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(whileStmt->body.get())->code = "processNext();";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, whileStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("if (!(hasMore()))") != std::string::npos);
	assert(code.find("processNext();") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testInfiniteLoop() {
	std::cout << "Test 6: Infinite loop\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create loop: loop { doWork(); }
	auto loopStmt = std::make_unique<LoopStatement>();
	loopStmt->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(loopStmt->body.get())->code = "doWork();";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, loopStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	// Infinite loop should have loop structure without condition check
	assert(code.find("doWork();") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testLoopWithBreak() {
	std::cout << "Test 7: Loop with break statement\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create loop with break
	auto loopStmt = std::make_unique<LoopStatement>();
	auto codeBlock = std::make_unique<CodeBlock>();

	auto work = std::make_unique<PlainOldCodeStatement>();
	work->code = "doWork();";

	auto breakStmt = std::make_unique<BreakStatement>();

	codeBlock->statements.push_back(std::move(work));
	codeBlock->statements.push_back(std::move(breakStmt));

	loopStmt->body = std::move(codeBlock);

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, loopStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("doWork();") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos); // break goes to break label

	std::cout << "✓ Test passed\n\n";
}

void testRangeForLoop() {
	std::cout << "Test 8: Range-based for loop\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";

	ActorCompiler compiler(actor, "test.actor.cpp", true, true, false);

	// Create range-for: for (auto& item : container) { process(item); }
	auto rangeForStmt = std::make_unique<RangeForStatement>();
	rangeForStmt->rangeDecl = "auto& item";
	rangeForStmt->rangeExpression = "container";
	rangeForStmt->body = std::make_unique<PlainOldCodeStatement>();
	static_cast<PlainOldCodeStatement*>(rangeForStmt->body.get())->code = "process(item);";

	Function* func = compiler.testGetFunction("test");
	Context ctx = Context::createUnreachable();

	compiler.testCompile(func, rangeForStmt.get(), ctx);

	std::ostringstream output;
	func->writeToStream(output);
	std::string code = output.str();

	std::cout << "Generated code:\n" << code << "\n";

	assert(code.find("for (auto& item : container)") != std::string::npos);
	assert(code.find("process(item);") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

int main() {
	std::cout << "=== Loop and Control Flow Compilation Tests ===\n\n";

	try {
		testIfStatement();
		testIfElseStatement();
		testConstexprIf();
		testForLoop();
		testWhileLoop();
		testInfiniteLoop();
		testLoopWithBreak();
		testRangeForLoop();

		std::cout << "All tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
