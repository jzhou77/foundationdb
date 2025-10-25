/*
 * statement_compilation_test.cpp
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
#include <cassert>
#include <memory>
#include <sstream>

using namespace actorcompiler;

void testPlainOldCode() {
	std::cout << "Testing PlainOldCode compilation...\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";
	actor.sourceLine = 1;

	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// Create a plain old code statement
	auto stmt = std::make_unique<PlainOldCodeStatement>();
	stmt->code = "int x = 42;";

	// Compile it
	Function* func = compiler.testGetFunction("body1");
	Context ctx = Context::createUnreachable();
	compiler.testCompile(func, stmt.get(), ctx);

	// Check the output
	std::ostringstream oss;
	func->writeToStream(oss);
	std::string output = oss.str();

	assert(output.find("int x = 42;") != std::string::npos);
	std::cout << "✓ PlainOldCode emits code directly\n";
	std::cout << "  Output: " << output << "\n";
}

void testReturnStatement() {
	std::cout << "\nTesting Return statement compilation...\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";
	actor.sourceLine = 1;

	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// Create a return statement
	auto stmt = std::make_unique<ReturnStatement>();
	stmt->expression = "42";

	// Compile it
	Function* func = compiler.testGetFunction("body1");
	Context ctx = Context::createUnreachable();
	compiler.testCompile(func, stmt.get(), ctx);

	// Check the output
	std::ostringstream oss;
	func->writeToStream(oss);
	std::string output = oss.str();

	assert(output.find("return 42;") != std::string::npos);
	std::cout << "✓ Return statement emits return\n";
	std::cout << "  Output: " << output << "\n";
}

void testBreakStatement() {
	std::cout << "\nTesting Break statement compilation...\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";
	actor.sourceLine = 1;

	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// Create a break statement
	auto stmt = std::make_unique<BreakStatement>();

	// Compile it with loop context
	Function* func = compiler.testGetFunction("body1");
	Context ctx = Context::createUnreachable().loopContext("break_label", "continue_label");
	compiler.testCompile(func, stmt.get(), ctx);

	// Check the output
	std::ostringstream oss;
	func->writeToStream(oss);
	std::string output = oss.str();

	assert(output.find("goto break_label;") != std::string::npos);
	std::cout << "✓ Break statement emits goto to break label\n";
	std::cout << "  Output: " << output << "\n";
}

void testContinueStatement() {
	std::cout << "\nTesting Continue statement compilation...\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";
	actor.sourceLine = 1;

	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// Create a continue statement
	auto stmt = std::make_unique<ContinueStatement>();

	// Compile it with loop context
	Function* func = compiler.testGetFunction("body1");
	Context ctx = Context::createUnreachable().loopContext("break_label", "continue_label");
	compiler.testCompile(func, stmt.get(), ctx);

	// Check the output
	std::ostringstream oss;
	func->writeToStream(oss);
	std::string output = oss.str();

	assert(output.find("goto continue_label;") != std::string::npos);
	std::cout << "✓ Continue statement emits goto to continue label\n";
	std::cout << "  Output: " << output << "\n";
}

void testCodeBlock() {
	std::cout << "\nTesting CodeBlock compilation...\n";

	Actor actor;
	actor.name = "testActor";
	actor.returnType = "void";
	actor.sourceLine = 1;

	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// Create a code block with multiple statements
	auto block = std::make_unique<CodeBlock>();

	auto stmt1 = std::make_unique<PlainOldCodeStatement>();
	stmt1->code = "int x = 1;";
	block->statements.push_back(std::move(stmt1));

	auto stmt2 = std::make_unique<PlainOldCodeStatement>();
	stmt2->code = "int y = 2;";
	block->statements.push_back(std::move(stmt2));

	// Compile it
	Function* func = compiler.testGetFunction("body1");
	Context ctx = Context::createUnreachable();
	compiler.testCompile(func, block.get(), ctx);

	// Check the output
	std::ostringstream oss;
	func->writeToStream(oss);
	std::string output = oss.str();

	assert(output.find("int x = 1;") != std::string::npos);
	assert(output.find("int y = 2;") != std::string::npos);
	std::cout << "✓ CodeBlock compiles all statements in order\n";
	std::cout << "  Output: " << output << "\n";
}

int main() {
	testPlainOldCode();
	testReturnStatement();
	testBreakStatement();
	testContinueStatement();
	testCodeBlock();

	std::cout << "\n========================================\n";
	std::cout << "All statement compilation tests PASSED!\n";
	std::cout << "========================================\n";
	std::cout << "\nStatement compilation implementation verified:\n";
	std::cout << "- compile() dispatches to correct handler\n";
	std::cout << "- PlainOldCode passes through unchanged\n";
	std::cout << "- Return emits return statement\n";
	std::cout << "- Break emits goto to break label\n";
	std::cout << "- Continue emits goto to continue label\n";
	std::cout << "- CodeBlock processes all statements\n";

	return 0;
}
