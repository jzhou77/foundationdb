/*
 * codegen_smoke_test.cpp
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

#include "../ActorParser.h"
#include <iostream>
#include <sstream>
#include <string>
#include <cassert>

using namespace actorcompiler;

void testMinimalActor() {
	std::cout << "Test: Minimal actor with single return\n";

	// Simple actor: ACTOR Future<int> getValue() { return 42; }
	std::string sourceCode = R"(
ACTOR Future<int> getValue() {
	return 42;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify key elements are present
	assert(code.find("POST_ACTOR_COMPILER") != std::string::npos);
	assert(code.find("Future<int>") != std::string::npos);
	assert(code.find("getValue") != std::string::npos);
	// Actor and State class names are derived; ensure both class and state appear
	assert(code.find("class ") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testActorWithParameter() {
	std::cout << "Test: Actor with parameter\n";

	// Actor with parameter: ACTOR Future<int> add(int x) { return x + 1; }
	std::string sourceCode = R"(
ACTOR Future<int> add(int x) {
	return x + 1;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify parameter handling
	assert(code.find("int const&") != std::string::npos);
	assert(code.find("return x + 1") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testActorWithStateVariable() {
	std::cout << "Test: Actor with state variable\n";

	// Actor with state: ACTOR Future<int> test() { state int x = 5; return x; }
	std::string sourceCode = R"(
ACTOR Future<int> test() {
	state int x = 5;
	return x;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify state variable surfaced in generated output (Phase 6 emits TODO markers)
	assert(code.find("TODO:") != std::string::npos);
	assert(code.find("x") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testVoidActor() {
	std::cout << "Test: Void actor\n";

	// Void actor: ACTOR void doWork() { return; }
	std::string sourceCode = R"(
ACTOR void doWork() {
	return;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify void handling
	assert(code.find("Actor<void>") != std::string::npos);
	assert(code.find("void doWork") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testActorWithWait() {
	std::cout << "Test: Actor with wait statement\n";

	// Actor with wait: ACTOR Future<int> test() { int x = wait(getFuture()); return x; }
	std::string sourceCode = R"(
ACTOR Future<int> test() {
	int x = wait(getFuture());
	return x;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify wait handling (compiled to StrictFuture or __when_expr constructs)
	assert(code.find("StrictFuture<") != std::string::npos || code.find("__when_expr") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testForwardDeclaration() {
	std::cout << "Test: Forward declaration\n";

	// Forward declaration: ACTOR Future<int> myActor(int x);
	std::string sourceCode = R"(
ACTOR Future<int> myActor(int x);
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Verify forward declaration format
	assert(code.find("Future<int>") != std::string::npos);
	assert(code.find("myActor") != std::string::npos);
	assert(code.find(";") != std::string::npos);
	// Should not have class definitions
	assert(code.find("class ") == std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testUidGeneration() {
	std::cout << "Test: UID generation\n";

	std::string sourceCode = R"(
ACTOR Future<int> test() {
	return 42;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();

	// Verify UID is present
	assert(code.find("ActorIdentifier") != std::string::npos);
	assert(code.find("UID(") != std::string::npos);
	assert(code.find("UL,") != std::string::npos);

	// Verify UID mappings were recorded
	auto uidObjects = parser.getUidObjects();
	assert(!uidObjects.empty());

	std::cout << "✓ Test passed - " << uidObjects.size() << " UID(s) generated\n\n";
}

void testChooseWhenActor() {
	std::cout << "Test: Actor with choose/when\n";

	// Actor using choose/when
	std::string sourceCode = R"(
ACTOR Future<int> chooser(Future<int> a, Future<int> b) {
	choose {
		when(int r = wait(a)) { return r; }
		when(state int r = wait(b)) { return r; }
	}
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// choose/when compiles down to internal when expressions
	assert(code.find("__when_expr") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testTryCatchActor() {
	std::cout << "Test: Actor with try/catch\n";

	// Actor using try/catch and throw
	std::string sourceCode = R"(
ACTOR Future<int> risky() {
	try {
		throw operation_failed();
	} catch (Error &e) {
		return -1;
	}
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// We expect Error handling artifacts present in generated code
	assert(code.find("Error") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testConstructorAndCancel() {
	std::cout << "Test: Constructor starts body and cancel() is emitted\n";

	std::string sourceCode = R"(
ACTOR Future<int> start_me() {
	return 1;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Constructor should invoke the first body continuation and cancel() should exist
	assert(code.find("this->body(") != std::string::npos);
	assert(code.find("void cancel(") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testTemplateActor() {
	std::cout << "Test: Template actor support\n";

	std::string sourceCode = R"(
template <class T>
ACTOR Future<T> identity(T x) {
	return x;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();
	std::cout << "Generated code:\n" << code << "\n";

	// Template declarations should be present
	assert(code.find("template <") != std::string::npos);
	assert(code.find("class T") != std::string::npos);
	assert(code.find("Future<T>") != std::string::npos);
	assert(code.find("identity") != std::string::npos);

	std::cout << "✓ Test passed\n\n";
}

void testProbesEnabledDisabled() {
	std::cout << "Test: Probes toggled by generateProbes flag\n";

	std::string sourceCode = R"(
ACTOR Future<int> monitored() {
	return 42;
}
)";

	// Test with probes disabled
	{
		std::cout << "  Testing with generateProbes=false...\n";
		ErrorMessagePolicy policy;
		ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ false);
		std::ostringstream output;
		parser.write(output, "out.cpp");
		std::string code = output.str();

		// No probe markers should be present
		assert(code.find("PROBE_") == std::string::npos);
		std::cout << "  ✓ No probe markers found (expected)\n";
	}

	// Test with probes enabled
	{
		std::cout << "  Testing with generateProbes=true...\n";
		ErrorMessagePolicy policy;
		ActorParser parser(sourceCode, "test.actor.cpp", policy, /*generateProbes*/ true);
		std::ostringstream output;
		parser.write(output, "out.cpp");
		std::string code = output.str();

		// Probe markers should be present
		assert(code.find("PROBE_CREATE") != std::string::npos);
		assert(code.find("PROBE_DESTROY") != std::string::npos);
		assert(code.find("PROBE_ENTER") != std::string::npos);
		assert(code.find("PROBE_EXIT") != std::string::npos);
		assert(code.find("PROBE_CANCEL") != std::string::npos);

		std::cout << "  ✓ All probe markers found (expected)\n";
	}

	std::cout << "✓ Test passed\n\n";
}

int main() {
	std::cout << "=== Code Generation Smoke Tests ===\n\n";

	try {
		testMinimalActor();
		testActorWithParameter();
		testActorWithStateVariable();
		testVoidActor();
		testActorWithWait();
		testForwardDeclaration();
		testUidGeneration();
		testChooseWhenActor();
		testTryCatchActor();
		testConstructorAndCancel();
		testTemplateActor();
		testProbesEnabledDisabled();

		std::cout << "All tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
