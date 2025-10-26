// error_handling_test.cpp - Test error handling through callbacks
// Tests that error callbacks properly route errors through try/catch handlers

#include "../ActorCompiler.h"
#include "../ActorParser.h"
#include "../Error.h"
#include <cassert>
#include <iostream>
#include <sstream>

using namespace actorcompiler;

void testErrorCallbackRouting() {
	std::cout << "Test: Error callback routes to catch handler\n";

	std::string sourceCode = R"(
ACTOR Future<int> withErrorHandling() {
	try {
		state int x = wait(riskyOperation());
		return x;
	} catch (Error& e) {
		return -1;
	}
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();

	// Verify error callback exists
	assert(code.find("a_callback_error") != std::string::npos);

	// Verify error is stored in error variable (from catch context)
	assert(code.find("this->e = err") != std::string::npos ||
	       code.find("this->__current_error = err") != std::string::npos);

	// Verify error callback calls body(0)
	assert(code.find("this->body(0)") != std::string::npos);

	// Verify resume point checks for error
	assert(code.find(".code() != invalid_error_code") != std::string::npos);

	// Verify goto to catch handler
	assert(code.find("goto cont") != std::string::npos);

	std::cout << "✓ Test passed - error callback routes through catch handler\n\n";
}

void testErrorCallbackWithoutCatchHandler() {
	std::cout << "Test: Error callback without catch handler throws\n";

	std::string sourceCode = R"(
ACTOR Future<int> withoutErrorHandling() {
	state int x = wait(riskyOperation());
	return x;
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();

	// Verify error callback exists
	assert(code.find("a_callback_error") != std::string::npos);

	// Without catch handler, should throw the error
	assert(code.find("throw err") != std::string::npos);

	std::cout << "✓ Test passed - error callback throws without catch handler\n\n";
}

void testMultipleWaitsWithErrorHandling() {
	std::cout << "Test: Multiple waits with error handling\n";

	std::string sourceCode = R"(
ACTOR Future<int> multipleWaitsWithError() {
	try {
		state int x = wait(operation1());
		state int y = wait(operation2());
		return x + y;
	} catch (Error& e) {
		return -1;
	}
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();

	// Should have two callbacks (one per wait)
	int callbackCount = 0;
	size_t pos = 0;
	while ((pos = code.find("a_callback_error", pos)) != std::string::npos) {
		callbackCount++;
		pos += 16; // strlen("a_callback_error")
	}

	assert(callbackCount >= 2);

	// Both should route through catch handler
	assert(code.find("this->e = err") != std::string::npos ||
	       code.find("this->__current_error = err") != std::string::npos);

	std::cout << "✓ Test passed - multiple waits share error handler\n\n";
}

void testFastPathErrorHandling() {
	std::cout << "Test: Fast path error handling (ready future with error)\n";

	std::string sourceCode = R"(
ACTOR Future<int> withFastPathError() {
	try {
		state int x = wait(immediateError());
		return x;
	} catch (Error& e) {
		return -1;
	}
}
)";

	ErrorMessagePolicy policy;
	ActorParser parser(sourceCode, "test.actor.cpp", policy, false);

	std::ostringstream output;
	parser.write(output, "out.cpp");

	std::string code = output.str();

	// Fast path should check isError() on ready future
	assert(code.find("isError()") != std::string::npos);

	// Should store error and goto catch handler (fast path)
	assert(code.find("getError()") != std::string::npos);
	assert(code.find("goto cont") != std::string::npos);

	std::cout << "✓ Test passed - fast path error handling present\n\n";
}

int main() {
	std::cout << "=== Error Handling Integration Tests ===\n\n";

	try {
		testErrorCallbackRouting();
		testErrorCallbackWithoutCatchHandler();
		testMultipleWaitsWithErrorHandling();
		testFastPathErrorHandling();

		std::cout << "✅ All error handling tests passed!\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "❌ Test failed with exception: " << e.what() << "\n";
		return 1;
	}
}
