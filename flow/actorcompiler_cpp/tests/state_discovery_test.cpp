/*
 * state_discovery_test.cpp
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

#include "../ParseTree.h"
#include "../ActorCompiler.h"
#include <iostream>
#include <memory>
#include <cassert>

using namespace actorcompiler;

int main() {
	// Create a simple actor with state variables
	Actor actor;
	actor.name = "testActor";
	actor.returnType = "int";
	actor.sourceLine = 1;

	// Create body with state declarations
	auto body = std::make_unique<CodeBlock>();

	// Add state int x;
	auto stateDecl1 = std::make_unique<StateDeclarationStatement>();
	stateDecl1->decl.type = "int";
	stateDecl1->decl.name = "x";
	body->statements.push_back(std::move(stateDecl1));

	// Add state std::string message;
	auto stateDecl2 = std::make_unique<StateDeclarationStatement>();
	stateDecl2->decl.type = "std::string";
	stateDecl2->decl.name = "message";
	body->statements.push_back(std::move(stateDecl2));

	// Add nested state in if
	auto ifStmt = std::make_unique<IfStatement>();
	ifStmt->expression = "true";
	auto ifBody = std::make_unique<CodeBlock>();

	auto stateDecl3 = std::make_unique<StateDeclarationStatement>();
	stateDecl3->decl.type = "double";
	stateDecl3->decl.name = "value";
	ifBody->statements.push_back(std::move(stateDecl3));

	ifStmt->ifBody = std::move(ifBody);
	body->statements.push_back(std::move(ifStmt));

	actor.body = std::move(body);

	// Create compiler and check state discovery
	ActorCompiler compiler(actor, "test.actor.cpp", true, false, false);

	// The constructor should have discovered all three state variables
	const auto& stateVars = compiler.getStateVariables();

	// Verify we found all three state variables
	assert(stateVars.size() == 3);
	assert(stateVars.count("x") == 1);
	assert(stateVars.count("message") == 1);
	assert(stateVars.count("value") == 1);

	std::cout << "State discovery test PASSED!\n";
	std::cout << "Found " << stateVars.size() << " state variables: ";
	for (const auto& var : stateVars) {
		std::cout << var << " ";
	}
	std::cout << "\n";

	return 0;
}