/*
 * ParseTree.h
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

#ifndef ACTORCOMPILER_PARSETREE_H
#define ACTORCOMPILER_PARSETREE_H

#include <string>
#include <vector>
#include <memory>

namespace actorcompiler {

// Variable declaration (type, name, initializer)
struct VarDeclaration {
	std::string type;
	std::string name;
	std::string initializer;
	bool initializerConstructorSyntax = false;
};

// Abstract base class for all statement types in the AST
class Statement {
public:
	int firstSourceLine = 0;

	virtual ~Statement() = default;
	virtual bool containsWait() const { return false; }
	// Debug string representation similar to C# ToString()
	virtual std::string toString() const { return std::string(); }
};

// Plain C++ code statement (pass-through)
class PlainOldCodeStatement : public Statement {
public:
	std::string code;
	std::string toString() const override { return code; }
};

// State variable declaration
class StateDeclarationStatement : public Statement {
public:
	VarDeclaration decl;
	std::string toString() const override {
		if (decl.initializerConstructorSyntax)
			return std::string("State ") + decl.type + " " + decl.name + "(" + decl.initializer + ");";
		else
			return std::string("State ") + decl.type + " " + decl.name + " = " + decl.initializer + ";";
	}
};

// Forward declarations for compound statements
class CodeBlock;

// While loop
class WhileStatement : public Statement {
public:
	std::string expression;
	std::unique_ptr<Statement> body;

	bool containsWait() const override;
	std::string toString() const override { return std::string("While ") + expression; }
};

// For loop (traditional 3-part)
class ForStatement : public Statement {
public:
	std::string initExpression;
	std::string condExpression;
	std::string nextExpression;
	std::unique_ptr<Statement> body;

	bool containsWait() const override;
};

// Range-based for loop (C++11 style)
class RangeForStatement : public Statement {
public:
	std::string rangeExpression;
	std::string rangeDecl;
	std::unique_ptr<Statement> body;

	bool containsWait() const override;
};

// Infinite loop
class LoopStatement : public Statement {
public:
	std::unique_ptr<Statement> body;

	bool containsWait() const override;
	std::string toString() const override { return std::string("Loop ") + (body ? body->toString() : std::string("")); }
};

// Break statement
class BreakStatement : public Statement {};

// Continue statement
class ContinueStatement : public Statement {};

// If/else statement
class IfStatement : public Statement {
public:
	std::string expression;
	bool constexpr_ = false; // constexpr is a keyword
	std::unique_ptr<Statement> ifBody;
	std::unique_ptr<Statement> elseBody; // may be null

	bool containsWait() const override;
};

// Return statement
class ReturnStatement : public Statement {
public:
	std::string expression;
	std::string toString() const override { return std::string("Return ") + expression; }
};

// Wait statement (Flow-specific)
class WaitStatement : public Statement {
public:
	VarDeclaration result;
	std::string futureExpression;
	bool resultIsState = false;
	bool isWaitNext = false;

	bool containsWait() const override { return true; }
	std::string toString() const override {
		return std::string("Wait ") + result.type + " " + result.name + " <- " + futureExpression + " (" +
		       (resultIsState ? "state" : "local") + ")";
	}
};

// Choose statement (Flow-specific)
class ChooseStatement : public Statement {
public:
	std::unique_ptr<Statement> body;

	bool containsWait() const override;
	std::string toString() const override {
		return std::string("Choose ") + (body ? body->toString() : std::string(""));
	}
};

// When statement inside choose (Flow-specific)
class WhenStatement : public Statement {
public:
	std::unique_ptr<WaitStatement> wait;
	std::unique_ptr<Statement> body;

	bool containsWait() const override { return true; }
	std::string toString() const override {
		return std::string("When (") + (wait ? wait->toString() : std::string("")) + ") " +
		       (body ? body->toString() : std::string(""));
	}
};

// Try/catch statement
class TryStatement : public Statement {
public:
	struct Catch {
		std::string expression;
		std::unique_ptr<Statement> body;
		int firstSourceLine = 0;
	};

	std::unique_ptr<Statement> tryBody;
	std::vector<Catch> catches;

	bool containsWait() const override;
};

// Throw statement
class ThrowStatement : public Statement {
public:
	std::string expression;
};

// Code block (sequence of statements)
class CodeBlock : public Statement {
public:
	std::vector<std::unique_ptr<Statement>> statements;

	bool containsWait() const override;
	std::string toString() const override;
};

// DESCR declaration (for descriptor metadata)
struct Declaration {
	std::string type;
	std::string name;
	std::string comment;
};

// Actor function declaration
class Actor {
public:
	std::vector<std::string> attributes;
	std::string returnType; // empty if void
	std::string name;
	std::string enclosingClass; // if nested in a class
	std::vector<VarDeclaration> parameters;
	std::vector<VarDeclaration> templateFormals; // empty if not a template
	std::unique_ptr<CodeBlock> body;
	int sourceLine = 0;
	bool isStatic = false;
	bool isUncancellable = false;
	std::string testCaseParameters; // non-empty for TEST_CASE
	std::string nameSpace;
	bool isForwardDeclaration = false;
	bool isTestCase = false;

	bool isCancellable() const { return !returnType.empty() && !isUncancellable; }
	void setUncancellable() { isUncancellable = true; }
};

// DESCR structure declaration
struct Descr {
	std::string name;
	std::string superClassList;
	std::vector<Declaration> body;
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_PARSETREE_H
