/*
 * ParseTree.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#ifndef ACTOR_COMPILER_PARSE_TREE_H
#define ACTOR_COMPILER_PARSE_TREE_H

#include <string>
#include <vector>
#include <memory>

namespace actorcompiler {

// Custom hash function for pair<uint64_t, uint64_t>
struct PairHash {
	std::size_t operator()(const std::pair<uint64_t, uint64_t>& p) const {
		// Combine the hash values of the pair elements
		auto h1 = std::hash<uint64_t>{}(p.first);
		auto h2 = std::hash<uint64_t>{}(p.second);
		return h1 ^ (h2 << 1);
	}
};

struct VarDeclaration {
public:
	std::string type;
	std::string name;
	std::string initializer;
	bool initializerConstructorSyntax;
};

class Statement {
public:
	int firstSourceLine;
	virtual bool containsWait() const { return false; }
	virtual ~Statement() = default;
	virtual std::string toString() const { return "[Statement]"; }
};

class PlainOldCodeStatement : public Statement {
public:
	std::string code;
	PlainOldCodeStatement(std::string code) : code(code) {}

	std::string toString() const override { return "[PlainOld]: " + code; }
};

class StateDeclarationStatement : public Statement {
public:
	VarDeclaration decl;
	StateDeclarationStatement(VarDeclaration decl) : decl(decl) {}

	std::string toString() const override {
		if (decl.initializerConstructorSyntax)
			return "State " + decl.type + " " + decl.name + "(" + decl.initializer + ");";
		else
			return "State " + decl.type + " " + decl.name + " = " + decl.initializer + ";";
	}
};

class WhileStatement : public Statement {
public:
	std::string expression;
	std::shared_ptr<Statement> body;
	WhileStatement(std::string expression, std::shared_ptr<Statement> body) : expression(expression), body(body) {}

	bool containsWait() const override { return body->containsWait(); }
	std::string toString() const override { return "[While] " + expression + " " + body->toString(); }
};

class ForStatement : public Statement {
public:
	std::string initExpression = "";
	std::string condExpression = "";
	std::string nextExpression = "";
	std::shared_ptr<Statement> body;

	ForStatement(std::string initExpression,
	             std::string condExpression,
	             std::string nextExpression,
	             std::shared_ptr<Statement> body)
	  : initExpression(initExpression), condExpression(condExpression), nextExpression(nextExpression), body(body) {}

	bool containsWait() const override { return body->containsWait(); }
	std::string toString() const override {
		return "[For] " + initExpression + "; " + condExpression + "; " + nextExpression + " " + body->toString();
	}
};

class RangeForStatement : public Statement {
public:
	std::string rangeExpression;
	std::string rangeDecl;
	std::shared_ptr<Statement> body;

	RangeForStatement(std::string rangeExpression, std::string rangeDecl, std::shared_ptr<Statement> body)
	  : rangeExpression(rangeExpression), rangeDecl(rangeDecl), body(body) {}
	bool containsWait() const override { return body->containsWait(); }
	std::string toString() const override {
		return "[RangeFor] " + rangeDecl + " : " + rangeExpression + " " + body->toString();
	}
};

class LoopStatement : public Statement {
public:
	std::shared_ptr<Statement> body;
	LoopStatement(std::shared_ptr<Statement> body) : body(body) {}

	std::string toString() const override { return "[Loop] " + body->toString(); }
	bool containsWait() const override { return body->containsWait(); }
};

class BreakStatement : public Statement {
public:
	std::string toString() const override { return "[Break]"; }
};

class ContinueStatement : public Statement {
public:
	std::string toString() const override { return "[Continue]"; }
};

class IfStatement : public Statement {
public:
	std::string expression;
	bool _constexpr;
	std::shared_ptr<Statement> ifBody;
	std::shared_ptr<Statement> elseBody; // might be null

	IfStatement(std::string expression,
	            bool _constexpr,
	            std::shared_ptr<Statement> ifBody,
	            std::shared_ptr<Statement> elseBody)
	  : expression(expression), _constexpr(_constexpr), ifBody(ifBody), elseBody(elseBody) {}

	std::string toString() const override {
		std::string result = "[If] " + expression + " " + ifBody->toString();
		if (elseBody)
			result += " else " + elseBody->toString();
		return result;
	}
	bool containsWait() const override { return ifBody->containsWait() || (elseBody && elseBody->containsWait()); }
};

class ReturnStatement : public Statement {
public:
	std::string expression;
	ReturnStatement(std::string expression) : expression(expression) {}

	std::string toString() const override { return "[Return] " + expression; }
};

class WaitStatement : public Statement {
public:
	VarDeclaration result;
	std::string futureExpression;
	bool resultIsState;
	bool isWaitNext;
	std::string toString() const override {
		return "[Wait] " + result.type + " " + result.name + " <- " + futureExpression + " (" +
		       (resultIsState ? "state" : "local") + ")";
	}
	bool containsWait() const override { return true; }
};

class ChooseStatement : public Statement {
public:
	ChooseStatement(std::shared_ptr<Statement> body) : body(body) {}

	std::shared_ptr<Statement> body;
	std::string toString() const override { return "[Choose] " + body->toString(); }
	bool containsWait() const override { return body->containsWait(); }
};

class WhenStatement : public Statement {
public:
	std::shared_ptr<WaitStatement> wait;
	std::shared_ptr<Statement> body;
	WhenStatement(std::shared_ptr<WaitStatement> wait, std::shared_ptr<Statement> body) : wait(wait), body(body) {}

	std::string toString() const override { return "[When] (" + wait->toString() + ") " + body->toString(); }
	bool containsWait() const override { return true; }
};

class TryStatement : public Statement {
public:
	struct Catch {
		std::string expression;
		std::shared_ptr<Statement> body;
		int firstSourceLine;
	};

	std::shared_ptr<Statement> tryBody;
	std::vector<Catch> catches;

	TryStatement(std::shared_ptr<Statement> tryBody, std::vector<Catch> catches) : tryBody(tryBody), catches(catches) {}

	bool containsWait() const override {
		if (tryBody->containsWait())
			return true;
		for (const auto& c : catches)
			if (c.body && c.body->containsWait())
				return true;
		return false;
	}
	std::string toString() const override {
		std::string result = "[Try] " + tryBody->toString();
		for (const auto& c : catches) {
			result += "\n[CATCH] " + c.expression + " " + c.body->toString();
		}
		return result;
	}
};

class ThrowStatement : public Statement {
public:
	ThrowStatement(std::string expression) : expression(expression) {}
	std::string expression;
	std::string toString() const override { return "[Throw] " + expression; }
};

class CodeBlock : public Statement {
public:
	CodeBlock() = default;
	CodeBlock(std::vector<std::shared_ptr<Statement>> statements) : statements(statements) {}

	std::vector<std::shared_ptr<Statement>> statements;

	std::string toString() const override {
		std::string result = "[CodeBlock]\n";
		for (const auto& s : statements) {
			result += s->toString() + "\n";
		}
		result += "EndCodeBlock";
		return result;
	}

	bool containsWait() const override {
		for (const auto& s : statements)
			if (s->containsWait())
				return true;
		return false;
	}
};

class Declaration {
public:
	std::string type;
	std::string name;
	std::string comment;
};

class Actor {
public:
	std::vector<std::string> attributes;
	std::string returnType;
	std::string name;
	std::string enclosingClass = "";
	std::vector<VarDeclaration> parameters;
	std::vector<VarDeclaration> templateFormals; // empty if not a template
	std::shared_ptr<CodeBlock> body;
	int sourceLine;
	bool isStatic = false;

private:
	bool isUncancellable;

public:
	std::string testCaseParameters = "";
	std::string nameSpace = "";
	bool isForwardDeclaration = false;
	bool isTestCase = false;
	bool isCancellable() { return returnType.empty() == false && !isUncancellable; }
	void setUncancellable() { isUncancellable = true; }
};

class Descr {
public:
	std::string name;
	std::string superClassList;
	std::vector<Declaration> body;
};

} // namespace actorcompiler

#endif // ACTOR_COMPILER_PARSE_TREE_H
