/*
 * ActorParser.cpp
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

#include "ActorParser.h"
#include "Tokenizer.h"
#include "ActorCompiler.h"
#include <algorithm>
#include <stack>
#include <sstream>
#include <unordered_set>
#include <regex>
#include <string>
#include <vector>
#include <map>
#include <stdexcept>
#include <cstddef>
#include <iostream>

namespace actorcompiler {

ActorParser::ActorParser(const std::string& text,
                         const std::string& sourceFile,
                         const ErrorMessagePolicy& errorMessagePolicy,
                         bool generateProbes)
  : sourceFile(sourceFile), errorMessagePolicy(errorMessagePolicy), generateProbes(generateProbes) {
	// TODO: Implement in Step 4
	tokens = Tokenizer::tokenize(text);
	Tokenizer::countParens(tokens);
}

void ActorParser::write(std::ostream& writer, const std::string& destFileName) {
	writer << "#define POST_ACTOR_COMPILER 1\n";
	int outLine = 1;
	if (lineNumbersEnabled && !tokens.empty()) {
		writer << "#line " << tokens[0].sourceLine << " \"" << sourceFile << "\"\n";
		outLine++;
	}

	struct ClassContext {
		std::string name;
		int inBlocks;
	};
	int inBlocks = 0;
	std::stack<ClassContext> classStack;

	for (size_t i = 0; i < tokens.size(); ++i) {
		if (tokens[0].sourceLine == 0) {
			throw std::runtime_error("Internal error: Invalid source line (0)");
		}
		const auto& tk = tokens[i];
		if (tk.value == "ACTOR" || tk.value == "SWIFT_ACTOR" || tk.value == "TEST_CASE") {
			size_t end = i;
			Actor actor = parseActor(i, end);
			if (!classStack.empty()) {
				// Build enclosing class name chain A::B::C
				std::vector<std::string> names;
				auto tmp = classStack; // copy
				while (!tmp.empty()) {
					names.push_back(tmp.top().name);
					tmp.pop();
				}
				std::reverse(names.begin(), names.end());
				actor.enclosingClass.clear();
				for (size_t k = 0; k < names.size(); ++k) {
					if (k)
						actor.enclosingClass += "::";
					actor.enclosingClass += names[k];
				}
			}

			std::ostringstream actorOut;
			ActorCompiler ac(actor, sourceFile, inBlocks == 0, lineNumbersEnabled, generateProbes);
			ac.write(actorOut);
			for (const auto& kv : ac.getUidObjects()) {
				uidObjects.emplace(kv.first, kv.second);
			}

			std::string outStr = actorOut.str();
			std::istringstream iss(outStr);
			std::string line;
			bool hasLineNumber = false;
			bool hadLineNumber = true;
			while (std::getline(iss, line)) {
				if (lineNumbersEnabled) {
					bool isLineNumber = line.find("#line") != std::string::npos;
					if (isLineNumber)
						hadLineNumber = true;
					if (!isLineNumber && !hasLineNumber && hadLineNumber) {
						writer << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line " << (outLine + 1) << " \"" << destFileName
						       << "\"\n";
						outLine++;
						hadLineNumber = false;
					}
					hasLineNumber = isLineNumber;
				}
				// Trim trailing CR
				if (!line.empty() && (line.back() == '\r'))
					line.pop_back();
				writer << line << "\n";
				outLine++;
			}

			i = end;
			if (i < tokens.size() && lineNumbersEnabled) {
				writer << "#line " << tokens[i].sourceLine << " \"" << sourceFile << "\"\n";
				outLine++;
			}
		} else if (tk.value == "DESCR") {
			size_t end;
			Descr descr = parseDescr(i, end);
			int lines = 0;
			DescrCompiler(descr, tokens[i].braceDepth).write(writer, lines);
			i = end;
			outLine += lines;
			if (i < tokens.size() && lineNumbersEnabled) {
				writer << "#line " << tokens[i].sourceLine << " \"" << sourceFile << "\"\n";
				outLine++;
			}
		} else if (tk.value == "class" || tk.value == "struct" || tk.value == "union") {
			writer << tk.value;
			std::string name;
			if (parseClassContext(range(i + 1, tokens.size()), name)) {
				classStack.push(ClassContext{ name, inBlocks });
			}
		} else {
			if (tk.value == "{") {
				inBlocks++;
			} else if (tk.value == "}") {
				inBlocks--;
				if (!classStack.empty() && classStack.top().inBlocks == inBlocks) {
					classStack.pop();
				}
			}
			writer << tk.value;
			outLine += static_cast<int>(std::count(tk.value.begin(), tk.value.end(), '\n'));
		}
	}
}

TokenRange ActorParser::range(size_t begin, size_t end) const {
	return TokenRange(tokens, begin, end);
}

std::string ActorParser::str(const TokenRange& r) const {
	std::string s;
	s.reserve(r.length() * 2);
	for (auto it = r.cbegin(); it != r.cend(); ++it) {
		s += it->value;
	}
	return s;
}

std::string ActorParser::norm(const TokenRange& r) const {
	std::string s;
	bool inWs = false;
	bool leading = true;
	for (auto it = r.cbegin(); it != r.cend(); ++it) {
		const Token& t = *it;
		if (!t.isWhitespace()) {
			if (inWs && !leading)
				s += ' ';
			inWs = false;
			s += t.value;
			leading = false;
		} else {
			inWs = true;
		}
	}
	return s;
}

// ========== Parsing helpers and statements ==========

static std::regex kIdentifierRe("^[a-zA-Z_][a-zA-Z_0-9]*$");

bool ActorParser::parseClassContext(TokenRange toks, std::string& name) {
	name.clear();
	if (toks.isEmpty())
		return false;
	// Skip attributes [[...]] or alignas(...)
	Token first = toks.first();
	while (first.value == "[") {
		auto contents = first.getMatchingRangeIn(toks);
		toks = range(contents.endIndex() + 1, toks.endIndex());
		first = toks.first();
	}
	if (first.value == "alignas") {
		toks = range(first.position + 1, toks.endIndex());
		first = toks.first();
		first.assert("Expected ( after alignas", [](const Token& t) { return t.value == "("; });
		auto contents = first.getMatchingRangeIn(toks);
		toks = range(contents.endIndex() + 1, toks.endIndex());
		first = toks.first();
	}

	// Parse qualified identifier
	if (!std::regex_match(first.value, kIdentifierRe))
		return false;
	while (true) {
		first.assert("Expected identifier", [](const Token& t) { return std::regex_match(t.value, kIdentifierRe); });
		name += first.value;
		toks = range(first.position + 1, toks.endIndex());
		if (!toks.isEmpty() && toks.first().value == "::") {
			name += "::";
			toks = toks.skip(1);
			if (!toks.isEmpty())
				first = toks.first();
			else
				break;
		} else {
			break;
		}
	}
	// Accept ':' or '{' next
	if (!toks.isEmpty()) {
		Token nxt = toks.first();
		if (nxt.value == ":" || nxt.value == "{")
			return true;
	}
	return false;
}

// Split by delimiter at same paren depth, respecting angle brackets
static std::vector<Token> outsideAngles(const TokenRange& r) {
	return AngleBracketParser::notInsideAngleBrackets(r);
}

std::vector<Declaration> ActorParser::parseDescrCodeBlock(const TokenRange& toks) {
	std::vector<Declaration> declarations;
	TokenRange t = toks;
	while (true) {
		auto opt = t.firstOrDefault([](const Token& x) { return x.value == ";"; });
		if (!opt)
			break;
		Token delim = *opt;
		size_t pos = delim.position + 1;
		TokenRange potentialComment = range(pos, t.endIndex());
		// Skip inline comment after declaration
		while (!potentialComment.isEmpty() &&
		       (potentialComment.first().value == "\t" || potentialComment.first().value == " ")) {
			potentialComment = potentialComment.skip(1);
		}
		if (!potentialComment.isEmpty() && potentialComment.first().value.rfind("//", 0) == 0) {
			pos = potentialComment.first().position + 1;
		}
		TokenRange stmt = range(t.beginIndex(), pos);
		// ParseDeclaration for DESCR
		Declaration dec;
		TokenRange nameRange =
		    range(stmt.beginIndex(), delim.position).revSkipWhile(isWhitespace).revTakeWhile(isNonWhitespace);
		TokenRange typeRange = range(stmt.beginIndex(), nameRange.beginIndex());
		TokenRange commentRange = range(delim.position + 1, t.endIndex());
		dec.name = str(nameRange);
		dec.type = str(typeRange);
		{
			std::string c = str(commentRange);
			while (!c.empty() && (c.front() == '/' || c.front() == ' ' || c.front() == '\t'))
				c.erase(c.begin());
			dec.comment = c;
		}
		declarations.push_back(std::move(dec));
		t = range(pos, t.endIndex());
	}
	if (!t.all(isWhitespace)) {
		throw Error(t.skipWhile(isWhitespace).first().sourceLine, "Trailing unterminated statement in code block");
	}
	return declarations;
}

void ActorParser::parseDescrHeading(Descr& descr, const TokenRange& toks) {
	Token first = toks.skipWhile(isWhitespace).first();
	first.assert("non-struct DESCR!", [](const Token& t) { return t.value == "struct"; });
	TokenRange rest = toks.skipWhile(isWhitespace).skip(1).skipWhile(isWhitespace);
	auto colon = rest.firstOrDefault([](const Token& t) { return t.value == ":"; });
	TokenRange nameRange = rest;
	if (colon) {
		descr.superClassList = str(range(colon->position + 1, rest.endIndex()));
		nameRange = range(rest.beginIndex(), colon->position);
	}
	descr.name = str(nameRange);
}

// Normalize whitespace/identifiers while parsing declarations
void ActorParser::parseDeclaration(TokenRange tokens,
                                   Token& name,
                                   TokenRange& type,
                                   TokenRange& initializer,
                                   bool& constructorSyntax) {
	initializer = TokenRange(tokens.getAllTokens(), 0, 0); // placeholder; we'll detect null by length 0 and begin=end
	TokenRange beforeInitializer = tokens;
	constructorSyntax = false;

	// Look for '=' outside angle brackets
	std::vector<Token> outside = outsideAngles(tokens);
	Token equals;
	bool hasEquals = false;
	for (const auto& t : outside) {
		if (t.value == "=" && t.parenDepth == tokens.first().parenDepth) {
			equals = t;
			hasEquals = true;
			break;
		}
	}
	if (hasEquals) {
		beforeInitializer = range(tokens.beginIndex(), equals.position);
		initializer = range(equals.position + 1, tokens.endIndex());
	} else {
		// constructor syntax: type name(initializer)
		TokenRange rest = tokens;
		auto open = outsideAngles(rest);
		Token paren;
		bool hasParen = false;
		for (const auto& t : open) {
			if (t.value == "(") {
				paren = t;
				hasParen = true;
				break;
			}
		}
		if (hasParen) {
			constructorSyntax = true;
			beforeInitializer = range(tokens.beginIndex(), paren.position);
			auto params = range(paren.position + 1, tokens.endIndex()).takeWhile([&](const Token& t) {
				return t.parenDepth > paren.parenDepth;
			});
			initializer = params;
		} else {
			// brace uniform init unsupported for state variables
			for (const auto& t : outside) {
				if (t.value == "{") {
					throw Error(t.sourceLine,
					            "Uniform initialization syntax is not currently supported for state variables (use '(' "
					            "instead of '}' ?)");
				}
			}
		}
	}
	name = range(beforeInitializer.beginIndex(), beforeInitializer.endIndex()).last(isNonWhitespace);
	if (beforeInitializer.beginIndex() == static_cast<size_t>(name.position)) {
		throw Error(beforeInitializer.first().sourceLine, "Declaration has no type.");
	}
	type = range(beforeInitializer.beginIndex(), name.position);
}

VarDeclaration ActorParser::parseVarDeclaration(const TokenRange& tokens) {
	Token name;
	TokenRange type = range(tokens.beginIndex(), tokens.beginIndex());
	TokenRange initializer = range(tokens.beginIndex(), tokens.beginIndex());
	bool constructorSyntax;
	parseDeclaration(tokens, name, type, initializer, constructorSyntax);
	VarDeclaration vd;
	vd.name = name.value;
	vd.type = norm(type);
	vd.initializer = (initializer.length() == 0 ? std::string() : norm(initializer));
	vd.initializerConstructorSyntax = constructorSyntax;
	return vd;
}

LoopStatement* ActorParser::parseLoopStatement(const TokenRange& toks) {
	auto* s = new LoopStatement();
	s->body.reset(parseCompoundStatement(toks.consume("loop")));
	return s;
}

ChooseStatement* ActorParser::parseChooseStatement(const TokenRange& toks) {
	auto* s = new ChooseStatement();
	s->body.reset(parseCompoundStatement(toks.consume("choose")));
	return s;
}

WhenStatement* ActorParser::parseWhenStatement(const TokenRange& toks) {
	TokenRange expr = toks.consume("when")
	                      .skipWhile(isWhitespace)
	                      .first()
	                      .assert("Expected (", [](const Token& t) { return t.value == "("; })
	                      .getMatchingRangeIn(toks)
	                      .skipWhile(isWhitespace);
	auto* s = new WhenStatement();
	s->wait.reset(parseWaitStatement(expr));
	s->body.reset(parseCompoundStatement(range(expr.endIndex() + 1, toks.endIndex())));
	return s;
}

StateDeclarationStatement* ActorParser::parseStateDeclaration(const TokenRange& toks) {
	TokenRange t = toks.consume("state").revSkipWhile([](const Token& t) { return t.value == ";"; });
	auto* s = new StateDeclarationStatement();
	s->decl = parseVarDeclaration(t);
	return s;
}

ReturnStatement* ActorParser::parseReturnStatement(const TokenRange& toks) {
	TokenRange t = toks.consume("return").revSkipWhile([](const Token& t) { return t.value == ";"; });
	auto* s = new ReturnStatement();
	s->expression = norm(t);
	return s;
}

ThrowStatement* ActorParser::parseThrowStatement(const TokenRange& toks) {
	TokenRange t = toks.consume("throw").revSkipWhile([](const Token& t) { return t.value == ";"; });
	auto* s = new ThrowStatement();
	s->expression = norm(t);
	return s;
}

WaitStatement* ActorParser::parseWaitStatement(const TokenRange& toks) {
	auto* ws = new WaitStatement();
	ws->firstSourceLine = toks.first().sourceLine;
	TokenRange r = toks;
	if (r.first().value == "state") {
		ws->resultIsState = true;
		r = r.consume("state");
	}
	TokenRange initializer = range(r.beginIndex(), r.beginIndex());
	bool isStandalone = false;
	if (r.first().value == "wait" || r.first().value == "waitNext") {
		initializer = r.revSkipWhile([](const Token& t) { return t.value == ";"; });
		ws->result = VarDeclaration{ "Void", "_", std::string(), false };
		isStandalone = true;
	} else {
		Token name;
		TokenRange type = range(r.beginIndex(), r.beginIndex());
		TokenRange init = range(r.beginIndex(), r.beginIndex());
		bool ctorSyntax;
		parseDeclaration(r.revSkipWhile([](const Token& t) { return t.value == ";"; }), name, type, init, ctorSyntax);
		std::string types = norm(type);
		if (types == "Void") {
			throw Error(ws->firstSourceLine,
			            "Assigning the result of a Void wait is not allowed.  Just use a standalone wait statement.");
		}
		ws->result = VarDeclaration{ types, name.value, std::string(), false };
		initializer = init;
	}
	if (initializer.length() == 0) {
		throw Error(ws->firstSourceLine, "Wait statement must be a declaration or standalone statement");
	}
	TokenRange waitParams =
	    initializer.skipWhile(isWhitespace)
	        .consume("Statement contains a wait, but is not a valid wait statement or a supported compound statement.1",
	                 [&](const Token& t) {
		                 if (t.value == "wait")
			                 return true;
		                 if (t.value == "waitNext") {
			                 ws->isWaitNext = true;
			                 return true;
		                 }
		                 return false;
	                 })
	        .skipWhile(isWhitespace)
	        .first()
	        .assert("Expected (", [](const Token& t) { return t.value == "("; })
	        .getMatchingRangeIn(initializer);
	if (!range(waitParams.endIndex(), initializer.endIndex()).consume(")").all(isWhitespace)) {
		throw Error(toks.first().sourceLine,
		            "Statement contains a wait, but is not a valid wait statement or a supported compound statement.2");
	}
	ws->futureExpression = norm(waitParams);
	return ws;
}

WhileStatement* ActorParser::parseWhileStatement(const TokenRange& toks) {
	TokenRange afterWhile = toks.consume("while").skipWhile(isWhitespace);
	Token open = afterWhile.first();
	open.assert("Expected (", [](const Token& t) { return t.value == "("; });
	TokenRange expr = open.getMatchingRangeIn(toks);
	auto* s = new WhileStatement();
	s->expression = norm(expr);
	s->body.reset(parseCompoundStatement(range(expr.endIndex() + 1, toks.endIndex())));
	return s;
}

Statement* ActorParser::parseForStatement(const TokenRange& toks) {
	TokenRange afterFor = toks.consume("for").skipWhile(isWhitespace);
	Token open = afterFor.first();
	open.assert("Expected (", [](const Token& t) { return t.value == "("; });
	TokenRange head = open.getMatchingRangeIn(toks);
	// 3-part for separated by two ';'
	std::vector<Token> tt;
	for (auto it = head.cbegin(); it != head.cend(); ++it)
		tt.push_back(*it);
	std::vector<Token> semi;
	for (const auto& t : tt)
		if (t.parenDepth == head.first().parenDepth && t.braceDepth == head.first().braceDepth && t.value == ";")
			semi.push_back(t);
	if (semi.size() == 2) {
		Token s0 = semi[0], s1 = semi[1];
		TokenRange init = range(head.beginIndex(), s0.position);
		TokenRange cond = range(s0.position + 1, s1.position);
		TokenRange next = range(s1.position + 1, head.endIndex());
		TokenRange body = range(head.endIndex() + 1, toks.endIndex());
		auto* s = new ForStatement();
		s->initExpression = norm(init);
		s->condExpression = norm(cond);
		s->nextExpression = norm(next);
		s->body.reset(parseCompoundStatement(body));
		return s;
	}
	// range-for style: ':' occurs once
	std::vector<Token> colons;
	for (const auto& t : tt)
		if (t.parenDepth == head.first().parenDepth && t.braceDepth == head.first().braceDepth && t.value == ":")
			colons.push_back(t);
	if (colons.size() != 1) {
		throw Error(head.first().sourceLine, "for statement must be 3-arg style or c++11 2-arg style");
	}
	Token colon = colons[0];
	auto* s = new RangeForStatement();
	s->rangeExpression = norm(range(colon.position + 1, head.endIndex()).skipWhile(isWhitespace));
	s->rangeDecl = norm(range(head.beginIndex(), colon.position - 1).skipWhile(isWhitespace));
	s->body.reset(parseCompoundStatement(range(head.endIndex() + 1, toks.endIndex())));
	return s;
}

IfStatement* ActorParser::parseIfStatement(const TokenRange& toks) {
	TokenRange t = toks.consume("if");
	bool constexpr_ = false;
	if (t.first().value == "constexpr") {
		constexpr_ = true;
		t = t.consume("constexpr");
	}
	TokenRange afterIf = t.skipWhile(isWhitespace);
	Token open = afterIf.first();
	open.assert("Expected (", [](const Token& t) { return t.value == "("; });
	TokenRange expr = open.getMatchingRangeIn(toks);
	auto* s = new IfStatement();
	s->expression = norm(expr);
	s->constexpr_ = constexpr_;
	s->ifBody.reset(parseCompoundStatement(range(expr.endIndex() + 1, toks.endIndex())));
	return s;
}

void ActorParser::parseElseStatement(const TokenRange& toks, Statement* prevStatement) {
	IfStatement* ifs = dynamic_cast<IfStatement*>(prevStatement);
	while (ifs && ifs->elseBody)
		ifs = dynamic_cast<IfStatement*>(ifs->elseBody.get());
	if (!ifs)
		throw Error(toks.first().sourceLine, "else without matching if");
	ifs->elseBody.reset(parseCompoundStatement(toks.consume("else")));
}

TryStatement* ActorParser::parseTryStatement(const TokenRange& toks) {
	auto* s = new TryStatement();
	s->tryBody.reset(parseCompoundStatement(toks.consume("try")));
	return s;
}

void ActorParser::parseCatchStatement(const TokenRange& toks, Statement* prevStatement) {
	TryStatement* ts = dynamic_cast<TryStatement*>(prevStatement);
	if (!ts)
		throw Error(toks.first().sourceLine, "catch without matching try");
	TokenRange afterCatch = toks.consume("catch").skipWhile(isWhitespace);
	Token open = afterCatch.first();
	open.assert("Expected (", [](const Token& t) { return t.value == "("; });
	TokenRange expr = open.getMatchingRangeIn(toks);
	TryStatement::Catch c;
	c.expression = norm(expr);
	c.body.reset(parseCompoundStatement(range(expr.endIndex() + 1, toks.endIndex())));
	c.firstSourceLine = expr.first().sourceLine;
	ts->catches.push_back(std::move(c));
}

void ActorParser::parseStatement(const TokenRange& toks, std::vector<std::unique_ptr<Statement>>& statements) {
	TokenRange t = toks.skipWhile(isWhitespace);
	auto add = [&](std::unique_ptr<Statement> s) {
		s->firstSourceLine = t.first().sourceLine;
		statements.emplace_back(std::move(s));
	};
	const std::string& v = t.first().value;
	if (v == "loop") {
		add(std::unique_ptr<Statement>(parseLoopStatement(t)));
	} else if (v == "while") {
		add(std::unique_ptr<Statement>(parseWhileStatement(t)));
	} else if (v == "for") {
		add(std::unique_ptr<Statement>(parseForStatement(t)));
	} else if (v == "break") {
		add(std::make_unique<BreakStatement>());
	} else if (v == "continue") {
		add(std::make_unique<ContinueStatement>());
	} else if (v == "return") {
		add(std::unique_ptr<Statement>(parseReturnStatement(t)));
	} else if (v == "{") {
		add(std::unique_ptr<Statement>(parseCompoundStatement(t)));
	} else if (v == "if") {
		add(std::unique_ptr<Statement>(parseIfStatement(t)));
	} else if (v == "else") {
		parseElseStatement(t, statements.back().get());
	} else if (v == "choose") {
		add(std::unique_ptr<Statement>(parseChooseStatement(t)));
	} else if (v == "when") {
		add(std::unique_ptr<Statement>(parseWhenStatement(t)));
	} else if (v == "try") {
		add(std::unique_ptr<Statement>(parseTryStatement(t)));
	} else if (v == "catch") {
		parseCatchStatement(t, statements.back().get());
	} else if (v == "throw") {
		add(std::unique_ptr<Statement>(parseThrowStatement(t)));
	} else {
		static const std::unordered_set<std::string> illegal = {
			"goto", "do", "finally", "__if_exists", "__if_not_exists"
		};
		if (illegal.count(v))
			throw Error(t.first().sourceLine, "Statement '{0}' not supported in actors.", v.c_str());
		if (t.any([](const Token& x) { return x.value == "wait" || x.value == "waitNext"; })) {
			add(std::unique_ptr<Statement>(parseWaitStatement(t)));
		} else if (v == "state") {
			add(std::unique_ptr<Statement>(parseStateDeclaration(t)));
		} else if (v == "switch" && t.any([](const Token& x) { return x.value == "return"; })) {
			throw Error(t.first().sourceLine, "Unsupported compound statement containing return.");
		} else if (!t.isEmpty() && t.first().value.rfind("#", 0) == 0) {
			throw Error(t.first().sourceLine,
			            "Found \"{0}\". Preprocessor directives are not supported within ACTORs",
			            t.first().value.c_str());
		} else if (t.revSkipWhile([](const Token& x) { return x.value == ";"; }).any(isNonWhitespace)) {
			auto poc = std::make_unique<PlainOldCodeStatement>();
			poc->code = norm(t.revSkipWhile([](const Token& x) { return x.value == ";"; })) + ";";
			add(std::move(poc));
		}
	}
}

Statement* ActorParser::parseCompoundStatement(const TokenRange& toks) {
	Token first = toks.skipWhile(isWhitespace).first();
	if (first.value == "{") {
		auto inBraces = first.getMatchingRangeIn(toks);
		if (!range(inBraces.endIndex(), toks.endIndex()).consume("}").all(isWhitespace)) {
			throw Error(inBraces.last().sourceLine, "Unexpected tokens after compound statement");
		}
		CodeBlock cb = parseCodeBlock(inBraces);
		return new CodeBlock(std::move(cb));
	} else {
		// Single statement terminated by ';' at same depth
		auto semi = toks.firstOrDefault([&](const Token& x) {
			return x.parenDepth == toks.first().parenDepth && x.braceDepth == toks.first().braceDepth && x.value == ";";
		});
		if (!semi)
			throw Error(toks.first().sourceLine, "Expected ';' after statement");
		std::vector<std::unique_ptr<Statement>> statements;
		parseStatement(range(toks.beginIndex(), semi->position + 1), statements);
		return statements.empty() ? static_cast<Statement*>(new PlainOldCodeStatement()) : statements[0].release();
	}
}

CodeBlock ActorParser::parseCodeBlock(const TokenRange& toks) {
	std::vector<std::unique_ptr<Statement>> statements;
	TokenRange t = toks;
	while (true) {
		auto opt = t.firstOrDefault([&](const Token& x) {
			return x.parenDepth == toks.first().parenDepth && x.braceDepth == toks.first().braceDepth &&
			       (x.value == ";" || x.value == "}");
		});
		if (!opt)
			break;
		Token delim = *opt;
		parseStatement(range(t.beginIndex(), delim.position + 1), statements);
		t = range(delim.position + 1, t.endIndex());
	}
	if (!t.all(isWhitespace)) {
		throw Error(t.skipWhile(isWhitespace).first().sourceLine, "Trailing unterminated statement in code block");
	}
	CodeBlock cb;
	for (auto& up : statements)
		cb.statements.emplace_back(std::move(up));
	return cb;
}

void ActorParser::parseTestCaseHeading(Actor& actor, TokenRange toks) {
	actor.isStatic = true;
	TokenRange paramRange =
	    toks.last(isNonWhitespace)
	        .assert("Unexpected tokens after test case parameter list.",
	                [&](const Token& t) { return t.value == ")" && t.parenDepth == toks.first().parenDepth; })
	        .getMatchingRangeIn(toks);
	actor.testCaseParameters = str(paramRange);
	actor.name = std::string("flowTestCase") + std::to_string(toks.first().sourceLine);
	actor.parameters = { VarDeclaration{ "UnitTestParameters", "params", std::string(), false } };
	actor.returnType = "Void";
}

void ActorParser::parseActorHeading(Actor& actor, TokenRange toks) {
	Token templateTok = toks.first();
	if (templateTok.value == "template") {
		TokenRange tmp = range(templateTok.position + 1, toks.endIndex()).skipWhile(isWhitespace);
		Token open = tmp.first();
		open.assert("Invalid template declaration", [](const Token& t) { return t.value == "<"; });
		TokenRange templateParams = open.getMatchingRangeIn(toks);
		// Split templates by comma
		std::vector<TokenRange> parts;
		TokenRange p = templateParams;
		while (!p.isEmpty()) {
			auto opt = p.firstOrDefault([&](const Token& t) { return t.value == ","; });
			if (!opt) {
				parts.push_back(p);
				break;
			}
			parts.push_back(range(p.beginIndex(), opt->position));
			p = range(opt->position + 1, p.endIndex());
		}
		actor.templateFormals.clear();
		for (const auto& pr : parts)
			actor.templateFormals.push_back(parseVarDeclaration(pr));
		toks = range(templateParams.endIndex() + 1, toks.endIndex());
	}
	// attributes [[...]]
	Token attribute = toks.skipWhile(isWhitespace).first();
	while (attribute.value == "[") {
		auto contents = attribute.getMatchingRangeIn(toks);
		std::string attr = "[" + norm(contents) + "]";
		actor.attributes.push_back(attr);
		toks = range(contents.endIndex() + 1, toks.endIndex());
		attribute = toks.skipWhile(isWhitespace).first();
	}
	Token staticKw = toks.skipWhile(isWhitespace).first();
	if (staticKw.value == "static") {
		actor.isStatic = true;
		toks = range(staticKw.position + 1, toks.endIndex());
	}
	Token unc = toks.skipWhile(isWhitespace).first();
	if (unc.value == "UNCANCELLABLE") {
		actor.setUncancellable();
		toks = range(unc.position + 1, toks.endIndex());
	}

	// Find parameter list
	TokenRange paramRange =
	    toks.last(isNonWhitespace)
	        .assert("Unexpected tokens after actor parameter list.",
	                [&](const Token& t) { return t.value == ")" && t.parenDepth == toks.first().parenDepth; })
	        .getMatchingRangeIn(toks);
	// Split parameters by comma at same depth
	std::vector<TokenRange> params;
	TokenRange pp = paramRange;
	while (!pp.isEmpty()) {
		auto opt = AngleBracketParser::notInsideAngleBrackets(pp); // tokens outside angle brackets
		int basePD = pp.first().parenDepth;
		int foundPos = -1;
		for (const auto& t : opt) {
			if (t.value == "," && t.parenDepth == basePD) {
				foundPos = t.position;
				break;
			}
		}
		if (foundPos < 0) {
			params.push_back(pp);
			break;
		}
		params.push_back(range(pp.beginIndex(), foundPos));
		pp = range(foundPos + 1, pp.endIndex());
	}
	actor.parameters.clear();
	for (const auto& pr : params)
		actor.parameters.push_back(parseVarDeclaration(pr));

	Token nameTok = range(toks.beginIndex(), paramRange.beginIndex() - 1).last(isNonWhitespace);
	actor.name = nameTok.value;

	TokenRange returnTypeR = range(toks.first().position + 1, nameTok.position).skipWhile(isWhitespace);
	Token ret = returnTypeR.first();
	if (ret.value == "Future") {
		TokenRange afterFuture = returnTypeR.skip(1).skipWhile(isWhitespace);
		Token open2 = afterFuture.first();
		open2.assert("Expected <", [](const Token& t) { return t.value == "<"; });
		TokenRange ofType = open2.getMatchingRangeIn(returnTypeR);
		actor.returnType = norm(ofType);
		toks = range(ofType.endIndex() + 1, returnTypeR.endIndex());
	} else if (ret.value == "void") {
		actor.returnType.clear();
		toks = returnTypeR.skip(1);
	} else {
		throw Error(actor.sourceLine, "Actor apparently does not return Future<T>");
	}

	// optional namespace qualifier
	if (!toks.isEmpty()) {
		if (toks.last().value == "::") {
			actor.nameSpace = str(range(toks.beginIndex(), toks.endIndex() - 1));
		} else {
			// tolerated as in C# (debug aid)
		}
	}
	if (errorMessagePolicy.actorsNoDiscardByDefault() &&
	    std::find(actor.attributes.begin(), actor.attributes.end(), "[[flow_allow_discard]]") ==
	        actor.attributes.end()) {
		if (actor.isCancellable())
			actor.attributes.push_back("[[nodiscard]]");
	}
	std::unordered_set<std::string> knownFlow{ "[[flow_allow_discard]]" };
	for (auto it = actor.attributes.begin(); it != actor.attributes.end();) {
		if (it->rfind("[[flow_", 0) == 0) {
			if (!knownFlow.count(*it))
				throw Error(actor.sourceLine, (std::string("Unknown flow attribute ") + *it).c_str());
			it = actor.attributes.erase(it);
		} else
			++it;
	}
}

Descr ActorParser::parseDescr(size_t pos, size_t& end) {
	Descr descr;
	TokenRange toksR = range(pos + 1, tokens.size());
	TokenRange heading = toksR.takeWhile([](const Token& t) { return t.value != "{"; });
	TokenRange body = range(heading.endIndex() + 1, tokens.size()).takeWhile([&](const Token& t) {
		return t.braceDepth > toksR.first().braceDepth || t.value == ";";
	});
	parseDescrHeading(descr, heading);
	descr.body = parseDescrCodeBlock(body);
	end = body.endIndex() + 1;
	return descr;
}

Actor ActorParser::parseActor(size_t pos, size_t& end) {
	Actor actor;
	Token head = tokens[pos];
	actor.sourceLine = head.sourceLine;
	TokenRange toksR = range(pos + 1, tokens.size());
	TokenRange heading = toksR.takeWhile([](const Token& t) { return t.value != "{"; });
	TokenRange toSemicolon = toksR.takeWhile([](const Token& t) { return t.value != ";"; });
	actor.isForwardDeclaration = (toSemicolon.length() < heading.length());
	if (actor.isForwardDeclaration) {
		heading = toSemicolon;
		if (head.value == "ACTOR" || head.value == "SWIFT_ACTOR") {
			parseActorHeading(actor, heading);
		} else {
			head.assert("ACTOR expected!", [](const Token&) { return false; });
		}
		end = heading.endIndex() + 1;
	} else {
		TokenRange body = range(heading.endIndex() + 1, tokens.size()).takeWhile([&](const Token& t) {
			return t.braceDepth > toksR.first().braceDepth;
		});
		if (head.value == "ACTOR" || head.value == "SWIFT_ACTOR") {
			parseActorHeading(actor, heading);
		} else if (head.value == "TEST_CASE") {
			parseTestCaseHeading(actor, heading);
			actor.isTestCase = true;
		} else {
			head.assert("ACTOR or TEST_CASE expected!", [](const Token&) { return false; });
		}
		actor.body = std::make_unique<CodeBlock>(parseCodeBlock(body));
		if (!actor.body->containsWait()) {
			errorMessagePolicy.handleActorWithoutWait(sourceFile, actor);
		}
		end = body.endIndex() + 1;
	}
	return actor;
}

} // namespace actorcompiler
