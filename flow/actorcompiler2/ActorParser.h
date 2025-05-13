/*
 * ActorParser.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#pragma once

#ifndef ACTOR_COMPILER_ACTOR_PARSER_H
#define ACTOR_COMPILER_ACTOR_PARSER_H

#include <algorithm>
#include <boost/regex.hpp>
#include <cassert>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "ParseTree.h"

namespace actorcompiler {

class Error : public std::exception {
public:
	int sourceLine;
	std::string message;

	Error(int sourceLine, const std::string& message) : sourceLine(sourceLine), message(message) {}

	const char* what() const noexcept override { return message.c_str(); }
};

class Actor;
class Descr;

class ErrorMessagePolicy {
public:
	bool DisableDiagnostics = false;

	void HandleActorWithoutWait(const std::string& sourceFile, const Actor& actor) const;

	bool ActorsNoDiscardByDefault() const { return !DisableDiagnostics; }
};

// Forward declarations
class TokenRange;

// Token class
class Token {
public:
	std::string value;
	int position;
	int sourceLine;
	int braceDepth;
	int parenDepth;

	Token() : position(0), sourceLine(0), braceDepth(0), parenDepth(0) {}
	Token(const std::string s) : value(s), position(0), sourceLine(0), braceDepth(0), parenDepth(0) {}
	Token& operator=(const Token& r) {
		if (this != &r) {
			value = r.value;
			position = r.position;
			sourceLine = r.sourceLine;
			braceDepth = r.braceDepth;
			parenDepth = r.parenDepth;
		}
		return *this;
	}

	bool isWhitespace() const {
		return value == " " || value == "\n" || value == "\r" || value == "\r\n" || value == "\t" ||
		       value.starts_with("//") || value.starts_with("/*");
	}

	Token Assert(const std::string& error, std::function<bool(const Token&)> pred) const;

	TokenRange getMatchingRangeIn(TokenRange range) const;
};

// TokenRange class
class TokenRange {
public:
	TokenRange() = default;
	TokenRange(const std::vector<Token>& tokens, int beginPos, int endPos)
	  : tokens(tokens), beginPos(beginPos), endPos(endPos) {
		if (beginPos > endPos) {
			assert(false);
			throw std::invalid_argument("Invalid TokenRange");
		}
	}

	bool empty() const { return beginPos == endPos; }
	int begin() const { return beginPos; }
	int end() const { return endPos; }

	Token first() const {
		if (empty()) {
			assert(false);
			throw std::invalid_argument("Empty TokenRange");
		}
		return tokens[beginPos];
	}

	std::optional<Token> first(std::function<bool(const Token&)> predicate) const {
		std::optional<Token> result;
		for (int i = beginPos; i < endPos; i++) {
			if (predicate(tokens[i])) {
				return tokens[i];
			}
		}
		return result;
	}

	// Returns true if all tokens in the range satisfy the predicate
	bool all(std::function<bool(const Token&)> predicate) const {
		for (int i = beginPos; i < endPos; i++) {
			if (!predicate(tokens[i])) {
				return false;
			}
		}
		return true;
	}

	// Returns all matching tokens in the range satisfy the predicate
	std::vector<Token> where(std::function<bool(const Token&)> predicate) const {
		std::vector<Token> result;
		for (int i = beginPos; i < endPos; i++) {
			if (predicate(tokens[i])) {
				result.push_back(tokens[i]);
			}
		}
		return result;
	}

	// Returns true if any token in the range satisfy the predicate
	bool any(std::function<bool(const Token&)> predicate) const {
		for (int i = beginPos; i < endPos; i++) {
			if (predicate(tokens[i])) {
				return true;
			}
		}
		return false;
	}

	Token last() const {
		if (empty()) {
			assert(false);
			throw std::invalid_argument("Empty TokenRange");
		}
		return tokens[endPos - 1];
	}

	Token operator[](int index) const {
		if (index < 0 || index >= tokens.size()) {
			assert(false);
			throw std::out_of_range("Index out of range");
		}
		return tokens[index];
	}

	Token last(std::function<bool(const Token&)> pred) const {
		for (int i = endPos - 1; i >= beginPos; i--) {
			if (pred(tokens[i]))
				return tokens[i];
		}
		assert(false);
		throw std::runtime_error("Matching token not found");
	}

	TokenRange skip(int count) const { return TokenRange(tokens, beginPos + count, endPos); }

	TokenRange consume(const std::string& value) const {
		Token t = first();
		t.Assert("Expected " + value, [&](const Token& t) { return t.value == value; });
		return skip(1);
	}

	// TODO:
	/*IEnumerator<Token> GetEnumerator()
	    {
	        for (int i = beginPos; i < endPos; i++)
	            yield return tokens[i];
	}*/

	TokenRange consume(const std::string& error, std::function<bool(const Token&)> pred) const {
		first().Assert(error, pred);
		return skip(1);
	}

	TokenRange SkipWhile(std::function<bool(const Token&)> pred) const {
		for (int e = beginPos; e < endPos; e++)
			if (!pred(tokens[e]))
				return TokenRange(tokens, e, endPos);
		return TokenRange(tokens, endPos, endPos);
	}

	TokenRange TakeWhile(std::function<bool(const Token&)> pred) const {
		for (int e = beginPos; e < endPos; e++)
			if (!pred(tokens[e]))
				return TokenRange(tokens, beginPos, e);
		return TokenRange(tokens, beginPos, endPos);
	}

	TokenRange RevTakeWhile(std::function<bool(const Token&)> pred) const {
		for (int e = endPos - 1; e >= beginPos; e--)
			if (!pred(tokens[e]))
				return TokenRange(tokens, e + 1, endPos);
		return TokenRange(tokens, beginPos, endPos);
	}

	TokenRange RevSkipWhile(std::function<bool(const Token&)> pred) const {
		for (int e = endPos - 1; e >= beginPos; e--)
			if (!pred(tokens[e]))
				return TokenRange(tokens, beginPos, e + 1);
		return TokenRange(tokens, beginPos, beginPos);
	}

	std::vector<Token> getAllTokens() const { return tokens; }
	int length() const { return endPos - beginPos; }

private:
	std::vector<Token> tokens;
	int beginPos = -1;
	int endPos = -1;
};

extern std::optional<Token> first_of(const std::vector<Token>& tokens, std::function<bool(const Token&)> predicate);

// BracketParser namespace
namespace BracketParser {
std::vector<Token> notInsideBrackets(const TokenRange& tokens);
}

// AngleBracketParser namespace
namespace AngleBracketParser {
std::vector<Token> notInsideAngleBrackets(const TokenRange& tokens);
}

// ActorParser class
class ActorParser {
public:
	bool lineNumbersEnabled = true;
	std::vector<Token> tokens;
	std::string sourceFile;
	ErrorMessagePolicy errorMessagePolicy;
	bool generateProbes;
	std::unordered_map<std::pair<uint64_t, uint64_t>, std::string, PairHash> uidObjects;

	const std::regex identifierPattern = std::regex(R"(\\G[a-zA-Z_][a-zA-Z_0-9]*)");
	std::vector<boost::regex> tokenExpressions;

	ActorParser(const std::string& text,
	            const std::string& sourceFile,
	            const ErrorMessagePolicy& errorMessagePolicy,
	            bool generateProbes)
	  : sourceFile(sourceFile), errorMessagePolicy(errorMessagePolicy), generateProbes(generateProbes),
	    tokenExpressions(initializeTokenExpressions()) {
		tokens = tokenize(text);
		countParens();
	}

	class ClassContext {
	public:
		std::string name;
		int inBlocks;
	};

	void write(std::ostream& writer, const std::string& destFileName);

	std::vector<TokenRange> splitParameterList(TokenRange toks, const std::string& delimiter) const;

	std::vector<Token> normalizeWhitespace(const std::vector<Token>& tokens) const;
	std::vector<Token> normalizeWhitespace(const TokenRange& tokens) const;

	void parseDeclaration(TokenRange tokens,
	                      Token& name,
	                      TokenRange& type,
	                      TokenRange& initializer,
	                      bool& constructorSyntax) const;

	VarDeclaration parseVarDeclaration(TokenRange tokens) const;

	std::function<bool(const Token&)> Whitespace = [](const Token& t) { return t.isWhitespace(); };
	std::function<bool(const Token&)> NonWhitespace = [](const Token& t) { return !t.isWhitespace(); };

	void parseDescrHeading(Descr& descr, TokenRange toks) const;

	void parseTestCaseHeading(Actor& actor, TokenRange toks) const;

	void parseActorHeading(Actor& actor, TokenRange toks) const;

	std::shared_ptr<LoopStatement> parseLoopStatement(TokenRange toks) const;

	std::shared_ptr<ChooseStatement> parseChooseStatement(TokenRange toks) const;

	std::shared_ptr<WhenStatement> parseWhenStatement(TokenRange toks) const;

	std::shared_ptr<StateDeclarationStatement> parseStateDeclaration(TokenRange toks) const;

	std::shared_ptr<ReturnStatement> parseReturnStatement(TokenRange toks) const;

	std::shared_ptr<ThrowStatement> parseThrowStatement(TokenRange toks) const;

	std::shared_ptr<WaitStatement> parseWaitStatement(TokenRange toks) const;

	std::shared_ptr<WhileStatement> parseWhileStatement(TokenRange toks) const;

	std::shared_ptr<Statement> parseForStatement(TokenRange toks) const;

	std::shared_ptr<Statement> parseIfStatement(TokenRange toks) const;

	void parseElseStatement(TokenRange toks, const std::shared_ptr<Statement>& prevStatement) const;

	std::shared_ptr<Statement> parseTryStatement(TokenRange toks) const;

	void parseCatchStatement(TokenRange toks, const std::shared_ptr<Statement>& prevStatement) const;

	static std::set<std::string> illegalKeywords;

	void parseDeclaration(TokenRange toks, std::vector<Declaration>& declarations) const;

	void parseStatement(TokenRange toks, std::vector<std::shared_ptr<Statement>>& statements) const;

	std::shared_ptr<Statement> parseCompoundStatement(TokenRange toks) const;

	std::vector<Declaration> parseDescrCodeBlock(TokenRange toks) const;
	std::shared_ptr<CodeBlock> parseCodeBlock(TokenRange toks) const;

	TokenRange range(int beginPos, int endPos) const { return TokenRange(tokens, beginPos, endPos); }

	Descr parseDescr(int pos, int& end) const;

	Actor parseActor(int pos, int& end) const;

	std::string str(const TokenRange& tokens) const;
	std::string str(const std::vector<Token>& tokens) const;
	std::string str(int begin, int end) const { return str(range(begin, end)); }

	void countParens();

	std::vector<Token> tokenize(const std::string& text);

private:
	std::vector<boost::regex> initializeTokenExpressions();
	bool parseClassContext(TokenRange toks, std::string& name) const;
	std::string trim(const std::string& str) const;
	std::string trimStart(const std::string& str, const std::string& chars) const {
		auto firstNonChar = str.find_first_not_of(chars);
		if (firstNonChar == std::string::npos) {
			return ""; // All trimmed characters
		}

		return str.substr(firstNonChar);
	}
};

} // namespace actorcompiler

#endif
