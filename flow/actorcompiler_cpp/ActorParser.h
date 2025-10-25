/*
 * ActorParser.h
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

#ifndef ACTORCOMPILER_ACTORPARSER_H
#define ACTORCOMPILER_ACTORPARSER_H

#include "Token.h"
#include "TokenRange.h"
#include "ParseTree.h"
#include "Error.h"
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>
#include <ostream>

namespace actorcompiler {

// Parser for Flow actor syntax
class ActorParser {
private:
	std::vector<Token> tokens;
	std::string sourceFile;
	ErrorMessagePolicy errorMessagePolicy;
	bool generateProbes;
	bool lineNumbersEnabled = true;
	std::map<std::pair<uint64_t, uint64_t>, std::string> uidObjects;

public:
	ActorParser(const std::string& text,
	            const std::string& sourceFile,
	            const ErrorMessagePolicy& errorMessagePolicy,
	            bool generateProbes);

	// Write processed output to stream
	void write(std::ostream& writer, const std::string& destFileName);

	// Get UID mappings for actors
	const std::map<std::pair<uint64_t, uint64_t>, std::string>& getUidObjects() const { return uidObjects; }

private:
	// Parsing methods (to be implemented in Step 4)
	Actor parseActor(size_t pos, size_t& end);
	Descr parseDescr(size_t pos, size_t& end);

	// Statement and helper parsers
	CodeBlock parseCodeBlock(const TokenRange& toks);
	Statement* parseCompoundStatement(const TokenRange& toks);
	void parseStatement(const TokenRange& toks, std::vector<std::unique_ptr<Statement>>& statements);
	LoopStatement* parseLoopStatement(const TokenRange& toks);
	ChooseStatement* parseChooseStatement(const TokenRange& toks);
	WhenStatement* parseWhenStatement(const TokenRange& toks);
	StateDeclarationStatement* parseStateDeclaration(const TokenRange& toks);
	ReturnStatement* parseReturnStatement(const TokenRange& toks);
	ThrowStatement* parseThrowStatement(const TokenRange& toks);
	WaitStatement* parseWaitStatement(const TokenRange& toks);
	WhileStatement* parseWhileStatement(const TokenRange& toks);
	Statement* parseForStatement(const TokenRange& toks);
	IfStatement* parseIfStatement(const TokenRange& toks);
	void parseElseStatement(const TokenRange& toks, Statement* prevStatement);
	TryStatement* parseTryStatement(const TokenRange& toks);
	void parseCatchStatement(const TokenRange& toks, Statement* prevStatement);

	void parseDescrHeading(Descr& descr, const TokenRange& toks);
	std::vector<Declaration> parseDescrCodeBlock(const TokenRange& toks);

	bool parseClassContext(TokenRange toks, std::string& name);
	void parseActorHeading(Actor& actor, TokenRange toks);
	void parseTestCaseHeading(Actor& actor, TokenRange toks);

	// Declaration helpers
	void parseDeclaration(TokenRange tokens,
	                      Token& name,
	                      TokenRange& type,
	                      TokenRange& initializer,
	                      bool& constructorSyntax);
	VarDeclaration parseVarDeclaration(const TokenRange& tokens);

	// Predicates
	static bool isWhitespace(const Token& t) { return t.isWhitespace(); }
	static bool isNonWhitespace(const Token& t) { return !t.isWhitespace(); }

	// Helper methods
	TokenRange range(size_t begin, size_t end) const;
	std::string str(const TokenRange& range) const;
	std::string norm(const TokenRange& range) const; // normalize whitespace to single spaces
};

} // namespace actorcompiler

#endif // ACTORCOMPILER_ACTORPARSER_H
