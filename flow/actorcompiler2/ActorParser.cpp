/*
 * ActorParser.cpp
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

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <vector>
#include <string>
#include <regex>
#include <memory>
#include <numeric>
#include <list>
#include <sstream>
#include <functional>

#include "ActorCompiler.h"
#include "ActorParser.h"
#include "ParseTree.h"

namespace actorcompiler {

Token Token::Assert(const std::string& error, std::function<bool(const Token&)> pred) const {
	if (!pred(*this))
		throw Error(sourceLine, error);
	return *this;
}

TokenRange Token::getMatchingRangeIn(const TokenRange& range) const {
	std::function<bool(const Token&)> pred;
	int dir = 0;
	if (value == "(") {
		pred = [&](const Token& t) -> bool { return t.value != ")" || t.parenDepth != parenDepth; };
		dir += 1;
	} else if (value == ")") {
		pred = [&](const Token& t) -> bool { return t.value != "(" || t.parenDepth != parenDepth; };
		dir -= 1;
	} else if (value == "{") {
		pred = [&](const Token& t) -> bool { return t.value != "}" || t.braceDepth != braceDepth; };
		dir += 1;
	} else if (value == "}") {
		pred = [&](const Token& t) -> bool { return t.value != "{" || t.braceDepth != braceDepth; };
		dir -= 1;
	} else if (value == "<") {
		return TokenRange(
		    range.getAllTokens(),
		    position + 1,
		    AngleBracketParser::notInsideAngleBrackets(TokenRange(range.getAllTokens(), position, range.end()))[1]
		        .position);
		// skip the "<", which is considered "outside"
		// get the ">", which is likewise "outside"

	} else if (value == "[") {
		return TokenRange(
		    range.getAllTokens(),
		    position + 1,
		    BracketParser::notInsideBrackets(TokenRange(range.getAllTokens(), position, range.end()))[1].position);
		// skip the "[", which is considered "outside"
		// get the "]", which is likewise "outside"
	} else {
		std::cerr << "Error: Can't match this token " << value << std::endl;
		throw std::runtime_error("Can't match this token!");
	}

	TokenRange r;
	if (dir == -1) {
		r = TokenRange(range.getAllTokens(), range.begin(), position).RevTakeWhile(pred);
		if (r.begin() == range.begin())
			throw Error(sourceLine, "Syntax error: Unmatched " + value);
	} else {
		r = TokenRange(range.getAllTokens(), position + 1, range.end()).TakeWhile(pred);
		if (r.end() == range.end())
			throw Error(sourceLine, "Syntax error: Unmatched " + value);
	}
	return r;
}

std::optional<Token> first_of(const std::vector<Token>& tokens, std::function<bool(const Token&)> predicate) {
	for (const auto& token : tokens) {
		if (predicate(token)) {
			return token;
		}
	}
	return std::nullopt;
}

// BracketParser namespace
namespace BracketParser {
std::vector<Token> notInsideBrackets(const TokenRange& tokens) {
	int bracketDepth = 0;
	std::optional<int> basePD = std::nullopt;
	std::vector<Token> result;
	for (int i = tokens.begin(); i < tokens.end(); i++) {
		const auto& tok = tokens[i];
		if (!basePD.has_value())
			basePD = tok.parenDepth;
		if (tok.parenDepth == basePD && tok.value == "]")
			bracketDepth--;
		if (bracketDepth == 0)
			result.push_back(tok);
		if (tok.parenDepth == basePD && tok.value == "[")
			bracketDepth++;
	}
	return result;
}
} // namespace BracketParser

// AngleBracketParser namespace
namespace AngleBracketParser {
std::vector<Token> notInsideAngleBrackets(const TokenRange& tokens) {
	int angleDepth = 0;
	std::optional<int> basePD = std::nullopt;
	std::vector<Token> result;
	for (int i = tokens.begin(); i < tokens.end(); i++) {
		const auto& tok = tokens[i];
		if (!basePD.has_value())
			basePD = tok.parenDepth;
		if (tok.parenDepth == basePD && tok.value == ">")
			angleDepth--;
		if (angleDepth == 0)
			result.push_back(tok);
		if (tok.parenDepth == basePD && tok.value == "<")
			angleDepth++;
	}
	return result;
}
} // namespace AngleBracketParser

// ActorParser class
std::set<std::string> ActorParser::illegalKeywords = { "goto", "do", "finally", "__if_exists", "__if_not_exists" };

bool ActorParser::parseClassContext(const TokenRange& toks, std::string& name) const {
	name = "";
	if (toks.empty()) {
		return false;
	}

	// http://nongnu.org/hcb/#attribute-specifier-seq
	Token first;
	TokenRange currentRange = toks;

	while (true) {
		first = currentRange.first(NonWhitespace).value();
		if (first.value == "[") {
			auto contents = first.getMatchingRangeIn(currentRange);
			currentRange = range(contents.end() + 1, currentRange.end());
		} else if (first.value == "alignas") {
			currentRange = range(first.position + 1, currentRange.end());
			first = currentRange.first(NonWhitespace).value();

			if (first.value != "(") {
				assert(false);
				throw std::runtime_error("Expected ( after alignas");
			}

			auto contents = first.getMatchingRangeIn(currentRange);
			currentRange = range(contents.end() + 1, currentRange.end());
		} else {
			break;
		}
	}

	// http://nongnu.org/hcb/#class-head-name
	first = currentRange.first(NonWhitespace).value();
	if (!std::regex_match(first.value, identifierPattern)) {
		return false;
	}

	while (true) {
		if (!std::regex_match(first.value, identifierPattern)) {
			assert(false);
			throw std::runtime_error("Expected identifier");
		}

		name += first.value;
		currentRange = range(first.position + 1, currentRange.end());

		if (currentRange.first(NonWhitespace).value().value == "::") {
			name += "::";
			currentRange = currentRange.SkipWhile(Whitespace).skip(1);
		} else {
			break;
		}

		first = currentRange.first(NonWhitespace).value();
	}

	// http://nongnu.org/hcb/#class-virt-specifier-seq
	auto predicate = [&](const Token& t) -> bool {
		return Whitespace(t) || t.value == "final" || t.value == "explicit";
	};
	currentRange = currentRange.SkipWhile(predicate);

	first = currentRange.first(NonWhitespace).value();
	if (first.value == ":" || first.value == "{") {
		// At this point we've confirmed that this is a class.
		return true;
	}

	return false;
}

static std::vector<std::string> split(const std::string& str) {
	std::vector<std::string> lines;
	size_t start = 0;
	size_t end = 0;

	while ((end = str.find('\n', start)) != std::string::npos) {
		lines.push_back(str.substr(start, end - start));
		start = end + 1;
	}

	// Add the last line if there is one
	if (start < str.length()) {
		lines.push_back(str.substr(start));
	}

	return lines;
}

void ActorParser::write(std::ostream& writer, const std::string& destFileName) {
	writer << "#define POST_ACTOR_COMPILER 1\n";
	int outLine = 1;
	if (lineNumbersEnabled) {
		writer << "#line " << tokens[0].sourceLine << " \"" << sourceFile << "\"\n";
		outLine++;
	}
	int inBlocks = 0;
	std::list<ClassContext> classContextStack;
	for (int i = 0; i < tokens.size(); i++) {
		if (tokens[0].sourceLine == 0) {
			assert(false);
			throw std::runtime_error("Internal error: Invalid source line (0)");
		}

		if (tokens[i].value == "ACTOR" || tokens[i].value == "SWIFT_ACTOR" || tokens[i].value == "TEST_CASE") {
			int end;
			auto actor = parseActor(i, end);
			if (classContextStack.size() > 0)
				actor.enclosingClass = std::accumulate(classContextStack.rbegin(),
				                                       classContextStack.rend(),
				                                       std::string(),
				                                       [](const std::string& acc, const ClassContext& ctx) {
					                                       return acc.empty() ? ctx.name : acc + "::" + ctx.name;
				                                       });
			auto actorWriter = std::make_unique<std::stringstream>();

			auto actorCompiler = ActorCompiler(actor, sourceFile, inBlocks == 0, lineNumbersEnabled, generateProbes);
			actorCompiler.Write(*actorWriter);
			for (auto& [key, value] : actorCompiler.uidObjects) {
				uidObjects[key] = value;
			}

			std::vector<std::string> actorLines = split(actorWriter->str());
			bool hasLineNumber = false;
			bool hadLineNumber = true;
			for (const auto& line : actorLines) {
				if (lineNumbersEnabled) {
					bool isLineNumber = line.find("#line") != std::string::npos;
					if (isLineNumber)
						hadLineNumber = true;
					if (!isLineNumber && !hasLineNumber && hadLineNumber) {
						writer << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line " << outLine + 1 << " \"" << destFileName
						       << "\"\n";
						outLine++;
						hadLineNumber = false;
					}
					hasLineNumber = isLineNumber;
				}
				writer << line; // TODO: line.TrimEnd('\n','\r')
				outLine++;
			}
			i = end;
			if (i != tokens.size() && lineNumbersEnabled) {
				writer << "#line " << tokens[i].sourceLine << " \"" << sourceFile << "\"\n";
				outLine++;
			}
		} else if (tokens[i].value == "DESCR") {
			int end;
			auto descr = parseDescr(i, end);
			int lines;
			DescrCompiler(descr, tokens[i].braceDepth).write(writer, lines);
			i = end;
			outLine += lines;
			if (i != tokens.size() && lineNumbersEnabled) {
				writer << "#line " << tokens[i].sourceLine << " \"" << sourceFile << "\"\n";
				outLine++;
			}
		} else if (tokens[i].value == "class" || tokens[i].value == "struct" || tokens[i].value == "union") {
			writer << tokens[i].value;
			std::string name;
			if (parseClassContext(TokenRange(tokens, i + 1, tokens.size()), name))
				classContextStack.push_back(ClassContext{ name, inBlocks });
		} else {
			if (tokens[i].value == "{")
				inBlocks++;
			else if (tokens[i].value == "}") {
				inBlocks--;
				if (classContextStack.size() > 0 && classContextStack.back().inBlocks == inBlocks)
					classContextStack.pop_back();
			}
			writer << tokens[i].value;
			outLine += std::count(tokens[i].value.begin(), tokens[i].value.end(), '\n');
		}
	}
}

std::vector<TokenRange> ActorParser::splitParameterList(const TokenRange& toks, const std::string& delimiter) const {
	std::vector<TokenRange> result;
	if (toks.empty())
		return result;

	TokenRange toks2 = toks;
	while (true) {
		auto tokens = AngleBracketParser::notInsideAngleBrackets(toks2);
		int i = 0;
		for (; i < tokens.size(); i++) {
			if (tokens[i].value == delimiter && tokens[i].parenDepth == toks2.first().parenDepth) {
				break;
			}
		}
		if (i == tokens.size())
			break;
		result.push_back(range(toks2.begin(), tokens[i].position));
		toks2 = range(tokens[i].position + 1, toks2.end());
	}
	result.push_back(toks2);
	return result;
}

std::vector<Token> ActorParser::normalizeWhitespace(const std::vector<Token>& tokens) const {
	std::vector<Token> result;
	bool inWhitespace = false;
	bool leading = true;
	for (const auto& tok : tokens) {
		if (!tok.isWhitespace()) {
			if (inWhitespace && !leading)
				result.emplace_back(" ");
			inWhitespace = false;
			result.push_back(tok);
			leading = false;
		} else {
			inWhitespace = true;
		}
	}
	return result;
}

std::vector<Token> ActorParser::normalizeWhitespace(const TokenRange& tokenstokens) const {
	std::vector<Token> result;
	bool inWhitespace = false;
	bool leading = true;
	for (int i = tokenstokens.begin(); i < tokenstokens.end(); i++) {
		const auto& tok = tokenstokens[i];
		if (!tok.isWhitespace()) {
			if (inWhitespace && !leading)
				result.emplace_back(" ");
			inWhitespace = false;
			result.push_back(tok);
			leading = false;
		} else {
			inWhitespace = true;
		}
	}
	return result;
}

void ActorParser::parseDeclaration(const TokenRange& tokens,
                                   Token& name,
                                   TokenRange& type,
                                   TokenRange& initializer,
                                   bool& constructorSyntax) const {
	initializer = TokenRange();
	auto beforeInitializer = tokens;
	constructorSyntax = false;

	auto equals = first_of(AngleBracketParser::notInsideAngleBrackets(tokens),
	                       [&](const Token& t) { return t.value == "=" && t.parenDepth == tokens.first().parenDepth; });
	if (equals.has_value()) {
		beforeInitializer = range(tokens.begin(), equals.value().position);
		initializer = range(equals.value().position + 1, tokens.end());
	} else {
		auto paren = first_of(AngleBracketParser::notInsideAngleBrackets(tokens), [&](const Token& t) {
			return t.value == "(" && t.parenDepth == tokens.first().parenDepth;
		});
		if (paren.has_value()) {
			constructorSyntax = true;
			beforeInitializer = range(tokens.begin(), paren.value().position);
			initializer = range(paren.value().position + 1, tokens.end()).TakeWhile([&](const Token& t) {
				return t.parenDepth > paren.value().parenDepth;
			});
		} else {
			auto brace = first_of(AngleBracketParser::notInsideAngleBrackets(tokens),
			                      [&](const Token& t) { return t.value == "{"; });
			if (brace.has_value()) {
				throw Error(brace.value().sourceLine,
				            "Uniform initialization syntax is not currently supported for state variables (use '(' "
				            "instead of '}}' ?)");
			}
		}
	}
	name = beforeInitializer.last(NonWhitespace);
	if (beforeInitializer.begin() == name.position)
		throw Error(beforeInitializer.first().sourceLine, "Declaration has no type.");
	type = range(beforeInitializer.begin(), name.position);
}

VarDeclaration ActorParser::parseVarDeclaration(const TokenRange& tokens) const {
	Token name;
	TokenRange type, initializer;
	bool constructorSyntax;
	parseDeclaration(tokens, name, type, initializer, constructorSyntax);
	return VarDeclaration{ .type = str(normalizeWhitespace(type)),
		                   .name = name.value,
		                   .initializer = initializer.empty() ? "" : str(normalizeWhitespace(initializer)),
		                   .initializerConstructorSyntax = constructorSyntax };
}

void ActorParser::parseDescrHeading(Descr& descr, const TokenRange& toks) const {
	// Check if the first non-whitespace token is "struct"
	Token firstToken = toks.first(NonWhitespace).value();
	if (firstToken.value != "struct") {
		assert(false);
		throw std::runtime_error("non-struct DESCR!");
	}

	// Skip whitespace, the struct token, and more whitespace
	TokenRange currentToks = toks.SkipWhile(Whitespace).skip(1).SkipWhile(Whitespace);

	// Find a colon if it exists
	Token colon;
	bool hasColon = false;

	// Implementation of FirstOrDefault in C++
	for (int i = currentToks.begin(); i < currentToks.end(); ++i) {
		if (currentToks[i].value == ":") {
			colon = currentToks[i];
			hasColon = true;
			break;
		}
	}

	if (hasColon) {
		descr.superClassList = trim(str(range(colon.position + 1, currentToks.end())));
		currentToks = range(currentToks.begin(), colon.position);
	}

	descr.name = trim(str(currentToks));
}

void ActorParser::parseTestCaseHeading(Actor& actor, const TokenRange& toks) const {
	actor.isStatic = true;
	auto paramRange =
	    toks.last(NonWhitespace)
	        .Assert("Unexpected tokens after test case parameter list.",
	                [&](const Token& t) { return t.value == ")" && t.parenDepth == toks.first().parenDepth; })
	        .getMatchingRangeIn(toks);
	actor.testCaseParameters = str(paramRange);
	actor.name = "flowTestCase" + std::to_string(toks.first().sourceLine);
	actor.parameters = std::vector<VarDeclaration>{ VarDeclaration{
		.type = "UnitTestParameters", .name = "params", .initializer = "", .initializerConstructorSyntax = false } };
	actor.returnType = "Void";
}

void ActorParser::parseActorHeading(Actor& actor, const TokenRange& toks) const {
	auto templateToken = toks.first(NonWhitespace).value();
	TokenRange toks2 = toks;
	if (templateToken.value == "template") {
		auto templateParams = range(templateToken.position + 1, toks.end())
		                          .first(NonWhitespace)
		                          .value()
		                          .Assert("Invalid template declaration", [](const Token& t) { return t.value == "<"; })
		                          .getMatchingRangeIn(toks);

		auto params = splitParameterList(templateParams, ",");
		for (const auto& param : params) {
			actor.templateFormals.push_back(parseVarDeclaration(param));
		}
		toks2 = range(templateParams.end() + 1, toks.end());
	}
	auto attribute = toks2.first(NonWhitespace).value();
	while (attribute.value == "[") {
		auto attributeContents = attribute.getMatchingRangeIn(toks2);
		if (attributeContents.length() < 2 || attributeContents.first().value != "[" ||
		    attributeContents.last().value != "]")
			throw Error(actor.sourceLine, "Invalid attribute: Expected [[...]]");
		actor.attributes.push_back("[" + str(normalizeWhitespace(attributeContents)) + "]");
		toks2 = range(attributeContents.end() + 1, toks2.end());
		attribute = toks2.first(NonWhitespace).value();
	}

	auto staticKeyword = toks.first(NonWhitespace).value();
	if (staticKeyword.value == "static") {
		actor.isStatic = true;
		toks2 = range(staticKeyword.position + 1, toks2.end());
	}
	auto uncancellableKeyword = toks2.first(NonWhitespace).value();
	if (uncancellableKeyword.value == "UNCANCELLABLE") {
		actor.setUncancellable();
		toks2 = range(uncancellableKeyword.position + 1, toks2.end());
	}

	// Find the parameter list
	auto paramRange =
	    toks2.last(NonWhitespace)
	        .Assert("Unexpected tokens after actor parameter list.",
	                [&](const Token& t) { return t.value == ")" && t.parenDepth == toks2.first().parenDepth; })
	        .getMatchingRangeIn(toks2);
	auto params = splitParameterList(paramRange, ",");
	for (const auto& param : params) {
		actor.parameters.push_back(parseVarDeclaration(param));
	}
	auto name = range(toks2.begin(), paramRange.begin() - 1).last(NonWhitespace);
	actor.name = name.value;

	auto returnType = TokenRange(toks2.getAllTokens(), toks2.first().position + 1, name.position).SkipWhile(Whitespace);
	auto retToken = returnType.first();
	if (retToken.value == "Future") {
		auto ofType = returnType.skip(1)
		                  .first(NonWhitespace)
		                  .value()
		                  .Assert("Expected <", [&](const Token& t) { return t.value == "<"; })
		                  .getMatchingRangeIn(returnType);
		actor.returnType = str(normalizeWhitespace(ofType));
		toks2 = range(ofType.end() + 1, returnType.end());
	} else if (retToken.value ==
	           "void" /* && !returnType.skip(1).any([this](const Token& t) { return !t.isWhitespace(); }) */) {
		actor.returnType = ""; // XXX
		toks2 = returnType.skip(1);
	} else {
		throw Error(actor.sourceLine, "Actor apparently does not return Future<T>");
	}
	toks2 = toks2.SkipWhile(Whitespace);
	if (!toks2.empty()) {
		if (toks2.last().value == "::") {
			actor.nameSpace = str(range(toks2.begin(), toks2.end() - 1));
		} else {
			std::cerr << "Tokens: '" << str(toks2) << "' " << toks2.length() << " '" << toks2.last().value << "'\n";
			throw Error(actor.sourceLine, "Unrecognized tokens preceding parameter list in actor declaration");
		}
	}
	bool hasFlowAttribute = std::any_of(actor.attributes.begin(), actor.attributes.end(), [](const std::string& s) {
		return s == "[[flow_allow_discard]]";
	});
	if (errorMessagePolicy.ActorsNoDiscardByDefault() && !hasFlowAttribute) {
		if (actor.isCancellable()) {
			actor.attributes.push_back("[[nodiscard]]");
		}
	}
	std::string knownFlowAttributes = "[[flow_allow_discard]]";
	for (const auto& attribute : actor.attributes) {
		if (attribute.starts_with("[[flow_") && knownFlowAttributes != attribute) {
			throw Error(actor.sourceLine, "Unknown flow attribute " + attribute);
		}
	}
	actor.attributes.erase(std::remove_if(actor.attributes.begin(),
	                                      actor.attributes.end(),
	                                      [&](const std::string& a) { return a.starts_with("[[flow_"); }),
	                       actor.attributes.end());
}

std::shared_ptr<LoopStatement> ActorParser::parseLoopStatement(const TokenRange& toks) const {
	return std::make_shared<LoopStatement>(parseCompoundStatement(toks.consume("loop")));
}

std::shared_ptr<ChooseStatement> ActorParser::parseChooseStatement(const TokenRange& toks) const {
	return std::make_shared<ChooseStatement>(parseCompoundStatement(toks.consume("choose")));
}

std::shared_ptr<WhenStatement> ActorParser::parseWhenStatement(const TokenRange& toks) const {
	auto expr = toks.consume("when")
	                .SkipWhile(Whitespace)
	                .first()
	                .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	                .getMatchingRangeIn(toks)
	                .SkipWhile(Whitespace);
	return std::make_shared<WhenStatement>(parseWaitStatement(expr),
	                                       parseCompoundStatement(range(expr.end() + 1, toks.end())));
}

std::shared_ptr<StateDeclarationStatement> ActorParser::parseStateDeclaration(const TokenRange& toks) const {
	auto toks2 = toks.consume("state").RevSkipWhile([](const Token& t) { return t.value == ";"; });
	return std::make_shared<StateDeclarationStatement>(parseVarDeclaration(toks2));
}

std::shared_ptr<ReturnStatement> ActorParser::parseReturnStatement(const TokenRange& toks) const {
	auto toks2 = toks.consume("return").RevSkipWhile([](const Token& t) { return t.value == ";"; });
	return std::make_shared<ReturnStatement>(str(normalizeWhitespace(toks2)));
}

std::shared_ptr<ThrowStatement> ActorParser::parseThrowStatement(const TokenRange& toks) const {
	auto toks2 = toks.consume("throw").RevSkipWhile([](const Token& t) { return t.value == ";"; });
	return std::make_shared<ThrowStatement>(str(normalizeWhitespace(toks2)));
}

std::shared_ptr<WaitStatement> ActorParser::parseWaitStatement(const TokenRange& toks) const {
	std::shared_ptr<WaitStatement> ws = std::make_shared<WaitStatement>();
	ws->firstSourceLine = toks.first().sourceLine;
	if (toks.first().value == "state") {
		ws->resultIsState = true;
		toks.consume("state");
	}
	TokenRange initializer;
	if (toks.first().value == "wait" || toks.first().value == "waitNext") {
		initializer = toks.RevSkipWhile([](const Token& t) { return t.value == ";"; });
		ws->result = VarDeclaration{ "_", "Void", "", false };
	} else {
		Token name;
		TokenRange type;
		bool constructorSyntax;
		parseDeclaration(toks.RevSkipWhile([](const Token& t) { return t.value == ";"; }),
		                 name,
		                 type,
		                 initializer,
		                 constructorSyntax);
		std::string typestring = str(normalizeWhitespace(type));
		if (typestring == "Void") {
			throw Error(ws->firstSourceLine,
			            "Assigning the result of a Void wait is not allowed. Just use a standalone wait statement.");
		}
		ws->result = VarDeclaration{ name.value, str(normalizeWhitespace(type)), "", false };
	}
	if (initializer.empty())
		throw Error(ws->firstSourceLine, "Wait statement must be a declaration or standalone statement");
	auto waitParams =
	    initializer.SkipWhile(Whitespace)
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
	        .SkipWhile(Whitespace)
	        .first()
	        .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	        .getMatchingRangeIn(initializer);
	if (!range(waitParams.end(), initializer.end()).consume(")").all(Whitespace)) {
		throw Error(toks.first().sourceLine,
		            "Statement contains a wait, but is not a valid wait statement or a supported compound statement.2");
	}
	ws->futureExpression = str(normalizeWhitespace(waitParams));
	return ws;
}

std::shared_ptr<WhileStatement> ActorParser::parseWhileStatement(const TokenRange& toks) const {
	auto expr = toks.consume("while")
	                .first(NonWhitespace)
	                .value()
	                .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	                .getMatchingRangeIn(toks);
	return std::make_shared<WhileStatement>(str(normalizeWhitespace(expr)),
	                                        parseCompoundStatement(range(expr.end() + 1, toks.end())));
}

std::shared_ptr<Statement> ActorParser::parseForStatement(const TokenRange& toks) const {
	auto head = toks.consume("for")
	                .first(NonWhitespace)
	                .value()
	                .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	                .getMatchingRangeIn(toks);
	auto delim = head.where([&](const Token& t) {
		return t.parenDepth == head.first().parenDepth && t.braceDepth == head.first().braceDepth && t.value == ";";
	});
	if (delim.size() == 2) {
		auto init = range(head.begin(), delim[0].position);
		auto cond = range(delim[0].position + 1, delim[1].position);
		auto next = range(delim[1].position + 1, head.end());
		auto body = range(head.end() + 1, toks.end());
		return std::make_shared<ForStatement>(str(normalizeWhitespace(init)),
		                                      str(normalizeWhitespace(cond)),
		                                      str(normalizeWhitespace(next)),
		                                      parseCompoundStatement(body));
	}
	delim = head.where([&](const Token& t) {
		return t.parenDepth == head.first().parenDepth && t.braceDepth == head.first().braceDepth && t.value == ":";
	});
	if (delim.size() != 1) {
		throw Error(head.first().sourceLine, "for statement must be 3-arg style or c++11 2-arg style");
	}
	return std::make_shared<RangeForStatement>(
	    str(normalizeWhitespace(range(head.begin(), delim[0].position - 1).SkipWhile(Whitespace))),
	    str(normalizeWhitespace(range(delim[0].position + 1, head.end()).SkipWhile(Whitespace))),
	    parseCompoundStatement(range(head.end() + 1, toks.end())));
}

std::shared_ptr<Statement> ActorParser::parseIfStatement(const TokenRange& toks) const {
	auto toks2 = toks.consume("if");
	toks2 = toks2.SkipWhile(Whitespace);
	bool _constexpr = toks2.first().value == "constexpr";
	if (_constexpr) {
		toks2 = toks2.consume("constexpr").SkipWhile(Whitespace);
	}

	auto expr = toks2.first(NonWhitespace)
	                .value()
	                .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	                .getMatchingRangeIn(toks2);
	return std::make_shared<IfStatement>(str(normalizeWhitespace(expr)),
	                                     _constexpr,
	                                     parseCompoundStatement(range(expr.end() + 1, toks2.end())),
	                                     nullptr);
}

void ActorParser::parseElseStatement(const TokenRange& toks, const std::shared_ptr<Statement>& prevStatement) const {
	auto ifStatement = std::dynamic_pointer_cast<IfStatement>(prevStatement);
	while (ifStatement != nullptr && ifStatement->elseBody != nullptr)
		ifStatement = std::dynamic_pointer_cast<IfStatement>(ifStatement->elseBody);

	if (ifStatement == nullptr)
		throw Error(toks.first().sourceLine, "else without matching if");

	ifStatement->elseBody = parseCompoundStatement(toks.consume("else"));
}

std::shared_ptr<Statement> ActorParser::parseTryStatement(const TokenRange& toks) const {
	return std::make_shared<TryStatement>(parseCompoundStatement(toks.consume("try")),
	                                      std::vector<TryStatement::Catch>{});
}

void ActorParser::parseCatchStatement(const TokenRange& toks, const std::shared_ptr<Statement>& prevStatement) const {
	auto tryStatement = std::dynamic_pointer_cast<TryStatement>(prevStatement);
	if (!tryStatement)
		throw Error(toks.first().sourceLine, "catch without matching try");
	auto expr = toks.consume("catch")
	                .first(NonWhitespace)
	                .value()
	                .Assert("Expected (", [&](const Token& t) { return t.value == "("; })
	                .getMatchingRangeIn(toks);
	tryStatement->catches.push_back(TryStatement::Catch{ str(normalizeWhitespace(expr)),
	                                                     parseCompoundStatement(range(expr.end() + 1, toks.end())),
	                                                     expr.first().sourceLine });
}

void ActorParser::parseDeclaration(const TokenRange& toks, std::vector<Declaration>& declarations) const {
	Declaration dec;
	auto delim = toks.first([&](const Token& t) { return t.value == ";"; }).value();
	auto nameRange = range(toks.begin(), delim.position).RevSkipWhile(Whitespace).RevTakeWhile(NonWhitespace);
	auto typeRange = range(toks.begin(), nameRange.begin());
	auto commentRange = range(delim.position + 1, toks.end());

	dec.name = trim(str(nameRange));
	dec.type = trim(str(typeRange));
	dec.comment = trimStart(trim(str(commentRange)), "/");
	declarations.push_back(dec);
}

void ActorParser::parseStatement(const TokenRange& toks, std::vector<std::shared_ptr<Statement>>& statements) const {
	auto toks2 = toks.SkipWhile(Whitespace);
	std::function<void(const std::shared_ptr<Statement>&)> add = [&statements,
	                                                              &toks2](std::shared_ptr<Statement> stmt) {
		stmt->firstSourceLine = toks2.first().sourceLine;
		statements.push_back(stmt);
	};
	const std::string& value = toks2.first().value;
	if (value == "loop") {
		add(parseLoopStatement(toks2));
	} else if (value == "while") {
		add(parseWhileStatement(toks2));
	} else if (value == "for") {
		add(parseForStatement(toks2));
	} else if (value == "break") {
		add(std::make_shared<BreakStatement>());
	} else if (value == "continue") {
		add(std::make_shared<ContinueStatement>());
	} else if (value == "return") {
		add(parseReturnStatement(toks2));
	} else if (value == "{") {
		add(parseCompoundStatement(toks2));
	} else if (value == "if") {
		add(parseIfStatement(toks2));
	} else if (value == "else") {
		parseElseStatement(toks2, statements.back());
	} else if (value == "choose") {
		add(parseChooseStatement(toks2));
	} else if (value == "when") {
		add(parseWhenStatement(toks2));
	} else if (value == "try") {
		add(parseTryStatement(toks2));
	} else if (value == "catch") {
		parseCatchStatement(toks2, statements.back());
	} else if (value == "throw") {
		add(parseThrowStatement(toks2));
	} else {
		if (illegalKeywords.count(toks2.first().value)) {
			throw Error(toks2.first().sourceLine,
			            std::format("Statement '{}' not supported in actors.", toks2.first().value));
		}
		if (toks2.any([&](const Token& t) { return t.value == "wait" || t.value == "waitNext"; })) {
			add(parseWaitStatement(toks2));
		} else if (toks2.first().value == "state") {
			add(parseStateDeclaration(toks2));
		} else if (toks2.first().value == "switch" && toks2.any([&](const Token& t) { return t.value == "return"; })) {
			throw Error(toks2.first().sourceLine, "Unsupported compound statement containing return.");
		} else if (toks2.first().value.starts_with("#")) {
			throw Error(toks2.first().sourceLine,
			            std::format("Found \"{}\". Preprocessor directives are not supported within ACTORs",
			                        toks2.first().value));
		} else if (toks2.RevSkipWhile([&](const Token& t) { return t.value == ";"; }).any(NonWhitespace)) {
			add(std::make_shared<PlainOldCodeStatement>(
			    str(normalizeWhitespace(toks2.RevSkipWhile([&](const Token& t) { return t.value == ";"; }))) + ";"));
		}
	}
}

std::shared_ptr<Statement> ActorParser::parseCompoundStatement(const TokenRange& toks) const {
	auto first = toks.first(NonWhitespace).value();
	if (first.value == "{") {
		auto inBraces = first.getMatchingRangeIn(toks);
		if (!range(inBraces.end(), toks.end()).consume("}").all(Whitespace))
			throw Error(inBraces.last().sourceLine, "Unexpected tokens after compound statement");
		return parseCodeBlock(inBraces);
	} else {
		std::vector<std::shared_ptr<Statement>> statements;
		parseStatement(toks.skip(1), statements);
		return statements[0];
	}
}

std::vector<Declaration> ActorParser::parseDescrCodeBlock(const TokenRange& toks) const {
	std::vector<Declaration> declarations;
	TokenRange toks2 = toks;
	while (true) {
		auto delim = toks2.first([&](const Token& t) { return t.value == ";"; });
		if (!delim.has_value())
			break;
		int pos = delim.value().position + 1;
		auto potentialComment =
		    range(pos, toks2.end()).SkipWhile([&](const Token& t) { return t.value == "\t" || t.value == " "; });
		if (!potentialComment.empty() && potentialComment.first().value.starts_with("//"))
			pos = potentialComment.first().position + 1;
		parseDeclaration(range(toks2.begin(), pos), declarations);
		toks2 = range(pos, toks2.end());
	}
	if (!toks2.all(Whitespace))
		throw Error(toks.first(NonWhitespace).value().sourceLine, "Trailing unterminated statement in code block");
	return declarations;
}

std::shared_ptr<CodeBlock> ActorParser::parseCodeBlock(const TokenRange& toks) const {
	std::vector<std::shared_ptr<Statement>> statements;
	TokenRange toks2 = toks;
	while (true) {
		std::optional<Token> delim = toks2.first([&](const Token& t) {
			return t.parenDepth == toks2.first().parenDepth && t.braceDepth == toks2.first().braceDepth &&
			       (t.value == ";" || t.value == "}");
		});
		if (!delim.has_value())
			break;
		parseStatement(range(toks.begin(), delim.value().position + 1), statements);
		toks2 = range(delim.value().position + 1, toks2.end());
	}
	if (!toks2.all(Whitespace))
		throw Error(toks2.first(NonWhitespace).value().sourceLine, "Trailing unterminated statement in code block");
	return std::make_shared<CodeBlock>(statements);
}

Descr ActorParser::parseDescr(int pos, int& end) const {
	Descr descr;
	auto toks = range(pos + 1, tokens.size());
	auto heading = toks.TakeWhile([&](const Token& t) { return t.value != "{"; });
	auto body = range(heading.end() + 1, tokens.size()).TakeWhile([&](const Token& t) {
		return t.braceDepth > toks.first().braceDepth || t.value == ";";
	});
	parseDescrHeading(descr, heading);
	descr.body = parseDescrCodeBlock(body);
	end = body.end() + 1;
	return descr;
}

Actor ActorParser::parseActor(int pos, int& end) const {
	Actor actor;
	auto head_token = tokens[pos];
	actor.sourceLine = head_token.sourceLine;

	auto toks = range(pos + 1, tokens.size());
	auto heading = toks.TakeWhile([&](const Token& t) { return t.value != "{"; });
	auto toSemicolon = toks.TakeWhile([&](const Token& t) { return t.value != ";"; });
	actor.isForwardDeclaration = toSemicolon.length() < heading.length();
	if (actor.isForwardDeclaration) {
		heading = toSemicolon;
		if (head_token.value == "ACTOR" || head_token.value == "SWIFT_ACTOR") {
			parseActorHeading(actor, heading);
		} else {
			head_token.Assert("ACTOR expected!", [&](const Token& t) { return false; });
		}
		end = heading.end() + 1;
	} else {
		auto body = range(heading.end() + 1, tokens.size()).TakeWhile([&](const Token& t) {
			return t.braceDepth > toks.first().braceDepth;
		});
		if (head_token.value == "ACTOR" || head_token.value == "SWIFT_ACTOR") {
			parseActorHeading(actor, heading);
		} else if (head_token.value == "TEST_CASE") {
			parseTestCaseHeading(actor, heading);
			actor.isTestCase = true;
		} else {
			head_token.Assert("ACTOR or TEST_CASE expected!", [&](const Token& t) { return false; });
		}

		actor.body = parseCodeBlock(body);

		if (!actor.body->containsWait())
			errorMessagePolicy.HandleActorWithoutWait(sourceFile, actor);

		end = body.end() + 1;
	}
	return actor;
}

std::string ActorParser::str(const std::vector<Token>& tokens) const {
	std::ostringstream oss;
	for (const auto& token : tokens) {
		oss << token.value;
	}
	return oss.str();
}

std::string ActorParser::str(const TokenRange& tokenrange) const {
	std::string result;
	for (int i = tokenrange.begin(); i < tokenrange.end(); i++) {
		result += tokenrange[i].value;
	}
	return result;
}

void ActorParser::countParens() {
	int braceDepth = 0, parenDepth = 0, lineCount = 1;
	Token lastParen, lastBrace;
	for (int i = 0; i < tokens.size(); i++) {
		if (tokens[i].value == "}") {
			braceDepth--;
			break;
		} else if (tokens[i].value == "{") {
			parenDepth--;
			break;
		} else if (tokens[i].value == "\r\n" || tokens[i].value == "\n") {
			lineCount++;
			break;
		}
		if (braceDepth < 0)
			throw Error(lineCount, "Mismatched braces");
		if (parenDepth < 0)
			throw Error(lineCount, "Mismatched parenthesis");
		tokens[i].position = i;
		tokens[i].sourceLine = lineCount;
		tokens[i].braceDepth = braceDepth;
		tokens[i].parenDepth = parenDepth;
		if (tokens[i].value.starts_with("/*"))
			lineCount += std::count(tokens[i].value.begin(), tokens[i].value.end(), '\n');

		if (tokens[i].value == "{") {
			braceDepth++;
			if (braceDepth == 1)
				lastBrace = tokens[i];
		} else if (tokens[i].value == "(") {
			parenDepth++;
			if (parenDepth == 1)
				lastParen = tokens[i];
			break;
		}
	}
	if (braceDepth != 0)
		throw Error(lastBrace.sourceLine, "Unmatched brace");
	if (parenDepth != 0)
		throw Error(lastParen.sourceLine, "Unmatched parenthesis");
}

std::vector<Token> ActorParser::tokenize(const std::string& text) {
	std::vector<Token> result;
	int pos = 0;
	while (pos < text.length()) {
		bool ok = false;
		for (const auto& re : tokenExpressions) {
			boost::match_results<std::string::const_iterator> match;
			std::string::const_iterator start = text.begin() + pos;
			std::string::const_iterator end = text.end();

			// Boost regex can match from a specific position in the string
			int i = 0;
			if (boost::regex_search(start, end, match, re, boost::match_continuous)) {
				std::string token(match[0].first, match[0].second);
				std::cout << "Token " << i++ << " : " << token << "\n";
				result.emplace_back(token);
				pos += match.length();
				ok = true;
				break;
			}
		}
		if (!ok) {
			assert(false);
			throw std::runtime_error(std::string("Can't tokenize! " + std::to_string(pos)).c_str());
		}
	}
	return result;
}

std::vector<boost::regex> ActorParser::initializeTokenExpressions() {
	std::vector<std::string> patterns = { "\\{",
		                                  "\\}",
		                                  "\\(",
		                                  "\\)",
		                                  "\\[",
		                                  "\\]",
		                                  "//[^\\n]*",
		                                  "/[*]([*][^/]|[^*])*[*]/",
		                                  "'(\\\\.|[^'\\n])*'", // < SOMEDAY: Not fully restrictive
		                                  "\"(\\\\.|[^\"\\n])*\"",
		                                  "[a-zA-Z_][a-zA-Z_0-9]*",
		                                  "\\r\\n",
		                                  "\\n",
		                                  "::",
		                                  ":",
		                                  "#[a-z]*", // Recognize preprocessor directives so that we can reject them
		                                  "." };

	std::vector<boost::regex> regexes;
	for (const auto& pattern : patterns) {
		// boost::regex::perl | boost::regex::mod_s is equivalent to RegexOptions.Singleline
		regexes.push_back(boost::regex("\\G" + pattern, boost::regex::perl | boost::regex::mod_s));
	}

	return regexes;
}

// Removes leading and trailing all whitespace characters
std::string ActorParser::trim(const std::string& str) const {
	const std::string whitespace = " \t\n\r\f\v";

	// Find first non-whitespace character
	auto start = str.find_first_not_of(whitespace);
	if (start == std::string::npos) {
		return ""; // All whitespace
	}

	// Find last non-whitespace character
	auto end = str.find_last_not_of(whitespace);

	// Return the trimmed substring
	return str.substr(start, end - start + 1);
}

} // namespace actorcompiler
