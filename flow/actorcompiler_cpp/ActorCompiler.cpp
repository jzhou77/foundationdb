/*
 * ActorCompiler.cpp
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

#include "ActorCompiler.h"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <vector>
#include <string>
#include <utility>
#include <cstdint>
#include <cstddef>
#include <openssl/sha.h>

namespace actorcompiler {

static uint64_t bytesToU64(const unsigned char* b) {
	uint64_t v = 0;
	for (int i = 0; i < 8; ++i) {
		v = (v << 8) | static_cast<uint64_t>(b[i]);
	}
	return v;
}

static std::pair<uint64_t, uint64_t> getUidFromString(const std::string& s) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char*>(s.data()), s.size(), hash);
	return { bytesToU64(hash), bytesToU64(hash + 8) };
}

ActorCompiler::ActorCompiler(const Actor& actor,
                             const std::string& sourceFile,
                             bool isTopLevel,
                             bool lineNumbersEnabled,
                             bool generateProbes)
  : actor(actor), sourceFile(sourceFile), isTopLevel(isTopLevel), lineNumbersEnabled(lineNumbersEnabled),
    generateProbes(generateProbes) {
	// Derive simple class names for this scaffold
	className = actor.name + "Actor";
	fullClassName = className; // no templates for scaffold
	stateClassName = className + "State";

	// Discover state variables in actor body
	if (actor.body) {
		findState(actor.body.get());
	}

	// Precompute a UID mapping for this actor identifier
	auto key = sourceFile + ":" + actor.name;
	auto uid = getUidFromString(key);
	this->uidObjects[{ uid.first, uid.second }] = key;
}

static std::string join(const std::vector<std::string>& xs, const std::string& sep) {
	std::ostringstream oss;
	for (size_t i = 0; i < xs.size(); ++i) {
		if (i)
			oss << sep;
		oss << xs[i];
	}
	return oss.str();
}

static std::vector<std::string> paramList(const std::vector<VarDeclaration>& params) {
	std::vector<std::string> out;
	out.reserve(params.size());
	for (auto const& p : params) {
		if (!p.initializer.empty())
			out.push_back(p.type + " const& " + p.name + " = " + p.initializer);
		else
			out.push_back(p.type + " const& " + p.name);
	}
	return out;
}

static void writeTemplate(std::ostream& w,
                          const std::vector<VarDeclaration>& tparams,
                          int sourceLine,
                          bool lineNumbersEnabled,
                          const std::string& sourceFile) {
	if (tparams.empty())
		return;
	if (lineNumbersEnabled) {
		w << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line " << sourceLine << " \"" << sourceFile << "\"\n";
	}
	std::vector<std::string> parts;
	parts.reserve(tparams.size());
	for (auto const& p : tparams)
		parts.push_back(p.type + " " + p.name);
	w << "template <" << join(parts, ", ") << ">\n";
}

void ActorCompiler::write(std::ostream& writer) {
	// Minimal scaffold: only emit a wrapper function with a stub body
	const std::string fullReturnType =
	    actor.returnType.empty() ? std::string("void") : (std::string("Future<") + actor.returnType + ">");

	// Forward declaration handling
	if (actor.isForwardDeclaration) {
		for (auto const& attr : actor.attributes)
			writer << attr << ' ';
		if (actor.isStatic)
			writer << "static ";
		writer << fullReturnType << ' ' << (actor.nameSpace.empty() ? std::string() : actor.nameSpace + "::")
		       << actor.name << "( " << join(paramList(actor.parameters), ", ") << " );\n";
		if (!actor.enclosingClass.empty()) {
			writer << "template <class> friend class " << stateClassName << ";\n";
		}
		return;
	}

	writeTemplate(writer, actor.templateFormals, actor.sourceLine, lineNumbersEnabled, sourceFile);
	if (lineNumbersEnabled) {
		writer << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line " << actor.sourceLine << " \"" << sourceFile << "\"\n";
	}
	for (auto const& attr : actor.attributes)
		writer << attr << ' ';
	if (actor.isStatic)
		writer << "static ";
	writer << fullReturnType << ' ' << (actor.nameSpace.empty() ? std::string() : actor.nameSpace + "::") << actor.name
	       << "( " << join(paramList(actor.parameters), ", ") << " ) {\n";
	if (!actor.returnType.empty()) {
		writer << "\treturn " << fullReturnType << "();\n";
	}
	writer << "}\n";

	// Emit ACTOR_TEST_CASE macro if present
	if (!actor.testCaseParameters.empty()) {
		writer << "ACTOR_TEST_CASE(" << actor.name << ", " << actor.testCaseParameters << ")\n";
	}
}

DescrCompiler::DescrCompiler(const Descr& descr, int braceDepth) : descr(descr) {
	memberIndentStr = std::string(braceDepth, '\t');
}

void DescrCompiler::write(std::ostream& writer, int& lines) {
	// Minimal scaffold: emit a simple struct with fields and optional base classes.
	lines = 0;
	writer << memberIndentStr;
	if (!descr.superClassList.empty())
		writer << "struct " << descr.name << " : " << descr.superClassList << " {\n";
	else
		writer << "struct " << descr.name << " {\n";
	lines += 1;
	for (auto const& d : descr.body) {
		writer << memberIndentStr << "\t" << d.type << ' ' << d.name;
		if (!d.comment.empty())
			writer << " //" << d.comment;
		writer << ";\n";
		lines += 1;
	}
	writer << memberIndentStr << "};\n";
	lines += 1;
}

void ActorCompiler::findState(Statement* stmt) {
	if (!stmt)
		return;

	// Check if this is a state declaration
	if (auto* stateDecl = dynamic_cast<StateDeclarationStatement*>(stmt)) {
		stateVariables.insert(stateDecl->decl.name);
		return;
	}

	// Recursively traverse compound statements
	if (auto* codeBlock = dynamic_cast<CodeBlock*>(stmt)) {
		for (auto& s : codeBlock->statements) {
			findState(s.get());
		}
	} else if (auto* whileStmt = dynamic_cast<WhileStatement*>(stmt)) {
		findState(whileStmt->body.get());
	} else if (auto* forStmt = dynamic_cast<ForStatement*>(stmt)) {
		findState(forStmt->body.get());
	} else if (auto* rangeForStmt = dynamic_cast<RangeForStatement*>(stmt)) {
		findState(rangeForStmt->body.get());
	} else if (auto* loopStmt = dynamic_cast<LoopStatement*>(stmt)) {
		findState(loopStmt->body.get());
	} else if (auto* ifStmt = dynamic_cast<IfStatement*>(stmt)) {
		findState(ifStmt->ifBody.get());
		findState(ifStmt->elseBody.get());
	} else if (auto* tryStmt = dynamic_cast<TryStatement*>(stmt)) {
		findState(tryStmt->tryBody.get());
		for (auto& catchClause : tryStmt->catches) {
			findState(catchClause.body.get());
		}
	} else if (auto* chooseStmt = dynamic_cast<ChooseStatement*>(stmt)) {
		findState(chooseStmt->body.get());
	} else if (auto* whenStmt = dynamic_cast<WhenStatement*>(stmt)) {
		// WaitStatement inside when doesn't need recursion (no nested statements)
		findState(whenStmt->body.get());
	}
}

void ErrorMessagePolicy::handleActorWithoutWait(const std::string& sourceFile, const Actor& actor) {
	if (!disableDiagnostics && !actor.isTestCase) {
		std::cerr << sourceFile << ":" << actor.sourceLine << ": warning: ACTOR " << actor.name
		          << " does not contain a wait() statement\n";
	}
}

} // namespace actorcompiler
