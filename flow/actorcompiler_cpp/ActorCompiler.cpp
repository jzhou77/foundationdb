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
#include <typeinfo>
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
    generateProbes(generateProbes), labelIndex(0) {
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

ActorCompiler::~ActorCompiler() {
	// Clean up dynamically allocated Function objects
	for (auto& pair : functions) {
		delete pair.second;
	}
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
	// Determine full return type (Future<T> or void)
	const std::string fullReturnType =
	    actor.returnType.empty() ? std::string("void") : (std::string("Future<") + actor.returnType + ">");

	// Generate unique class name to avoid collisions
	int classNameSuffix = 0;
	std::string baseClassName = std::string(1, (char)std::toupper(actor.name[0])) + actor.name.substr(1) + "Actor";

	// Add prefix for forward declarations or namespaces to avoid collisions
	std::string classPrefix;
	if (!actor.enclosingClass.empty() && actor.isForwardDeclaration) {
		classPrefix = actor.enclosingClass;
		// Replace :: with _
		for (size_t i = 0; i < classPrefix.length(); ++i) {
			if (classPrefix[i] == ':')
				classPrefix[i] = '_';
		}
		classPrefix += "_";
	} else if (!actor.nameSpace.empty()) {
		classPrefix = actor.nameSpace;
		for (size_t i = 0; i < classPrefix.length(); ++i) {
			if (classPrefix[i] == ':')
				classPrefix[i] = '_';
		}
		classPrefix += "_";
	}

	className = classPrefix + baseClassName;
	// TODO: Check usedClassNames and increment suffix if needed
	if (classNameSuffix > 0) {
		className += std::to_string(classNameSuffix);
	}

	// Build full class name with template actuals
	fullClassName = className + getTemplateActuals();
	stateClassName = className + "State";
	std::string fullStateClassName = stateClassName + getTemplateActuals();

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

	// Discover state variables from actor body
	if (actor.body) {
		findState(actor.body.get());
	}

	// Create the body function and compile the actor body
	Function* body = getFunction("body");
	Context bodyContext = Context::createUnreachable();

	// TODO: Add catch handler for error handling
	// bodyContext.catchHandler = getCatchFunction(body->name);

	// Compile the actor body
	if (actor.body) {
		compile(body, actor.body.get(), bodyContext);
	}

	// Add implicit return if needed
	if (actor.returnType.empty() && !body->endIsUnreachable) {
		body->writeLine("return Void();");
	}

	// Begin namespace if top-level and no explicit namespace
	if (isTopLevel && actor.nameSpace.empty()) {
		writer << "namespace {\n";
	}

	// ===== Write State Class =====
	writer << "// This generated class is to be used only via " << actor.name << "()\n";
	actorcompiler::writeTemplate(writer, actor.templateFormals, actor.sourceLine, lineNumbersEnabled, sourceFile);
	lineNumber(writer, actor.sourceLine);
	writer << "class " << stateClassName << " {\n";
	writer << "public:\n";

	lineNumber(writer, actor.sourceLine);
	writeStateConstructor(writer);
	writeStateDestructor(writer);
	writeFunctions(writer);

	// State variables with types
	for (const auto& varName : stateVariables) {
		auto typeIt = stateVariableTypes.find(varName);
		if (typeIt != stateVariableTypes.end()) {
			writer << "\t" << typeIt->second << " " << varName << ";\n";
		} else {
			// Fallback if type not tracked (shouldn't happen with proper discovery)
			writer << "\t// TODO: " << varName << ";\n";
		}
	}

	writer << "};\n";

	// ===== Write Actor Class =====
	writeActorClass(writer, fullStateClassName, body);

	// End namespace if we started one
	if (isTopLevel && actor.nameSpace.empty()) {
		writer << "} // namespace\n";
	}

	// ===== Write Actor Wrapper Function =====
	writeActorFunction(writer, fullReturnType);

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
		stateVariableTypes[stateDecl->decl.name] = stateDecl->decl.type;
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

Function* ActorCompiler::getFunction(const std::string& label) {
	// Check if function already exists
	auto it = functions.find(label);
	if (it != functions.end()) {
		return it->second;
	}

	// Create new function and register it
	Function* func = new Function();
	// Give the function a public name matching the label by default
	func->name = label;
	functions[label] = func;
	return func;
}

std::string ActorCompiler::generateLabel() {
	return "cont" + std::to_string(++labelIndex);
}

void ActorCompiler::compile(Function* func, Statement* stmt, const Context& ctx) {
	if (!stmt)
		return;

	// Dispatch to appropriate compilation method based on statement type
	if (auto* plainCode = dynamic_cast<PlainOldCodeStatement*>(stmt)) {
		compileStatement(func, plainCode, ctx);
	} else if (auto* stateDecl = dynamic_cast<StateDeclarationStatement*>(stmt)) {
		compileStatement(func, stateDecl, ctx);
	} else if (auto* returnStmt = dynamic_cast<ReturnStatement*>(stmt)) {
		compileStatement(func, returnStmt, ctx);
	} else if (auto* breakStmt = dynamic_cast<BreakStatement*>(stmt)) {
		compileStatement(func, breakStmt, ctx);
	} else if (auto* continueStmt = dynamic_cast<ContinueStatement*>(stmt)) {
		compileStatement(func, continueStmt, ctx);
	} else if (auto* codeBlock = dynamic_cast<CodeBlock*>(stmt)) {
		compileStatement(func, codeBlock, ctx);
	} else if (auto* waitStmt = dynamic_cast<WaitStatement*>(stmt)) {
		compileStatement(func, waitStmt, ctx);
	} else if (auto* ifStmt = dynamic_cast<IfStatement*>(stmt)) {
		compileStatement(func, ifStmt, ctx);
	} else if (auto* whileStmt = dynamic_cast<WhileStatement*>(stmt)) {
		compileStatement(func, whileStmt, ctx);
	} else if (auto* forStmt = dynamic_cast<ForStatement*>(stmt)) {
		compileStatement(func, forStmt, ctx);
	} else if (auto* loopStmt = dynamic_cast<LoopStatement*>(stmt)) {
		compileStatement(func, loopStmt, ctx);
	} else if (auto* rangeForStmt = dynamic_cast<RangeForStatement*>(stmt)) {
		compileStatement(func, rangeForStmt, ctx);
	} else if (auto* chooseStmt = dynamic_cast<ChooseStatement*>(stmt)) {
		compileStatement(func, chooseStmt, ctx);
	} else if (auto* tryStmt = dynamic_cast<TryStatement*>(stmt)) {
		compileStatement(func, tryStmt, ctx);
	} else if (auto* throwStmt = dynamic_cast<ThrowStatement*>(stmt)) {
		compileStatement(func, throwStmt, ctx);
	} else {
		// Unhandled statement type - will be implemented in later steps
		func->writeLine("// TODO: Compile " + std::string(typeid(*stmt).name()));
	}
}

void ActorCompiler::compileStatement(Function* func, PlainOldCodeStatement* stmt, const Context& ctx) {
	// Plain old code just passes through
	func->writeLine(stmt->code);
}

void ActorCompiler::compileStatement(Function* func, StateDeclarationStatement* stmt, const Context& ctx) {
	// State declarations are handled in the class definition, not in the function body
	// Generate initialization code if there's an initializer
	if (!stmt->decl.initializer.empty()) {
		if (stmt->decl.initializerConstructorSyntax) {
			func->writeLine(stmt->decl.name + " = " + stmt->decl.type + "(" + stmt->decl.initializer + ");");
		} else {
			func->writeLine(stmt->decl.name + " = " + stmt->decl.initializer + ";");
		}
	}
}

void ActorCompiler::compileStatement(Function* func, ReturnStatement* stmt, const Context& ctx) {
	// Generate return statement
	// For now, just emit the return directly - full actor return logic will be added later
	if (stmt->expression.empty()) {
		func->writeLine("return Void();");
	} else {
		func->writeLine("return " + stmt->expression + ";");
	}
}

void ActorCompiler::compileStatement(Function* func, BreakStatement* stmt, const Context& ctx) {
	// Generate goto to break target
	if (ctx.breakLabel.empty()) {
		throw Error(stmt->firstSourceLine, "break statement outside of loop");
	}
	func->writeLine("goto " + ctx.breakLabel + ";");
}

void ActorCompiler::compileStatement(Function* func, ContinueStatement* stmt, const Context& ctx) {
	// Generate goto to continue target
	if (ctx.continueLabel.empty()) {
		throw Error(stmt->firstSourceLine, "continue statement outside of loop");
	}
	func->writeLine("goto " + ctx.continueLabel + ";");
}

void ActorCompiler::compileStatement(Function* func, CodeBlock* stmt, const Context& ctx) {
	// Compile each statement in the block sequentially
	for (auto& s : stmt->statements) {
		compile(func, s.get(), ctx);
	}
}

void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
	// Generate a continuation label for code after the wait and a unique callback index
	std::string contLabel = generateLabel();
	Function* contFunc = getFunction(contLabel);
	int cbIndex = nextCallbackIndex();

	// Track callback for later class generation
	CallbackInfo cb;
	cb.type = stmt->result.type;
	cb.index = cbIndex;
	cb.continueLabel = contLabel;
	cb.resultName = stmt->result.name;
	cb.resultIsState = stmt->resultIsState;
	cb.errorHandler = ctx.catchHandler;
	cb.errorVarName = ctx.errorVarName;
	callbacks.push_back(cb);

	// Use a unique future variable per wait to avoid name collisions
	std::string futureVar =
	    (cbIndex == 0) ? std::string("__when_expr") : std::string("__when_expr_") + std::to_string(cbIndex);

	// Emit the wait expression assignment to a StrictFuture
	func->writeLine("StrictFuture<" + stmt->result.type + "> " + futureVar + " = " + stmt->futureExpression + ";");

	// Check if the future is already ready (fast path optimization)
	func->writeLine("if (" + futureVar + ".isReady()) {");
	func->indent(+1);

	// Check for error in ready future
	func->writeLine("if (" + futureVar + ".isError()) {");
	func->indent(+1);
	if (!ctx.catchHandler.empty()) {
		// Jump to error handler if one is set
		func->writeLine(ctx.errorVarName + " = " + futureVar + ".getError();");
		func->writeLine("goto " + ctx.catchHandler + ";");
	} else {
		// Re-throw if no handler
		func->writeLine("throw " + futureVar + ".getError();");
	}
	func->indent(-1);
	func->writeLine("} else {");
	func->indent(+1);

	// Extract value from ready future
	if (stmt->resultIsState) {
		// State variable - assign directly to member
		func->writeLine(stmt->result.name + " = " + futureVar + ".get();");
		func->writeLine("goto " + contLabel + ";");
	} else {
		// Local variable - pass as parameter to continuation (not yet supported)
		func->writeLine("// TODO: Non-state wait result not fully implemented");
		func->writeLine(stmt->result.type + " " + stmt->result.name + " = " + futureVar + ".get();");
		func->writeLine("goto " + contLabel + ";");
	}
	func->indent(-1);
	func->writeLine("}");
	func->indent(-1);

	func->writeLine("} else {");
	func->indent(+1);
	// Future not ready - set up async callback and suspend
	{
		std::string actorBase =
		    actor.returnType.empty() ? std::string("Actor<void>") : (std::string("Actor<") + actor.returnType + ">");
		func->writeLine("static_cast<" + actorBase + "*>(this)->actor_wait_state = " + std::to_string(cbIndex + 1) +
		                ";");
		func->writeLine(futureVar + ".addCallbackAndClear(static_cast<ActorCallback< " + className + ", " +
		                std::to_string(cbIndex) + ", " + stmt->result.type + " >*>(this));");
		func->writeLine("return 0; // Suspend until callback fires");
	}
	func->indent(-1);
	func->writeLine("}");

	// Emit continuation label
	func->writeLine("");
	// Resume point for this wait
	func->writeLine("resume_" + std::to_string(cbIndex + 1) + ":");
	{
		std::string actorBase =
		    actor.returnType.empty() ? std::string("Actor<void>") : (std::string("Actor<") + actor.returnType + ">");
		func->writeLine("static_cast<" + actorBase + "*>(this)->actor_wait_state = 0;");
		// Check if callback set an error (error callback stores error in errorVarName)
		if (!ctx.catchHandler.empty() && !ctx.errorVarName.empty()) {
			func->writeLine("if (" + ctx.errorVarName + ".code() != invalid_error_code) {");
			func->indent(+1);
			func->writeLine("goto " + ctx.catchHandler + ";");
			func->indent(-1);
			func->writeLine("}");
		}
	}
	func->writeLine(contLabel + ":");
	// The continuation function will be filled in by subsequent compileStatement calls
}

void ActorCompiler::compileStatement(Function* func, IfStatement* stmt, const Context& ctx) {
	// Simplified if/else compilation - doesn't handle continuations yet
	// Full implementation would check if body contains waits and create continuation functions

	func->writeLine("if " + std::string(stmt->constexpr_ ? "constexpr " : "") + "(" + stmt->expression + ")");
	func->writeLine("{");
	func->indent(+1);

	// Compile if body
	compile(func, stmt->ifBody.get(), ctx);

	func->indent(-1);
	func->writeLine("}");

	// Compile else body if present
	if (stmt->elseBody) {
		func->writeLine("else");
		func->writeLine("{");
		func->indent(+1);

		compile(func, stmt->elseBody.get(), ctx);

		func->indent(-1);
		func->writeLine("}");
	}
}

void ActorCompiler::compileStatement(Function* func, WhileStatement* stmt, const Context& ctx) {
	// Compile while (x) { y } as for(;x;) { y }
	// Create an equivalent ForStatement
	ForStatement equivalent;
	equivalent.condExpression = stmt->expression;
	equivalent.body = std::move(stmt->body);
	equivalent.firstSourceLine = stmt->firstSourceLine;

	compileStatement(func, &equivalent, ctx);

	// Move the body back (since we don't want to invalidate the original)
	stmt->body = std::move(equivalent.body);
}

void ActorCompiler::compileStatement(Function* func, LoopStatement* stmt, const Context& ctx) {
	// Compile loop { body } as for(;;;) { body }
	ForStatement equivalent;
	equivalent.body = std::move(stmt->body);
	equivalent.firstSourceLine = stmt->firstSourceLine;

	compileStatement(func, &equivalent, ctx);

	// Move the body back
	stmt->body = std::move(equivalent.body);
}

void ActorCompiler::compileStatement(Function* func, ForStatement* stmt, const Context& ctx) {
	// Simplified for loop compilation
	// Full implementation would check for waits and create loop continuation functions

	// Emit init expression
	if (!stmt->initExpression.empty()) {
		func->writeLine(stmt->initExpression + ";");
	}

	// Generate loop head label
	std::string loopHeadLabel = generateLabel();
	func->writeLine("");
	func->writeLine(loopHeadLabel + ":");

	// Emit condition check (if present)
	std::string breakLabel = generateLabel();
	std::string continueLabel = generateLabel();

	if (!stmt->condExpression.empty()) {
		func->writeLine("if (!(" + stmt->condExpression + "))");
		func->indent(+1);
		func->writeLine("goto " + breakLabel + ";");
		func->indent(-1);
	}

	// Compile loop body with loop context
	Context loopCtx = ctx.loopContext(breakLabel, continueLabel);
	func->writeLine("{");
	func->indent(+1);
	compile(func, stmt->body.get(), loopCtx);
	func->indent(-1);
	func->writeLine("}");

	// Emit continue label and next expression
	func->writeLine("");
	func->writeLine(continueLabel + ":");
	if (!stmt->nextExpression.empty()) {
		func->writeLine(stmt->nextExpression + ";");
	}

	// Jump back to loop head
	func->writeLine("goto " + loopHeadLabel + ";");

	// Emit break label
	func->writeLine("");
	func->writeLine(breakLabel + ":");
}

void ActorCompiler::compileStatement(Function* func, RangeForStatement* stmt, const Context& ctx) {
	// Simplified range-for compilation - emits native C++ range-for
	// Full implementation would handle waits by converting to iterator-based for loop

	func->writeLine("for (" + stmt->rangeDecl + " : " + stmt->rangeExpression + ")");
	func->writeLine("{");
	func->indent(+1);

	compile(func, stmt->body.get(), ctx);

	func->indent(-1);
	func->writeLine("}");
}

void ActorCompiler::compileStatement(Function* func, ChooseStatement* stmt, const Context& ctx) {
	// Simplified choose/when implementation
	// Full implementation would generate callback functions and wire them to futures

	// The choose body should be a CodeBlock containing only WhenStatements
	CodeBlock* codeBlock = dynamic_cast<CodeBlock*>(stmt->body.get());
	if (!codeBlock) {
		throw Error(stmt->firstSourceLine, "'choose' must be followed by a compound statement");
	}

	// Collect all when statements
	std::vector<WhenStatement*> whenStmts;
	for (auto& s : codeBlock->statements) {
		WhenStatement* whenStmt = dynamic_cast<WhenStatement*>(s.get());
		if (!whenStmt) {
			throw Error(s->firstSourceLine, "only 'when' statements are valid in a 'choose' block");
		}
		whenStmts.push_back(whenStmt);
	}

	if (whenStmts.empty()) {
		throw Error(stmt->firstSourceLine, "'choose' block must contain at least one 'when' statement");
	}

	func->writeLine("// BEGIN choose block (simplified)");
	func->writeLine("{");
	func->indent(+1);

	// For each when clause, evaluate the future expression
	for (size_t i = 0; i < whenStmts.size(); ++i) {
		WhenStatement* when = whenStmts[i];
		WaitStatement* wait = when->wait.get();

		std::string futureVar = "__when_expr_" + std::to_string(i);
		func->writeLine("StrictFuture<" + wait->result.type + "> " + futureVar + " = " + wait->futureExpression + ";");
	}

	// Check if any future is already ready (fast path)
	for (size_t i = 0; i < whenStmts.size(); ++i) {
		WhenStatement* when = whenStmts[i];
		WaitStatement* wait = when->wait.get();
		std::string futureVar = "__when_expr_" + std::to_string(i);

		func->writeLine("if (" + futureVar + ".isReady()) {");
		func->indent(+1);

		// Check for error
		func->writeLine("if (" + futureVar + ".isError()) {");
		func->indent(+1);
		if (!ctx.catchHandler.empty()) {
			func->writeLine(ctx.errorVarName + " = " + futureVar + ".getError();");
			func->writeLine("goto " + ctx.catchHandler + ";");
		} else {
			func->writeLine("throw " + futureVar + ".getError();");
		}
		func->indent(-1);
		func->writeLine("} else {");
		func->indent(+1);

		// Extract value and compile body
		if (wait->resultIsState) {
			func->writeLine(wait->result.name + " = " + futureVar + ".get();");
		} else {
			func->writeLine(wait->result.type + " " + wait->result.name + " = " + futureVar + ".get();");
		}

		// Compile the when body
		if (when->body) {
			compile(func, when->body.get(), ctx);
		}

		// Jump to end of choose block
		std::string endLabel = generateLabel();
		func->writeLine("goto " + endLabel + "; // end of when clause " + std::to_string(i));
		func->indent(-1);
		func->writeLine("}");
		func->indent(-1);
		func->writeLine("}");
	}

	// If no future is ready, set up callbacks (simplified with TODO)
	func->writeLine("// TODO: Set up ActorCallback for all futures");
	func->writeLine("// actor_wait_state = ...;");
	for (size_t i = 0; i < whenStmts.size(); ++i) {
		std::string futureVar = "__when_expr_" + std::to_string(i);
		func->writeLine("// " + futureVar + ".addCallbackAndClear(static_cast<ActorCallback<...>*>(this));");
	}
	func->writeLine("return; // Suspend until one callback fires");

	func->indent(-1);
	func->writeLine("}");
	func->writeLine("// END choose block");
}

void ActorCompiler::compileStatement(Function* func, TryStatement* stmt, const Context& ctx) {
	// Simplified try/catch implementation
	// Full implementation would generate catch handler functions

	// Flow actors only support a single catch clause
	if (stmt->catches.size() != 1) {
		throw Error(stmt->firstSourceLine, "try statement must have exactly one catch clause");
	}

	const auto& catchClause = stmt->catches[0];

	// Parse the catch expression to extract error variable name
	std::string errorVarName = "__current_error";
	std::string catchExpr = catchClause.expression;

	// Remove spaces
	catchExpr.erase(std::remove(catchExpr.begin(), catchExpr.end(), ' '), catchExpr.end());

	if (catchExpr != "...") {
		// Expected format: "Error&varName"
		if (catchExpr.find("Error&") == 0) {
			errorVarName = catchExpr.substr(6); // Skip "Error&"
		} else {
			throw Error(catchClause.firstSourceLine, "Only type 'Error&' or '...' may be caught in an actor function");
		}
	}

	// Generate catch handler label
	std::string catchLabel = generateLabel();

	func->writeLine("// BEGIN try block");
	func->writeLine("try {");
	func->indent(+1);

	// Compile try body with catch context
	Context tryCtx = ctx.withCatch(errorVarName, "__error_code", catchLabel);
	compile(func, stmt->tryBody.get(), tryCtx);

	func->indent(-1);
	func->writeLine("}");
	func->writeLine("catch (Error& " + errorVarName + ") {");
	func->indent(+1);
	func->writeLine("goto " + catchLabel + ";");
	func->indent(-1);
	func->writeLine("}");
	func->writeLine("catch (...) {");
	func->indent(+1);
	func->writeLine(errorVarName + " = unknown_error();");
	func->writeLine("goto " + catchLabel + ";");
	func->indent(-1);
	func->writeLine("}");

	// Emit catch handler label and compile catch body
	func->writeLine("");
	func->writeLine(catchLabel + ":");
	func->writeLine("{");
	func->indent(+1);
	compile(func, catchClause.body.get(), ctx);
	func->indent(-1);
	func->writeLine("}");
	func->writeLine("// END try block");
}

void ActorCompiler::compileStatement(Function* func, ThrowStatement* stmt, const Context& ctx) {
	// Throw statement - re-throws or throws new error

	if (stmt->expression.empty()) {
		// Re-throw current exception
		if (!ctx.catchHandler.empty() && !ctx.errorVarName.empty()) {
			func->writeLine("goto " + ctx.catchHandler + "; // re-throw");
		} else {
			throw Error(stmt->firstSourceLine, "throw statement with no expression has no current exception in scope");
		}
	} else {
		// Throw new exception
		if (!ctx.catchHandler.empty()) {
			// If we have a catch handler, store error and goto it
			func->writeLine(ctx.errorVarName + " = " + stmt->expression + ";");
			func->writeLine("goto " + ctx.catchHandler + "; // throw");
		} else {
			// No catch handler, use C++ throw
			func->writeLine("throw " + stmt->expression + ";");
		}
	}
}

// ========== Code Generation Main Methods ==========

void ActorCompiler::writeActorFunction(std::ostream& writer, const std::string& fullReturnType) {
	writeTemplate(writer);
	lineNumber(writer, actor.sourceLine);

	// Write attributes
	for (const auto& attr : actor.attributes) {
		writer << attr << " ";
	}

	// Write static keyword if applicable
	if (actor.isStatic) {
		writer << "static ";
	}

	// Write function signature
	std::string nameSpace = actor.nameSpace.empty() ? "" : actor.nameSpace + "::";
	writer << fullReturnType << " " << nameSpace << actor.name << "( " << join(parameterList(), ", ") << " ) {\n";

	lineNumber(writer, actor.sourceLine);

	// Construct the actor instance
	std::string newActor = "new " + fullClassName + "(";
	std::vector<std::string> paramNames;
	for (const auto& p : actor.parameters) {
		paramNames.push_back(p.name);
	}
	newActor += join(paramNames, ", ") + ")";

	if (generateProbes) {
		writer << "\t// PROBE_ENTER(\"" << actor.name << "\")\n";
	}

	// Create the actor instance in a temporary to allow exit probe before return
	writer << "\tauto __actor_ptr = " << newActor << ";\n";

	if (generateProbes) {
		writer << "\t// PROBE_EXIT(\"" << actor.name << "\")\n";
	}

	// Return the actor or just construct it
	if (!actor.returnType.empty()) {
		writer << "\treturn Future<" << actor.returnType << ">(__actor_ptr);\n";
	} else {
		// Actor constructed and immediately discarded; side-effects occur via constructor
		(void)0; // keep consistent structure
	}

	writer << "}\n";
}

void ActorCompiler::writeActorClass(std::ostream& writer, const std::string& fullStateClassName, Function* body) {
	// Comment indicating generated class
	writer << "// This generated class is to be used only via " << actor.name << "()\n";

	writeTemplate(writer);
	lineNumber(writer, actor.sourceLine);

	// Generate callback base classes from callbacks vector
	std::string callbackBases;
	if (!callbacks.empty()) {
		for (size_t i = 0; i < callbacks.size(); ++i) {
			const auto& cb = callbacks[i];
			callbackBases += std::string("public ActorCallback< ") + className + ", " + std::to_string(cb.index) +
			                 ", " + cb.type + " >, ";
		}
	}

	// Class declaration with inheritance
	std::string returnType = actor.returnType.empty() ? "void" : actor.returnType;
	writer << "class " << className << " final : public Actor<" << returnType << ">, " << callbackBases
	       << "public FastAllocated<" << fullClassName << ">, public " << fullStateClassName << " {\n";
	writer << "public:\n";
	writer << "\tusing FastAllocated<" << fullClassName << ">::operator new;\n";
	writer << "\tusing FastAllocated<" << fullClassName << ">::operator delete;\n";

	// Generate actor identifier
	auto actorIdentifierKey = sourceFile + ":" + actor.name;
	auto actorIdentifier = getUidFromString(actorIdentifierKey);
	uidObjects[actorIdentifier] = actorIdentifierKey;

	writer << "\tstatic constexpr ActorIdentifier __actorIdentifier = UID(" << actorIdentifier.first << "UL, "
	       << actorIdentifier.second << "UL);\n";
	writer << "\tActiveActorHelper activeActorHelper;\n";

	// Destroy method
	writer << "#pragma clang diagnostic push\n";
	writer << "#pragma clang diagnostic ignored \"-Wdelete-non-virtual-dtor\"\n";
	if (!actor.returnType.empty()) {
		writer << "\tvoid destroy() override {\n";
		writer << "\t\tactiveActorHelper.~ActiveActorHelper();\n";
		writer << "\t\tstatic_cast<Actor<" << actor.returnType << ">*>(this)->~Actor();\n";
		writer << "\t\toperator delete(this);\n";
		writer << "\t}\n";
	} else {
		writer << "\tvoid destroy() {\n";
		writer << "\t\tactiveActorHelper.~ActiveActorHelper();\n";
		writer << "\t\tstatic_cast<Actor<void>*>(this)->~Actor();\n";
		writer << "\t\toperator delete(this);\n";
		writer << "\t}\n";
	}
	writer << "#pragma clang diagnostic pop\n";

	// Friend declarations for callback base classes (not strictly necessary, but keeps options open)
	for (const auto& cb : callbacks) {
		(void)cb; // suppress unused warning if empty
	}

	lineNumber(writer, actor.sourceLine);

	// Constructor - simplified for now, calls the body continuation to start execution
	writeTemplate(writer);
	writer << "\t" << className << "(" << join(parameterList(), ", ") << ") : " << fullStateClassName << "("
	       << join(
	              [&]() {
		              std::vector<std::string> names;
		              for (const auto& p : actor.parameters) {
			              names.push_back(p.name);
		              }
		              return names;
	              }(),
	              ", ")
	       << ") {\n";
	writer << "\t\t// TODO: Initialize actor state\n";
	writer << "\t\t// Kick off actor by invoking the first continuation\n";
	writer << "\t\tthis->" << (body && !body->name.empty() ? body->name : std::string("body")) << "(0);\n";
	writer << "\t}\n";

	// Cancel function - invokes error callback with actor_cancelled()
	writer << "\tvoid cancel() override {\n";
	if (generateProbes) {
		writer << "\t\t// PROBE_CANCEL(\"" << actor.name << "\")\n";
	}
	if (!callbacks.empty()) {
		writer << "\t\tauto wait_state = static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state;\n";
		writer << "\t\tstatic_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state = -1;\n";
		writer << "\t\tswitch (wait_state) {\n";
		// Group callbacks by index (in C# they group by CallbackGroup, but we use index)
		for (size_t i = 0; i < callbacks.size(); ++i) {
			writer << "\t\tcase " << (i + 1) << ": this->a_callback_error(static_cast<ActorCallback<" << className
			       << ", " << i << ", " << callbacks[i].type << ">*>(nullptr), actor_cancelled()); break;\n";
		}
		writer << "\t\t}\n";
	}
	writer << "\t}\n";

	// Emit callback handlers for each registered callback index
	for (const auto& cb : callbacks) {
		// a_callback_fire - callback fires when wait completes successfully
		writer << "\tvoid a_callback_fire(ActorCallback< " << className << ", " << cb.index << ", " << cb.type
		       << " >*, " << cb.type << " const& value) {\n";
		writer << "\t\tfreeAfter(static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this));\n";
		writer << "\t\tint oldstate = static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state;\n";
		writer << "\t\tstatic_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state = 0;\n";
		if (cb.resultIsState && !cb.resultName.empty()) {
			writer << "\t\tthis->" << cb.resultName << " = value;\n";
		} else {
			writer << "\t\t// NOTE: non-state wait result variable '" << cb.resultName
			       << "' not yet supported in callback\n";
		}
		writer << "\t\tthis->body(0);\n";
		writer << "\t}\n";

		// a_callback_error - callback fires when wait completes with error
		writer << "\tvoid a_callback_error(ActorCallback< " << className << ", " << cb.index << ", " << cb.type
		       << " >*, Error err) {\n";
		writer << "\t\tfreeAfter(static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this));\n";
		writer << "\t\tint oldstate = static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state;\n";
		writer << "\t\tstatic_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
		       << ">*>(this)->actor_wait_state = 0;\n";

		// If we have an error handler, invoke it; otherwise throw
		if (!cb.errorHandler.empty()) {
			// Store error in the error variable and goto the handler
			writer << "\t\tthis->" << cb.errorVarName << " = err;\n";
			writer << "\t\tthis->body(0); // Will goto error handler at next resume point\n";
		} else {
			// No error handler, throw the error
			writer << "\t\tdelete static_cast<Actor<" << (actor.returnType.empty() ? "void" : actor.returnType)
			       << ">*>(this);\n";
			writer << "\t\tthrow err;\n";
		}
		writer << "\t}\n";
	}

	writer << "};\n";
}

void ActorCompiler::writeStateConstructor(std::ostream& writer) {
	writer << "\t" << stateClassName << "(" << join(parameterList(), ", ") << ")";

	// Member initializers
	bool firstInitializer = true;
	for (const auto& varName : stateVariables) {
		// Find the corresponding state variable in actor.parameters or locals
		// For now, just initialize actor parameters
		for (const auto& param : actor.parameters) {
			if (param.name == varName) {
				if (firstInitializer) {
					writer << "\n\t  : ";
					firstInitializer = false;
				} else {
					writer << ",\n\t    ";
				}
				writer << varName << "(" << varName << ")";
			}
		}
	}

	writer << " {\n";

	// Probe hook if generateProbes is true
	if (generateProbes) {
		writer << "\t\t// PROBE_CREATE(\"" << actor.name << "\")\n";
	}

	writer << "\t}\n";
}

void ActorCompiler::writeStateDestructor(std::ostream& writer) {
	writer << "\t~" << stateClassName << "() {\n";

	// Probe hook if generateProbes is true
	if (generateProbes) {
		writer << "\t\t// PROBE_DESTROY(\"" << actor.name << "\")\n";
	}

	writer << "\t}\n";
}

void ActorCompiler::writeFunctions(std::ostream& writer) {
	for (const auto& pair : functions) {
		Function* func = pair.second;
		if (func->getBodyText().length() > 0) {
			writeFunction(writer, func);
		}

		// TODO: Handle function overloads if present
	}
}

void ActorCompiler::writeFunction(std::ostream& writer, Function* func) {
	// Function signature with proper formal parameters
	std::string returnTypeStr = func->returnType.empty() ? "int" : func->returnType + " ";
	writer << "\t" << returnTypeStr << func->name << "(";

	// Formal parameters: loopDepth for continuation tracking
	if (!func->formalParameters.empty()) {
		writer << join(func->formalParameters, ", ");
	} else {
		writer << "int loopDepth";
	}

	writer << ")";

	// Function specifiers
	if (!func->specifiers.empty()) {
		writer << " " << func->specifiers;
	}

	writer << " {\n";

	// Resume switch: if resuming from a wait, jump to the appropriate resume label
	if (func->name == "body" && !callbacks.empty()) {
		std::string actorBase =
		    actor.returnType.empty() ? std::string("Actor<void>") : (std::string("Actor<") + actor.returnType + ">");
		writer << "\t\tif (static_cast<" << actorBase << "*>(this)->actor_wait_state > 0) {\n";
		writer << "\t\t\tswitch (static_cast<" << actorBase << "*>(this)->actor_wait_state) {\n";
		for (size_t i = 0; i < callbacks.size(); ++i) {
			writer << "\t\t\t\tcase " << (i + 1) << ": goto resume_" << (i + 1) << ";\n";
		}
		writer << "\t\t\t}\n";
		writer << "\t\t}\n\n";
	}

	// Function body
	std::string bodyText = func->getBodyText();
	if (!bodyText.empty()) {
		// Add indentation to each line
		size_t pos = 0;
		while (pos < bodyText.length()) {
			size_t endPos = bodyText.find('\n', pos);
			if (endPos == std::string::npos) {
				endPos = bodyText.length();
			}

			std::string line = bodyText.substr(pos, endPos - pos);
			if (!line.empty()) {
				writer << "\t" << line << "\n";
			} else {
				writer << "\n";
			}

			pos = endPos + 1;
		}
	}

	// Return statement if not unreachable
	if (!func->endIsUnreachable) {
		writer << "\t\treturn loopDepth;\n";
	}

	writer << "\t}\n";
}

// ========== Code Generation Helper Methods ==========

void ActorCompiler::writeTemplate(std::ostream& writer) {
	// Delegate to static helper in actorcompiler namespace
	actorcompiler::writeTemplate(writer, actor.templateFormals, actor.sourceLine, lineNumbersEnabled, sourceFile);
}

void ActorCompiler::lineNumber(std::ostream& writer, int line) {
	if (lineNumbersEnabled && line >= 0) {
		writer << "#line " << line << " \"" << sourceFile << "\"\n";
	}
}

std::vector<std::string> ActorCompiler::parameterList() const {
	std::vector<std::string> params;
	for (const auto& p : actor.parameters) {
		std::string param = p.type + " const& " + p.name;
		if (!p.initializer.empty()) {
			param += " = " + p.initializer;
		}
		params.push_back(param);
	}
	return params;
}

std::string ActorCompiler::getTemplateActuals() const {
	if (actor.templateFormals.empty()) {
		return "";
	}

	std::string result = "<";
	for (size_t i = 0; i < actor.templateFormals.size(); ++i) {
		if (i > 0) {
			result += ", ";
		}
		result += actor.templateFormals[i].name;
	}
	result += ">";
	return result;
}

std::pair<uint64_t, uint64_t> ActorCompiler::getUidFromString(const std::string& str) {
	// Use OpenSSL SHA256 to generate deterministic UID
	unsigned char hash[SHA256_DIGEST_LENGTH];
	SHA256(reinterpret_cast<const unsigned char*>(str.data()), str.size(), hash);

	// Convert first 16 bytes to two uint64_t values
	uint64_t uid1 = 0, uid2 = 0;
	for (int i = 0; i < 8; ++i) {
		uid1 = (uid1 << 8) | hash[i];
		uid2 = (uid2 << 8) | hash[i + 8];
	}

	return { uid1, uid2 };
}

// ========== Error Handling ==========

void ErrorMessagePolicy::handleActorWithoutWait(const std::string& sourceFile, const Actor& actor) {
	if (!disableDiagnostics && !actor.isTestCase) {
		std::cerr << sourceFile << ":" << actor.sourceLine << ": warning: ACTOR " << actor.name
		          << " does not contain a wait() statement\n";
	}
}

} // namespace actorcompiler
