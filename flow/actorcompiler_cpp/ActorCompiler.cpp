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

	// Add actor parameters as state variables (they need to be accessible throughout actor lifetime)
	for (const auto& param : actor.parameters) {
		stateVariables.insert(param.name);
		stateVariableTypes[param.name] = param.type;
	}

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
	Function* body = getFunction("a_body1");
	body->returnType = "int";
	body->formalParameters = {"int loopDepth=0"};
	Context bodyContext = Context::createUnreachable();

	// Compile the actor body
	if (actor.body) {
		compile(body, actor.body.get(), bodyContext);
	}

	// Add implicit return if needed (for void actors with no explicit return)
	if (actor.returnType.empty() && !body->endIsUnreachable) {
		// Use same SAV pattern as explicit return
		body->writeLine("if (!static_cast<" + className + "*>(this)->SAV<Void>::futures) { " +
		                "this->~" + stateClassName + "(); static_cast<" + className +
		                "*>(this)->destroy(); return 0; }");
		body->writeLine("this->~" + stateClassName + "();");
		body->writeLine("static_cast<" + className + "*>(this)->finishSendAndDelPromiseRef();");
		body->writeLine("return 0;");
	}

	// Generate catch handler for body function
	Function* catchFunc = getFunction("a_body1Catch1");
	catchFunc->returnType = "int";
	catchFunc->formalParameters = {"Error error", "int loopDepth=0"};
	catchFunc->endIsUnreachable = true; // We include return in body, don't add another
	catchFunc->writeLine("this->~" + stateClassName + "();");
	catchFunc->writeLine("static_cast<" + className + "*>(this)->sendErrorAndDelPromiseRef(error);");
	catchFunc->writeLine("loopDepth = 0;");
	catchFunc->writeLine("");  // Blank line before return
	catchFunc->writeLine("return loopDepth;");

	// Begin namespace if top-level and no explicit namespace
	if (isTopLevel && actor.nameSpace.empty()) {
		writer << "namespace {\n";
	}

	// ===== Write State Class =====
	writer << "// This generated class is to be used only via " << actor.name << "()\n";
	actorcompiler::writeTemplate(writer, actor.templateFormals, actor.sourceLine, lineNumbersEnabled, sourceFile);
	lineNumber(writer, actor.sourceLine);
	// Add template parameter for the actor type
	writer << "template <class " << className << ">\n";
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

bool ActorCompiler::containsWait(Statement* stmt) {
	if (!stmt)
		return false;

	// Check if this is a wait statement
	if (dynamic_cast<WaitStatement*>(stmt)) {
		return true;
	}

	// Recursively check compound statements
	if (auto* codeBlock = dynamic_cast<CodeBlock*>(stmt)) {
		for (auto& s : codeBlock->statements) {
			if (containsWait(s.get()))
				return true;
		}
	} else if (auto* whileStmt = dynamic_cast<WhileStatement*>(stmt)) {
		return containsWait(whileStmt->body.get());
	} else if (auto* forStmt = dynamic_cast<ForStatement*>(stmt)) {
		return containsWait(forStmt->body.get());
	} else if (auto* rangeForStmt = dynamic_cast<RangeForStatement*>(stmt)) {
		return containsWait(rangeForStmt->body.get());
	} else if (auto* loopStmt = dynamic_cast<LoopStatement*>(stmt)) {
		return containsWait(loopStmt->body.get());
	} else if (auto* ifStmt = dynamic_cast<IfStatement*>(stmt)) {
		return containsWait(ifStmt->ifBody.get()) || containsWait(ifStmt->elseBody.get());
	} else if (auto* tryStmt = dynamic_cast<TryStatement*>(stmt)) {
		if (containsWait(tryStmt->tryBody.get()))
			return true;
		for (auto& catchClause : tryStmt->catches) {
			if (containsWait(catchClause.body.get()))
				return true;
		}
	} else if (auto* chooseStmt = dynamic_cast<ChooseStatement*>(stmt)) {
		return containsWait(chooseStmt->body.get());
	} else if (auto* whenStmt = dynamic_cast<WhenStatement*>(stmt)) {
		return containsWait(whenStmt->body.get()) || whenStmt->wait != nullptr;
	}

	return false;
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
	// Generate return statement using SAV (Send And Value) pattern
	std::string returnType = actor.returnType.empty() ? "Void" : actor.returnType;
	std::string expression = stmt->expression.empty() ? "Void()" : stmt->expression;

	// Check if anyone is waiting for the result
	func->writeLine("if (!static_cast<" + className + "*>(this)->SAV<" + returnType + ">::futures) { (" +
	                "void)(" + expression + "); this->~" + stateClassName + "(); static_cast<" + className +
	                "*>(this)->destroy(); return 0; }");

	// Place return value in SAV using placement new
	func->writeLine("new (&static_cast<" + className + "*>(this)->SAV< " + returnType + " >::value()) " +
	                returnType + "(std::move(" + expression + ")); // state_var_RVO");

	// Cleanup and finish promise
	func->writeLine("this->~" + stateClassName + "();");
	func->writeLine("static_cast<" + className + "*>(this)->finishSendAndDelPromiseRef();");
	func->writeLine("return 0;");
}

void ActorCompiler::compileStatement(Function* func, BreakStatement* stmt, const Context& ctx) {
	// Generate goto to break target
	if (ctx.breakLabel.empty()) {
		throw Error(stmt->firstSourceLine, "break statement outside of loop");
	}

	// If inside a loop with continuation methods, use return to break handler
	if (ctx.loopDepth > 0) {
		func->writeLine("return " + ctx.breakLabel + "(loopDepth==0?0:loopDepth-1); // break");
	} else {
		func->writeLine("goto " + ctx.breakLabel + ";");
	}
}

void ActorCompiler::compileStatement(Function* func, ContinueStatement* stmt, const Context& ctx) {
	// Generate goto to continue target
	if (ctx.continueLabel.empty()) {
		throw Error(stmt->firstSourceLine, "continue statement outside of loop");
	}

	// If inside a loop with continuation methods, use return to loop head
	if (ctx.loopDepth > 0) {
		func->writeLine("if (loopDepth == 0) return " + ctx.continueLabel + "(0);");
		func->writeLine("");
		func->writeLine("return loopDepth;");
	} else {
		func->writeLine("goto " + ctx.continueLabel + ";");
	}
}

void ActorCompiler::compileStatement(Function* func, CodeBlock* stmt, const Context& ctx) {
	// Compile each statement in the block sequentially
	// After a wait statement, switch to continuation function
	Function* currentFunc = func;
	for (auto& s : stmt->statements) {
		compile(currentFunc, s.get(), ctx);
		// Check if we should switch to a continuation function
		if (pendingContinuation) {
			currentFunc = pendingContinuation;
			pendingContinuation = nullptr;
		}
	}

	// If we're inside a loop and ended in a continuation, add loop-back logic
	if (ctx.loopDepth > 0 && currentFunc != func && !ctx.continueLabel.empty()) {
		if (!currentFunc->endIsUnreachable) {
			currentFunc->writeLine("if (loopDepth == 0) return " + ctx.continueLabel + "(0);");
			currentFunc->writeLine("");
			currentFunc->writeLine("return loopDepth;");
			currentFunc->endIsUnreachable = true;
		}
	}
}

void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
	// Generate continuation method names and callback index
	int cbIndex = nextCallbackIndex();
	int localWaitIndex = func->getNextWaitIndex();  // Track waits per function for nested naming

	std::string whenMethodName;
	std::string contMethodName;

	// If inside a loop, use loop-specific naming
	if (ctx.loopDepth > 0 && !ctx.loopBodyPrefix.empty()) {
		// Inside loop: continuations are named relative to loop body
		// e.g., "a_body1cont1loopBody1when1" and "a_body1cont1loopBody1cont1"
		whenMethodName = ctx.loopBodyPrefix + "when" + std::to_string(localWaitIndex);
		contMethodName = ctx.loopBodyPrefix + "cont" + std::to_string(localWaitIndex);
	} else {
		// Normal case: use function name + when/cont
		whenMethodName = func->name + "when" + std::to_string(localWaitIndex);

		// Use continuation index from context if available (e.g., inside try block with catch handler),
		// otherwise use callback index + 1 for normal waits
		int contIndex = ctx.continuationIndex > 0 ? ctx.continuationIndex : (cbIndex + 1);
		contMethodName = "a_body1cont" + std::to_string(contIndex);
	}

	// If this is a state variable result, ensure it's in stateVariables
	if (stmt->resultIsState && !stmt->result.name.empty()) {
		stateVariables.insert(stmt->result.name);
		stateVariableTypes[stmt->result.name] = stmt->result.type;
	}

	// Track callback for later class generation
	CallbackInfo cb;
	cb.type = stmt->result.type;
	cb.index = cbIndex;
	cb.continueLabel = whenMethodName;  // Store when method name for callback generation
	cb.resultName = stmt->result.name;
	cb.resultIsState = stmt->resultIsState;
	cb.errorHandler = ctx.catchHandler.empty() ? "a_body1Catch1" : ctx.catchHandler;
	cb.errorVarName = "error";
	callbacks.push_back(cb);

	// Use a unique future variable per wait to avoid name collisions
	std::string futureVar = "__when_expr_" + std::to_string(cbIndex);

	// Emit the wait expression assignment to a StrictFuture
	func->writeLine("StrictFuture<" + stmt->result.type + "> " + futureVar + " = " + stmt->futureExpression + ";");

	// Check for cancellation - route to appropriate error handler
	std::string errorHandler = ctx.catchHandler.empty() ? "a_body1Catch1" : ctx.catchHandler;

	// In loop context, adjust loopDepth for error handling
	std::string loopDepthAdjustment = ctx.loopDepth > 0 ? "std::max(0, loopDepth - 1)" : "loopDepth";
	func->writeLine("if (static_cast<" + className + "*>(this)->actor_wait_state < 0) return " + errorHandler + "(actor_cancelled(), " + loopDepthAdjustment + ");");

	// Check if the future is already ready (fast path optimization)
	func->writeLine("if (" + futureVar + ".isReady()) { if (" + futureVar + ".isError()) return " + errorHandler + "(" +
	                futureVar + ".getError(), " + loopDepthAdjustment + "); else return " + whenMethodName + "(" + futureVar + ".get(), loopDepth); };");

	// Future not ready - set up async callback and suspend
	func->writeLine("static_cast<" + className + "*>(this)->actor_wait_state = " + std::to_string(cbIndex + 1) + ";");
	func->writeLine(futureVar + ".addCallbackAndClear(static_cast<ActorCallback< " + className + ", " +
	                std::to_string(cbIndex) + ", " + stmt->result.type + " >*>(static_cast<" + className + "*>(this)));");
	func->writeLine("loopDepth = 0;");

	// Now generate the when methods (const& and && overloads) and continuation
	generateWhenMethod(whenMethodName, contMethodName, stmt->result.type, stmt->result.name, cbIndex);

	// Set pending continuation so subsequent statements go into the cont method
	pendingContinuation = getFunction(contMethodName);
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
	// Check if loop body contains wait statements
	if (containsWait(stmt->body.get())) {
		// Generate continuation methods for loop
		compileLoopWithContinuations(func, stmt->body.get(), stmt->condExpression, stmt->nextExpression, ctx);
		return;
	}

	// Simple loop without waits - use goto-based compilation
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
	// Flow actors only support a single catch clause
	if (stmt->catches.size() != 1) {
		throw Error(stmt->firstSourceLine, "try statement must have exactly one catch clause");
	}

	const auto& catchClause = stmt->catches[0];

	// Parse the catch expression to extract error variable name
	std::string errorVarName = "e";
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

	// Generate catch continuation method name (e.g., a_body1Catch2)
	int catchIndex = nextCatchHandlerIndex();
	std::string catchMethodName = "a_body1Catch" + std::to_string(catchIndex);

	// Create the catch continuation method
	Function* catchFunc = getFunction(catchMethodName);
	catchFunc->returnType = "int";
	catchFunc->formalParameters = {"const Error& " + errorVarName, "int loopDepth=0"};
	catchFunc->endIsUnreachable = true; // We include return in body, don't add another

	// Wrap catch body in try-catch to allow propagation to outer handler
	catchFunc->writeLine("try {");
	catchFunc->indent(+1);

	// Compile the catch body into the catch method
	Context catchCtx = ctx; // Inherit context but no inner catch handler
	compile(catchFunc, catchClause.body.get(), catchCtx);

	catchFunc->indent(-1);
	catchFunc->writeLine("}");
	catchFunc->writeLine("catch (Error& error) {");
	catchFunc->indent(+1);
	catchFunc->writeLine("loopDepth = a_body1Catch1(error, loopDepth);");
	catchFunc->indent(-1);
	catchFunc->writeLine("} catch (...) {");
	catchFunc->indent(+1);
	catchFunc->writeLine("loopDepth = a_body1Catch1(unknown_error(), loopDepth);");
	catchFunc->indent(-1);
	catchFunc->writeLine("}");
	catchFunc->writeLine("");
	catchFunc->writeLine("return loopDepth;");

	// Now generate the try block in the main function
	func->writeLine("try {");
	func->indent(+1);

	// Compile try body with context pointing to inner catch handler
	// Set continuationIndex to match catch handler index so continuations are named correctly
	Context tryCtx = ctx.withCatch(errorVarName, "unused", catchMethodName);
	tryCtx.continuationIndex = catchIndex;
	compile(func, stmt->tryBody.get(), tryCtx);

	func->indent(-1);
	func->writeLine("}");
	func->writeLine("catch (Error& error) {");
	func->indent(+1);
	func->writeLine("loopDepth = " + catchMethodName + "(error, loopDepth);");
	func->indent(-1);
	func->writeLine("} catch (...) {");
	func->indent(+1);
	func->writeLine("loopDepth = " + catchMethodName + "(unknown_error(), loopDepth);");
	func->indent(-1);
	func->writeLine("}");
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

void ActorCompiler::generateWhenMethod(const std::string& whenMethodName,
                                        const std::string& contMethodName,
                                        const std::string& type,
                                        const std::string& resultName,
                                        int cbIndex) {
	// Generate const& overload
	Function* whenFuncConst = getFunction(whenMethodName);
	whenFuncConst->returnType = "int";
	whenFuncConst->formalParameters = {type + " const& __" + resultName, "int loopDepth"};
	if (!resultName.empty()) {
		whenFuncConst->writeLine(resultName + " = __" + resultName + ";");
	}
	whenFuncConst->writeLine("loopDepth = " + contMethodName + "(loopDepth);");
	whenFuncConst->writeLine("return loopDepth;");
	whenFuncConst->endIsUnreachable = true;

	// Generate && overload (separate function with overload marker)
	Function* whenFuncMove = getFunction(whenMethodName + "_rvalue");
	whenFuncMove->name = whenMethodName;  // Same name for overload
	whenFuncMove->returnType = "int";
	whenFuncMove->formalParameters = {type + " && __" + resultName, "int loopDepth"};
	if (!resultName.empty()) {
		whenFuncMove->writeLine(resultName + " = std::move(__" + resultName + ");");
	}
	whenFuncMove->writeLine("loopDepth = " + contMethodName + "(loopDepth);");
	whenFuncMove->writeLine("return loopDepth;");
	whenFuncMove->endIsUnreachable = true;

	// Create the continuation method (body will be filled by subsequent compilation)
	Function* contFunc = getFunction(contMethodName);
	contFunc->returnType = "int";
	contFunc->formalParameters = {"int loopDepth"};

	// Generate exitChoose cleanup method
	std::string exitMethodName = "a_exitChoose" + std::to_string(cbIndex + 1);
	Function* exitFunc = getFunction(exitMethodName);
	exitFunc->returnType = "void";
	exitFunc->formalParameters = {};
	exitFunc->writeLine("if (static_cast<" + className + "*>(this)->actor_wait_state > 0) static_cast<" + className +
	                    "*>(this)->actor_wait_state = 0;");
	exitFunc->writeLine("static_cast<" + className + "*>(this)->ActorCallback< " + className + ", " +
	                    std::to_string(cbIndex) + ", " + type + " >::remove();");
	exitFunc->writeLine("");
	exitFunc->endIsUnreachable = true;
}

void ActorCompiler::compileLoopWithContinuations(Function* func,
                                                  Statement* loopBody,
                                                  const std::string& condExpression,
                                                  const std::string& nextExpression,
                                                  const Context& ctx) {
	// Increment loop counter for unique numbering
	int loopNum = ++loopCounter;

	// Generate unique names for loop continuation methods based on current function
	std::string loopHeadName = func->name + "loopHead" + std::to_string(loopNum);
	std::string loopBodyName = func->name + "loopBody" + std::to_string(loopNum);
	std::string loopBreakName = func->name + "break" + std::to_string(loopNum);

	// In the current function, just call the loop head method
	func->writeLine("loopDepth = " + loopHeadName + "(loopDepth);");
	func->writeLine("");
	func->writeLine("return loopDepth;");
	func->endIsUnreachable = true;

	// Create the loopHead method
	Function* loopHeadFunc = getFunction(loopHeadName);
	loopHeadFunc->returnType = "int";
	loopHeadFunc->formalParameters = {"int loopDepth"};
	loopHeadFunc->writeLine("int oldLoopDepth = ++loopDepth;");
	loopHeadFunc->writeLine("while (loopDepth == oldLoopDepth) loopDepth = " + loopBodyName + "(loopDepth);");
	loopHeadFunc->writeLine("");
	loopHeadFunc->writeLine("return loopDepth;");
	loopHeadFunc->endIsUnreachable = true;

	// Create the loopBody method
	Function* loopBodyFunc = getFunction(loopBodyName);
	loopBodyFunc->returnType = "int";
	loopBodyFunc->formalParameters = {"int loopDepth"};

	// Generate condition check and break
	if (!condExpression.empty()) {
		loopBodyFunc->writeLine("if (" + condExpression + ")");
		loopBodyFunc->writeLine("{");
		loopBodyFunc->indent(+1);
		loopBodyFunc->writeLine("return " + loopBreakName + "(loopDepth==0?0:loopDepth-1); // break");
		loopBodyFunc->indent(-1);
		loopBodyFunc->writeLine("}");
	}

	// Compile the loop body with a context that knows:
	// 1. It's inside a loop (loopDepth > 0)
	// 2. The loop body prefix for naming continuations
	// 3. Break/continue labels that are method names
	Context loopCtx = ctx.loopBodyContext(
		ctx.loopDepth + 1,  // Increment depth for nested loops
		loopBodyName,       // This becomes the prefix for continuations
		loopBreakName,      // Break returns to break handler
		loopHeadName        // Continue returns to loop head
	);

	compile(loopBodyFunc, loopBody, loopCtx);

	// loopBodyFunc should be marked unreachable after compiling the wait

	// Create the break handler method
	// This wraps the next continuation in try-catch
	Function* breakFunc = getFunction(loopBreakName);
	breakFunc->returnType = "int";
	breakFunc->formalParameters = {"int loopDepth"};
	breakFunc->writeLine("try {");
	breakFunc->indent(+1);

	// The break handler calls the next continuation after the loop
	// Create the next continuation method that will hold code after the loop
	std::string nextContName;
	if (ctx.loopDepth > 0) {
		// Inside nested loop - next continuation is relative to outer loop body
		// This is complex, for now just use cont + cbIndex
		nextContName = "a_body1cont" + std::to_string(callbackCounter);
	} else {
		// Top-level loop in function - next continuation is cont + cbIndex
		nextContName = "a_body1cont" + std::to_string(callbackCounter);
	}

	breakFunc->writeLine("return " + nextContName + "(loopDepth);");
	breakFunc->indent(-1);
	breakFunc->writeLine("}");
	breakFunc->writeLine("catch (Error& error) {");
	breakFunc->indent(+1);
	breakFunc->writeLine("loopDepth = a_body1Catch1(error, loopDepth);");
	breakFunc->indent(-1);
	breakFunc->writeLine("} catch (...) {");
	breakFunc->indent(+1);
	breakFunc->writeLine("loopDepth = a_body1Catch1(unknown_error(), loopDepth);");
	breakFunc->indent(-1);
	breakFunc->writeLine("}");
	breakFunc->writeLine("");
	breakFunc->writeLine("return loopDepth;");
	breakFunc->endIsUnreachable = true;

	// Create the next continuation method for code after the loop
	Function* nextContFunc = getFunction(nextContName);
	nextContFunc->returnType = "int";
	nextContFunc->formalParameters = {"int loopDepth"};

	// Set the next continuation as pending so code after the loop goes there
	pendingContinuation = nextContFunc;
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
	       << "public FastAllocated<" << fullClassName << ">, public " << stateClassName << "<" << className << "> {\n";
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

	// Friend declarations for callback base classes
	for (const auto& cb : callbacks) {
		writer << "friend struct ActorCallback< " << className << ", " << cb.index << ", " << cb.type << " >;\n";
	}

	lineNumber(writer, actor.sourceLine);

	// Constructor with proper initialization list
	writeTemplate(writer);
	// Reuse returnType variable already declared above at line 897
	std::vector<std::string> paramNames;
	for (const auto& p : actor.parameters) {
		paramNames.push_back(p.name);
	}

	writer << "\t" << className << "(" << join(parameterList(), ", ") << ")\n";
	writer << "\t\t : Actor<" << returnType << ">(),\n";
	writer << "\t\t   " << stateClassName << "<" << className << ">(" << join(paramNames, ", ") << "),\n";
	writer << "\t\t   activeActorHelper(__actorIdentifier)\n";
	writer << "\t{\n";

	// ACAC instrumentation
	writer << "\t\t#ifdef WITH_ACAC\n";
	auto constructorBlockKey = sourceFile + ":" + actor.name + ":constructor";
	auto constructorBlockId = getUidFromString(constructorBlockKey);
	uidObjects[constructorBlockId] = constructorBlockKey;
	writer << "\t\tstatic constexpr ActorBlockIdentifier __identifier = UID("
	       << constructorBlockId.first << "UL, " << constructorBlockId.second << "UL);\n";
	writer << "\t\tActorExecutionContextHelper __helper(this->activeActorHelper.actorID, __identifier);\n";
	writer << "\t\t#endif // WITH_ACAC\n";

	// Lineage support
	writer << "\t\t#ifdef ENABLE_SAMPLING\n";
	writer << "\t\tthis->lineage.setActorName(\"" << actor.name << "\");\n";
	writer << "\t\tLineageScope _(&this->lineage);\n";
	writer << "\t\t#endif\n";

	writer << "\t\tthis->a_body1();\n";
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
		// Generate case for each callback using actual callback index
		for (size_t i = 0; i < callbacks.size(); ++i) {
			int cbIndex = callbacks[i].index;
			writer << "\t\tcase " << (cbIndex + 1) << ": this->a_callback_error(static_cast<ActorCallback<" << className
			       << ", " << cbIndex << ", " << callbacks[i].type << ">*>(nullptr), actor_cancelled()); break;\n";
		}
		writer << "\t\t}\n";
	}
	writer << "\t}\n";

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

	// Generate callback methods in state class
	for (const auto& cb : callbacks) {
		writeStateCallbackMethods(writer, cb);
	}
}

void ActorCompiler::writeFunction(std::ostream& writer, Function* func) {
	// Function signature with proper formal parameters
	std::string returnTypeStr = func->returnType.empty() ? "int" : func->returnType + " ";
	writer << "\t" << returnTypeStr << func->name << "(";

	// Formal parameters
	if (!func->formalParameters.empty()) {
		writer << join(func->formalParameters, ",");
	} else if (func->returnType != "void") {
		// Only add default loopDepth for non-void functions (body/cont methods)
		writer << "int loopDepth";
	}
	// else: void return type (exitChoose methods) - no parameters

	writer << ")";

	// Function specifiers
	if (!func->specifiers.empty()) {
		writer << " " << func->specifiers;
	}

	writer << " \n\t{\n";

	// Determine if this function needs try-catch wrapper
	// Only a_body methods (not a_cont, not a_when, not a_Catch) should have try-catch
	bool needsTryCatch = (func->name.find("a_body") == 0 || func->name.find("a_Body") == 0) &&
	                     func->name.find("Catch") == std::string::npos &&
	                     func->name.find("cont") == std::string::npos &&
	                     func->name.find("Cont") == std::string::npos &&
	                     func->name.find("when") == std::string::npos &&
	                     func->name.find("When") == std::string::npos;

	// Add try block if needed
	if (needsTryCatch) {
		writer << "\t\ttry {\n";
	}

	// Function body
	std::string bodyText = func->getBodyText();
	if (!bodyText.empty()) {
		// Add indentation to each line
		// Inside try block: 3 tabs total (class member + function body + try block)
		// Regular function: 2 tabs total (class member + function body)
		std::string baseIndent = needsTryCatch ? "\t\t\t" : "\t\t";
		size_t pos = 0;
		while (pos < bodyText.length()) {
			size_t endPos = bodyText.find('\n', pos);
			if (endPos == std::string::npos) {
				endPos = bodyText.length();
			}

			std::string line = bodyText.substr(pos, endPos - pos);
			if (!line.empty()) {
				writer << baseIndent << line << "\n";
			} else {
				writer << "\n";
			}

			pos = endPos + 1;
		}
	}

	// Add catch blocks if needed
	if (needsTryCatch) {
		writer << "\t\t}\n";
		writer << "\t\tcatch (Error& error) {\n";
		writer << "\t\t\tloopDepth = a_body1Catch1(error, loopDepth);\n";
		writer << "\t\t} catch (...) {\n";
		writer << "\t\t\tloopDepth = a_body1Catch1(unknown_error(), loopDepth);\n";
		writer << "\t\t}\n";
	}

	// Return statement if not unreachable
	if (!func->endIsUnreachable) {
		writer << "\t\treturn loopDepth;\n";
	}

	writer << "\t}\n";
}

void ActorCompiler::writeStateCallbackMethods(std::ostream& writer, const CallbackInfo& cb) {
	std::string exitMethodName = "a_exitChoose" + std::to_string(cb.index + 1);
	std::string whenMethodName = cb.continueLabel;  // This is the when method name
	std::string catchMethodName = cb.errorHandler;

	// a_callback_fire - const& overload
	writer << "\tvoid a_callback_fire(ActorCallback< " << className << ", " << cb.index << ", " << cb.type
	       << " >*," << cb.type << " const& value) \n";
	writer << "\t{\n";
	writer << "\t\t#ifdef WITH_ACAC\n";
	auto callbackFireKey = sourceFile + ":" + actor.name + ":callback_fire:" + std::to_string(cb.index);
	auto callbackFireId = getUidFromString(callbackFireKey);
	uidObjects[callbackFireId] = callbackFireKey;
	writer << "\t\tstatic constexpr ActorBlockIdentifier __identifier = UID(" << callbackFireId.first
	       << "UL, " << callbackFireId.second << "UL);\n";
	writer << "\t\tActorExecutionContextHelper __helper(static_cast<" << className
	       << "*>(this)->activeActorHelper.actorID, __identifier);\n";
	writer << "\t\t#endif // WITH_ACAC\n";
	writer << "\t\t" << exitMethodName << "();\n";
	writer << "\t\ttry {\n";
	writer << "\t\t\t" << whenMethodName << "(value, 0);\n";
	writer << "\t\t}\n";
	writer << "\t\tcatch (Error& error) {\n";
	writer << "\t\t\t" << catchMethodName << "(error, 0);\n";
	writer << "\t\t} catch (...) {\n";
	writer << "\t\t\t" << catchMethodName << "(unknown_error(), 0);\n";
	writer << "\t\t}\n";
	writer << "\n";
	writer << "\t}\n";

	// a_callback_fire - && overload
	writer << "\tvoid a_callback_fire(ActorCallback< " << className << ", " << cb.index << ", " << cb.type
	       << " >*," << cb.type << " && value) \n";
	writer << "\t{\n";
	writer << "\t\t#ifdef WITH_ACAC\n";
	writer << "\t\tstatic constexpr ActorBlockIdentifier __identifier = UID(" << callbackFireId.first
	       << "UL, " << callbackFireId.second << "UL);\n";
	writer << "\t\tActorExecutionContextHelper __helper(static_cast<" << className
	       << "*>(this)->activeActorHelper.actorID, __identifier);\n";
	writer << "\t\t#endif // WITH_ACAC\n";
	writer << "\t\t" << exitMethodName << "();\n";
	writer << "\t\ttry {\n";
	writer << "\t\t\t" << whenMethodName << "(std::move(value), 0);\n";
	writer << "\t\t}\n";
	writer << "\t\tcatch (Error& error) {\n";
	writer << "\t\t\t" << catchMethodName << "(error, 0);\n";
	writer << "\t\t} catch (...) {\n";
	writer << "\t\t\t" << catchMethodName << "(unknown_error(), 0);\n";
	writer << "\t\t}\n";
	writer << "\n";
	writer << "\t}\n";

	// a_callback_error
	writer << "\tvoid a_callback_error(ActorCallback< " << className << ", " << cb.index << ", " << cb.type
	       << " >*,Error err) \n";
	writer << "\t{\n";
	writer << "\t\t#ifdef WITH_ACAC\n";
	auto callbackErrorKey = sourceFile + ":" + actor.name + ":callback_error:" + std::to_string(cb.index);
	auto callbackErrorId = getUidFromString(callbackErrorKey);
	uidObjects[callbackErrorId] = callbackErrorKey;
	writer << "\t\tstatic constexpr ActorBlockIdentifier __identifier = UID(" << callbackErrorId.first
	       << "UL, " << callbackErrorId.second << "UL);\n";
	writer << "\t\tActorExecutionContextHelper __helper(static_cast<" << className
	       << "*>(this)->activeActorHelper.actorID, __identifier);\n";
	writer << "\t\t#endif // WITH_ACAC\n";
	writer << "\t\t" << exitMethodName << "();\n";
	writer << "\t\ttry {\n";
	writer << "\t\t\t" << catchMethodName << "(err, 0);\n";
	writer << "\t\t}\n";
	writer << "\t\tcatch (Error& error) {\n";
	writer << "\t\t\t" << catchMethodName << "(error, 0);\n";
	writer << "\t\t} catch (...) {\n";
	writer << "\t\t\t" << catchMethodName << "(unknown_error(), 0);\n";
	writer << "\t\t}\n";
	writer << "\n";
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
