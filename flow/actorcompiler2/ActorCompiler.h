/*
 * ActorCompiler.h
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

#ifndef FLOW_ACTOR_COMPILER_H
#define FLOW_ACTOR_COMPILER_H

#include <cstdint>
#include <format>
#include <string>
#include <vector>
#include <unordered_map>
#include <regex>
#include <map>
#include <iostream>
#include <set>
#include <sstream>

#include "ParseTree.h"

namespace actorcompiler {

class Error;

template <typename T>
T safe_value(const std::optional<T>& opt, const char* file, int line) {
	if (!opt.has_value()) {
		std::cerr << "Empty optional accessed at " << file << ":" << line << std::endl;
		throw std::bad_optional_access();
	}
	return opt.value();
}

template <typename Container>
std::string join(const Container& container, const std::string& delimiter) {
	std::ostringstream result;
	bool first = true;

	for (const auto& item : container) {
		if (!first) {
			result << delimiter;
		}
		result << item;
		first = false;
	}

	return result.str();
}

class Function {
public:
	std::string name;
	std::string returnType;
	std::vector<std::string> formalParameters;
	bool endIsUnreachable = false;
	std::string exceptionParameterIs = "";
	bool publicName = false;
	std::string specifiers;
	std::string indentation;
	std::stringstream body; // C++ uses stringstream instead of StreamWriter/MemoryStream
	bool wasCalled = false;
	std::unique_ptr<Function> overload;

	Function() = default;
	Function(const Function& r)
	  : name(r.name), returnType(r.returnType), formalParameters(r.formalParameters),
	    endIsUnreachable(r.endIsUnreachable), exceptionParameterIs(r.exceptionParameterIs), publicName(r.publicName),
	    specifiers(r.specifiers), indentation(r.indentation), wasCalled(r.wasCalled) {
		// body is not copied
		if (r.overload != nullptr) {
			overload = std::make_unique<Function>(*r.overload);
		}
	}

	Function& operator=(const Function& r) {
		if (this != &r) {
			name = r.name;
			returnType = r.returnType;
			formalParameters = r.formalParameters;
			endIsUnreachable = r.endIsUnreachable;
			exceptionParameterIs = r.exceptionParameterIs;
			publicName = r.publicName;
			specifiers = r.specifiers;
			indentation = r.indentation;
			wasCalled = r.wasCalled;
			if (r.overload != nullptr) {
				overload = std::make_unique<Function>(*r.overload);
			} else {
				overload.reset();
			}
		}
		return *this;
	}
	bool operator!=(const Function& r) {
		return name != r.name || returnType != r.returnType || formalParameters != r.formalParameters ||
		       endIsUnreachable != r.endIsUnreachable || exceptionParameterIs != r.exceptionParameterIs ||
		       publicName != r.publicName || specifiers != r.specifiers || indentation != r.indentation ||
		       wasCalled != r.wasCalled || *overload != *r.overload;
	}

	bool getWasCalled() const { return wasCalled; }

	void setOverload(const Function& overload) {
		auto copy = std::make_unique<Function>(overload);
		this->overload = std::move(copy);
	}

	Function* popOverload() {
		Function* result = this->overload.get();
		this->overload = nullptr;
		return result;
	}

	void addOverload(const std::vector<std::string>& params) {
		Function newOverload;
		newOverload.name = name;
		newOverload.returnType = returnType;
		newOverload.endIsUnreachable = endIsUnreachable;
		newOverload.formalParameters = params;
		newOverload.indentation = indentation;
		setOverload(newOverload);
	}

	// Variadic template to mimic C#'s params keyword
	template <typename... Args>
	void addOverload(Args... args) {
		std::vector<std::string> params = { args... };
		addOverload(params);
	}

	void Indent(int change) {
		for (int i = 0; i < change; i++) {
			indentation += '\t';
		}
		if (change < 0) {
			indentation = indentation.substr(0, indentation.length() + change);
		}
		if (overload != nullptr) {
			overload->Indent(change);
		}
	}

	void WriteLineUnindented(const std::string& s) {
		body << s << std::endl;
		if (overload != nullptr) {
			overload->WriteLineUnindented(s);
		}
	}

	void WriteLine(const std::string& line) {
		body << indentation << line << std::endl;
		if (overload != nullptr) {
			overload->WriteLine(line);
		}
	}

	std::string BodyText() { return body.str(); }

	std::string useByName() {
		wasCalled = true;
		if (publicName)
			return name;
		else
			return "a_" + name;
	}

	virtual std::string call(const std::vector<std::string>& parameters) {
		std::string paramStr = join(parameters, ", ");
		return useByName() + "(" + paramStr + ")";
	}

	// Variadic template to mimic C#'s params keyword
	template <typename... Args>
	std::string call(Args... args) {
		std::vector<std::string> params = { args... };
		return call(params);
	}

	// Destructor to handle overload chain
	virtual ~Function() { overload.reset(); }

	/* A (C++) continuation function for point P in the (actor) control flow graph
	 *      int fP( [params,] int loopDepth = 0 );
	 * has the following responsibilities:
	 *      (A) If loopDepth==0, run the actor beginning at point P until
	 *          (1) it waits, in which case set an appropriate callback and return 0, or
	 *          (2) it returns, in which case destroy the actor and return 0.
	 *      (B) If loopDepth>0, run the actor beginning at point P until
	 *          (1) it waits, in which case set an appropriate callback and return 0, or
	 *          (2) it returns, in which case destroy the actor and return 0, or
	 *          (3) it reaches the bottom of the Nth innermost loop containing P, in which case
	 *                  return max(0, the given loopDepth - N)    (N=0 for the innermost loop, N=1 for the next
	 * innermost, etc)
	 *
	 * Examples:
	 *      Source:
	 *          loop
	 *              [P]
	 *              loop
	 *                  [P']
	 *                  loop
	 *                      [P'']
	 *                      break
	 *                  [Q']
	 *                  break
	 *              [Q]
	 *
	 *      fP(1) should execute everything from [P] to [Q] and then return 1 (since [Q] is at the bottom of the 0th
	 * innermost loop containing [P]) fP'(2) should execute everything from [P'] to [Q] and then return 1 (since [Q] is
	 * at the bottom of the 1st innermost loop containing [P']) fP''(3) should execute everything from [P''] to [Q] and
	 * then return 1 (since [Q] is at the bottom of the 2nd innermost loop containing [P'']) fQ'(2) should execute
	 * everything from [Q'] to [Q] and then return 1 (since [Q] is at the bottom of the 1st innermost loop containing
	 * [Q']) fQ(1) should return 1 (since [Q] is at the bottom of the 0th innermost loop containing [Q])
	 */
};

class LiteralBreak : public Function {
public:
	LiteralBreak() { name = "break!"; }

	std::string call(const std::vector<std::string>& parameters) override {
		wasCalled = true;
		if (!parameters.empty()) {
			throw std::runtime_error("LiteralBreak called with parameters!");
		}
		return "break";
	}
};

class LiteralContinue : public Function {
public:
	LiteralContinue() { name = "continue!"; }

	std::string call(const std::vector<std::string>& parameters) override {
		wasCalled = true;
		if (!parameters.empty()) {
			throw std::runtime_error("LiteralContinue called with parameters!");
		}
		return "continue";
	}
};

class StateVar : public VarDeclaration {
public:
	StateVar() = default;

	int sourceLine = 0;
};

class CallbackVar : public StateVar {
public:
	int callbackGroup;
};

class DescrCompiler {
private:
	Descr descr;
	std::string memberIndentStr;

public:
	DescrCompiler(const Descr& descr, int braceDepth) {
		this->descr = descr;
		this->memberIndentStr = std::string(braceDepth, '\t');
	}

	void write(std::ostream& writer, int& lines);
};

// Context for compilation
class Context {
public:
	std::optional<Function> target;
	std::optional<Function> next;
	std::optional<Function> breakF;
	std::optional<Function> continueF;
	std::optional<Function> catchFErr;
	// The number of (loopDepth-increasing) loops entered inside the innermost
	// try (thus, that will be exited by a throw)
	int tryLoopDepth = -1;

	Context() = default;
	Context(const Context& r) = default;

	Context Clone() const { return Context(*this); }

	Context WithTarget(const Function& t) const {
		Context cx = Clone();
		cx.target = t;
		cx.next = std::nullopt;
		return cx;
	}

	Context LoopContext(const Function& newTarget,
	                    const Function newBreakF,
	                    const Function newContinueF,
	                    int deltaLoopDepth) {
		Context cx(*this);
		cx.next = std::nullopt;
		cx.tryLoopDepth = tryLoopDepth + deltaLoopDepth;
		return cx;
	}

	Context WithCatch(const Function& newCatchFErr) const {
		Context cx(*this);
		cx.next = std::nullopt;
		cx.catchFErr = newCatchFErr;
		cx.tryLoopDepth = 0;
		return cx;
	}

	void unreachable() { target = std::nullopt; }
};

class ActorCompiler {
public:
	ActorCompiler(Actor actor, std::string sourceFile, bool isTopLevel, bool lineNumbersEnabled, bool generateProbes)
	  : actor(actor), sourceFile(sourceFile), isTopLevel(isTopLevel), LineNumbersEnabled(lineNumbersEnabled),
	    generateProbes(generateProbes) {
		FindState();
	}
	~ActorCompiler() {}

	// Code generation methods
	void CompilePlainStatement(const std::shared_ptr<PlainOldCodeStatement> stmt, Context cx);
	void CompileStateDeclStatement(const std::shared_ptr<StateDeclarationStatement> stmt, Context cx);
	void CompileForStatement(const std::shared_ptr<ForStatement> stmt, Context cx);
	void CompileLoopStatement(const std::shared_ptr<LoopStatement> stmt, Context cx);
	void CompileChooseStatement(const std::shared_ptr<ChooseStatement> stmt, Context cx);
	void CompileWhenStatement(const std::shared_ptr<ChooseStatement> stmt, Context cx);
	void CompileWhileStatement(const std::shared_ptr<WhileStatement> stmt, Context cx);
	void CompileRangeForStatement(const std::shared_ptr<RangeForStatement> stmt, Context cx);
	void CompileBreakStatement(const std::shared_ptr<BreakStatement>& stmt, Context cx);
	void CompileContinueStatement(const std::shared_ptr<ContinueStatement>& stmt, Context cx);
	void CompileWaitStatement(const std::shared_ptr<WaitStatement>& stmt, Context cx);
	void CompileCodeBlockStatement(const std::shared_ptr<CodeBlock>& stmt, Context cx);
	void CompileReturnStatement(const std::shared_ptr<ReturnStatement>& stmt, Context cx);
	void CompileIfStatement(const std::shared_ptr<IfStatement>& stmt, Context cx);
	void CompileTryStatement(const std::shared_ptr<TryStatement>& stmt, Context cx);
	void CompileThrowStatement(const std::shared_ptr<ThrowStatement>& stmt, Context cx);

	void CompileStatement(const std::shared_ptr<Statement>& stmt, Context cx);

	// Compile returns a new context based on the one that is passed in, but (unlike CompileStatement)
	//   does not modify its parameter
	// The target of the returned context is null if the end of the CodeBlock is unreachable (otherwise
	//   it is the target Function to which the end of the CodeBlock was written)
	Context Compile(std::shared_ptr<CodeBlock> block, const Context& context, bool okToContinue = true);
	void WriteAllFunctions(std::ostream& output);
	void Write(std::ostream& writer);

	// Helper methods for code generation
	void WriteCancelFunc(std::ostream& output);
	void WriteConstructor(Function& body, std::ostream& writer, const std::string& fullStateClassName);
	void WriteStateConstructor(std::ostream& output);
	void WriteStateDestructor(std::ostream& output);
	void WriteClassHeader(std::ostream& output);
	void WriteClassFooter(std::ostream& output);
	void WriteStateClassDefinition(std::ostream& output);

	void WriteFunctions(std::ostream& writer);
	void WriteFunction(std::ostream& writer, Function& func, const std::string& body);

	Function& getFunction(const std::string& baseName,
	                      const std::string& addName,
	                      const std::vector<std::string>& formalParameters,
	                      const std::vector<std::string>& overloadFormalParameters);
	Function& getFunction(const std::string& baseName,
	                      const std::string& addName,
	                      const std::vector<std::string>& formalParameters);
	std::vector<std::string> ParameterList();

	// Utility methods
	void LineNumber(std::ostream& writer, int SourceLine);
	void LineNumber(Function& func, int line);
	std::string GetTemplateActuals(const std::vector<VarDeclaration>& extraParameters);
	bool WillContinue(std::shared_ptr<Statement> stmt);
	std::shared_ptr<CodeBlock> AsCodeBlock(std::shared_ptr<Statement> stmt);

	static void TryCatch(Context cx,
	                     std::optional<Function> catchFErr,
	                     int catchLoopDepth,
	                     std::function<void()> action,
	                     bool useLoopDepth = true);
	Context TryCatchCompile(std::shared_ptr<CodeBlock> block, Context cx);
	void WriteTemplate(std::ostream& writer, const std::vector<VarDeclaration>& extraParameters);
	void WriteActorClass(std::ostream& writer, const std::string& fullStateClassName, Function& body);

	void WriteTemplaWrite(std::ostream& writer);
	void ProbeEnter(Function& fun, const std::string& name, int index = -1);
	void ProbeExit(Function& fun, const std::string& name, int index = -1);
	void ProbeCreate(Function& fun, const std::string& name);
	void ProbeDestroy(Function& fun, const std::string& name);

	static std::string AdjustLoopDepth(int subtract);
	static CodeBlock* AsCodeBlock(Statement* stmt);

	std::unordered_map<std::pair<uint64_t, uint64_t>, std::string, PairHash> uidObjects;

private:
	// Actor definition
	Actor actor;
	std::vector<std::string> includes;
	std::string className, fullClassName;
	std::string stateClassName;
	std::string sourceFile;

	std::vector<StateVar> state;
	std::vector<CallbackVar> callbacks;
	bool isTopLevel;

	const std::string loopDepth0 = "int loopDepth=0";
	const std::string loopDepth = "int loopDepth";
	const std::string thisAddress = "reinterpret_cast<unsigned long>(this)";
	const int codeIndent = +2;
	const std::string memberIndentStr = "\t";

	static std::set<std::string> usedClassNames;
	bool LineNumbersEnabled;
	int chooseGroups = 0, whenCount = 0;
	std::string This;
	bool generateProbes;

	std::map<std::string, Function> functions;
	std::map<std::string, int> iterators;

	std::string getIteratorName(Context cx);
	bool EmitNativeLoop(int sourceLine, const std::string& head, std::shared_ptr<Statement> body, Context cx);

	// Helper methods
	void FindState();
	uint64_t ByteToLong(const uint8_t* bytes, size_t length);
	std::pair<uint64_t, uint64_t> GetUidFromString(const std::string& str);
	void WriteActorFunction(std::ostream& writer, const std::string& fullReturnType);
	std::vector<std::shared_ptr<Statement>> Flatten(const std::shared_ptr<Statement>& stmt);

	// void ParseActorCode(const std::string& actorCode);
	// void FindStateVariables();
	// std::string JoinStrings(const std::vector<std::string>& elements, const std::string& delimiter);
};

} // namespace actorcompiler

#endif