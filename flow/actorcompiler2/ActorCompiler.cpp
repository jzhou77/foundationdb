/*
 * ActorCompiler.cpp
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

#include "ActorCompiler.h"
#include "ActorParser.h"
#include "ParseTree.h"

#include <cassert>
#include <openssl/sha.h> // for SHA256 functionality

namespace actorcompiler {

void ErrorMessagePolicy::HandleActorWithoutWait(const std::string& sourceFile, const Actor& actor) const {
	if (!DisableDiagnostics && !actor.isTestCase) {
		// TODO(atn34): Once cmake is the only build system we can make this an error instead of a warning.
		std::cerr << sourceFile << ":" << actor.sourceLine << ": warning: ACTOR " << actor.name
		          << " does not contain a wait() statement" << std::endl;
	}
}

void DescrCompiler::write(std::ostream& writer, int& lines) {
	lines = 0;
	writer << memberIndentStr << "template<> struct Descriptor<struct " << descr.name << "> {" << std::endl;
	writer << memberIndentStr << "\tstatic StringRef typeName() { return \"" << descr.name << "\"_sr; }" << std::endl;
	writer << memberIndentStr << "\ttypedef " << descr.name << " type;" << std::endl;
	lines += 3;

	for (const auto& dec : descr.body) {
		writer << memberIndentStr << "\tstruct " << dec.name << "Descriptor {" << std::endl;
		writer << memberIndentStr << "\t\tstatic StringRef name() { return \"" << dec.name << "\"_sr; }" << std::endl;
		writer << memberIndentStr << "\t\tstatic StringRef typeName() { return \"" << dec.type << "\"_sr; }"
		       << std::endl;
		writer << memberIndentStr << "\t\tstatic StringRef comment() { return \"" << dec.comment << "\"_sr; }"
		       << std::endl;
		writer << memberIndentStr << "\t\ttypedef " << dec.type << " type;" << std::endl;
		writer << memberIndentStr << "\t\tstatic inline type get(" << descr.name << "& from);" << std::endl;
		writer << memberIndentStr << "\t};" << std::endl;
		lines += 7;
	}

	writer << memberIndentStr << "\ttypedef std::tuple<";
	bool FirstDesc = true;
	for (const auto& dec : descr.body) {
		if (!FirstDesc)
			writer << ",";
		writer << dec.name << "Descriptor";
		FirstDesc = false;
	}
	writer << "> fields;" << std::endl;

	writer << memberIndentStr
	       << "\ttypedef make_index_sequence_impl<0, index_sequence<>, std::tuple_size<fields>::value>::type "
	          "field_indexes;"
	       << std::endl;
	writer << memberIndentStr << "};" << std::endl;

	if (descr.superClassList.length() > 0)
		writer << memberIndentStr << "struct " << descr.name << " : " << descr.superClassList << " {" << std::endl;
	else
		writer << memberIndentStr << "struct " << descr.name << " {" << std::endl;

	lines += 4;

	for (const auto& dec : descr.body) {
		writer << memberIndentStr << "\t" << dec.type << " " << dec.name << "; //" << dec.comment << std::endl;
		lines++;
	}

	writer << memberIndentStr << "};" << std::endl;
	lines++;

	for (const auto& dec : descr.body) {
		writer << memberIndentStr << dec.type << " Descriptor<" << descr.name << ">::" << dec.name << "Descriptor::get("
		       << descr.name << "& from) { return from." << dec.name << "; }" << std::endl;
		lines++;
	}
}

std::set<std::string> ActorCompiler::usedClassNames;

void ActorCompiler::FindState() {
	state.clear();
	for (const auto& p : actor.parameters) {
		StateVar var;
		var.sourceLine = actor.sourceLine;
		var.name = p.name;
		var.type = p.type;
		var.initializer = p.name;
		var.initializerConstructorSyntax = false;
		state.push_back(var);
	}
}

// Generate an expression equivalent to max(0, loopDepth-subtract) for the given constant subtract
std::string ActorCompiler::AdjustLoopDepth(int subtract) {
	if (subtract == 0)
		return "loopDepth";
	else
		return std::format("std::max(0, loopDepth - {})", subtract);
}

uint64_t ActorCompiler::ByteToLong(const uint8_t* bytes, size_t length) {
	// NOTE: Always assume big endian.
	uint64_t result = 0;
	for (size_t i = 0; i < length; i++) {
		result += bytes[i];
		if (i < length - 1) { // Skip shift on the last iteration
			result <<= 8;
		}
	}
	return result;
}

std::pair<uint64_t, uint64_t> ActorCompiler::GetUidFromString(const std::string& str) {
	uint8_t sha256Hash[SHA256_DIGEST_LENGTH];
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, str.c_str(), str.length());
	SHA256_Final(sha256Hash, &sha256);

	return std::make_tuple(ByteToLong(sha256Hash, 8), ByteToLong(sha256Hash + 8, 8));
}

// Writes the function that returns the Actor object
void ActorCompiler::WriteActorFunction(std::ostream& writer, const std::string& fullReturnType) {
	WriteTemplate(writer, {});
	LineNumber(writer, actor.sourceLine);

	for (const std::string& attribute : actor.attributes) {
		writer << attribute << " ";
	}

	if (actor.isStatic)
		writer << "static ";

	writer << fullReturnType << " " << (actor.nameSpace.empty() ? "" : actor.nameSpace + "::") << actor.name << "( "
	       << join(ParameterList(), ", ") << " ) {\n";

	LineNumber(writer, actor.sourceLine);

	std::string paramNames;
	std::vector<std::string> names;
	for (const auto& p : actor.parameters) {
		names.push_back(p.name);
	}

	for (size_t i = 0; i < names.size(); i++) {
		if (i > 0)
			paramNames += ", ";
		paramNames += names[i];
	}

	std::string newActor = "new " + fullClassName + "(" + paramNames + ")";

	if (!actor.returnType.empty()) {
		writer << "\treturn Future<" << actor.returnType << ">(" << newActor << ");\n";
	} else {
		writer << "\t" << newActor << ";\n";
	}

	writer << "}\n";
}

void ActorCompiler::WriteActorClass(std::ostream& writer, const std::string& fullStateClassName, Function& body) {
	// The final actor class mixes in the State class, the Actor base class and all callback classes
	writer << "// This generated class is to be used only via " << actor.name << "()\n";
	WriteTemplate(writer, {});
	LineNumber(writer, actor.sourceLine);

	std::string callback_base_classes;
	for (size_t i = 0; i < callbacks.size(); i++) {
		if (i > 0)
			callback_base_classes += ", ";
		callback_base_classes += "public " + callbacks[i].type;
	}
	if (!callback_base_classes.empty())
		callback_base_classes += ", ";

	writer << "class " << className << " final : public Actor<"
	       << (actor.returnType.empty() ? "void" : actor.returnType) << ">, " << callback_base_classes
	       << "public FastAllocated<" << fullClassName << ">, public " << fullStateClassName << " {\n";

	writer << "public:\n";
	writer << "\tusing FastAllocated<" << fullClassName << ">::operator new;\n";
	writer << "\tusing FastAllocated<" << fullClassName << ">::operator delete;\n";

	auto actorIdentifierKey = this->sourceFile + ":" + this->actor.name;
	auto actorIdentifier = GetUidFromString(actorIdentifierKey);
	uidObjects.emplace(std::make_pair(actorIdentifier.first, actorIdentifier.second), actorIdentifierKey);

	// NOTE UL is required as a u64 postfix for large integers, otherwise Clang would complain
	writer << "\tstatic constexpr ActorIdentifier __actorIdentifier = UID(" << actorIdentifier.first << "UL, "
	       << actorIdentifier.second << "UL);\n";
	writer << "\tActiveActorHelper activeActorHelper;\n";
	writer << "#pragma clang diagnostic push\n";
	writer << "#pragma clang diagnostic ignored \"-Wdelete-non-virtual-dtor\"\n";

	if (!actor.returnType.empty())
		writer << " void destroy() override {\n"
		       << "activeActorHelper.~ActiveActorHelper();\n"
		       << "static_cast<Actor<" << actor.returnType << ">*>(this)->~Actor();\n"
		       << "operator delete(this);\n"
		       << "}\n";
	else
		writer << " void destroy() {\n"
		       << "activeActorHelper.~ActiveActorHelper();\n"
		       << "static_cast<Actor<void>*>(this)->~Actor();\n"
		       << "operator delete(this);\n"
		       << "}\n";

	writer << "#pragma clang diagnostic pop\n";

	for (const auto& cb : callbacks)
		writer << "friend struct " << cb.type << ";\n";

	LineNumber(writer, actor.sourceLine);
	WriteConstructor(body, writer, fullStateClassName);
	WriteCancelFunc(writer);
	writer << "};\n";
}

void ActorCompiler::TryCatch(Context& cx,
                             std::optional<Function> catchFErr,
                             int catchLoopDepth,
                             std::function<void()> action,
                             bool useLoopDepth) {
	if (catchFErr.has_value()) {
		cx.target.value().WriteLine("try {");
		cx.target.value().Indent(+1);
	}

	action();

	if (catchFErr.has_value()) {
		cx.target.value().Indent(-1);
		cx.target.value().WriteLine("}");
		cx.target.value().WriteLine("catch (Error& error) {");
		if (useLoopDepth)
			cx.target.value().WriteLine(
			    "\tloopDepth = " + catchFErr.value().call("error", AdjustLoopDepth(catchLoopDepth)) + ";");
		else
			cx.target.value().WriteLine("\t" + catchFErr.value().call("error", "0") + ";");
		cx.target.value().WriteLine("} catch (...) {");
		if (useLoopDepth)
			cx.target.value().WriteLine(
			    "\tloopDepth = " + catchFErr.value().call("unknown_error()", AdjustLoopDepth(catchLoopDepth)) + ";");
		else
			cx.target.value().WriteLine("\t" + catchFErr.value().call("unknown_error()", "0") + ";");
		cx.target.value().WriteLine("}");
	}
}

Context ActorCompiler::TryCatchCompile(std::shared_ptr<CodeBlock> block, Context& cx) {
	TryCatch(cx, cx.catchFErr, cx.tryLoopDepth, [&]() {
		cx = Compile(block, cx, true);
		if (cx.target.has_value()) {
			Function next = getFunction(cx.target->name, "cont", { loopDepth });
			cx.target.value().WriteLine("loopDepth = " + next.call("loopDepth") + ";");
			cx.target = next;
			cx.next = std::nullopt;
		}
	});
	return cx;
}

void ActorCompiler::WriteTemplate(std::ostream& writer, const std::vector<VarDeclaration>& extraParameters) {
	std::vector<VarDeclaration> formals;

	if (!actor.templateFormals.empty()) {
		formals.insert(formals.end(), actor.templateFormals.begin(), actor.templateFormals.end());
	}

	formals.insert(formals.end(), extraParameters.begin(), extraParameters.end());

	if (formals.empty())
		return;

	LineNumber(writer, actor.sourceLine);

	std::string templateParams;
	bool first = true;

	for (auto& p : formals) {
		if (!first) {
			templateParams += ", ";
		}
		templateParams += p.type + " " + p.name;
		first = false;
	}

	writer << "template <" + templateParams + ">\n";
}

std::string ActorCompiler::GetTemplateActuals(const std::vector<VarDeclaration>& extraParameters) {
	std::vector<VarDeclaration> formals;

	if (!actor.templateFormals.empty()) {
		formals.insert(formals.end(), actor.templateFormals.begin(), actor.templateFormals.end());
	}

	formals.insert(formals.end(), extraParameters.begin(), extraParameters.end());

	if (formals.empty())
		return "";

	std::string result = "<";
	bool first = true;

	for (auto& p : formals) {
		if (!first) {
			result += ", ";
		}
		result += p.name;
		first = false;
	}

	result += ">";
	return result;
}

bool ActorCompiler::WillContinue(std::shared_ptr<Statement> stmt) {
	std::vector<std::shared_ptr<Statement>> flattened = Flatten(stmt);

	return std::any_of(flattened.begin(), flattened.end(), [](const std::shared_ptr<Statement>& st) {
		// Check if statement is any of the specified types using dynamic_pointer_cast
		return std::dynamic_pointer_cast<ChooseStatement>(st) != nullptr ||
		       std::dynamic_pointer_cast<WaitStatement>(st) != nullptr ||
		       std::dynamic_pointer_cast<TryStatement>(st) != nullptr;
	});
}

std::shared_ptr<CodeBlock> ActorCompiler::AsCodeBlock(std::shared_ptr<Statement> stmt) {
	if (!stmt)
		return nullptr;

	auto cb = std::dynamic_pointer_cast<CodeBlock>(stmt);
	if (cb)
		return cb;

	return std::make_shared<CodeBlock>(std::vector<std::shared_ptr<Statement>>{ stmt });
}

void ActorCompiler::CompileStatement(const std::shared_ptr<PlainOldCodeStatement> stmt, Context& cx) {
	LineNumber(cx.target.value(), stmt->firstSourceLine);
	cx.target.value().WriteLine(stmt->code);
}

void ActorCompiler::CompileStatement(const std::shared_ptr<StateDeclarationStatement> stmt, Context& cx) {
	// if this state declaration is at the very top of the actor body
	bool isAtTop = false;
	std::vector<std::shared_ptr<Statement>>& stmts = actor.body->statements;
	for (auto it : stmts) {
		auto stateDecl = dynamic_pointer_cast<StateDeclarationStatement>(it);
		if (!stateDecl)
			break;
		if (stateDecl == stmt) {
			isAtTop = true;
			break;
		}
	}

	if (isAtTop) {
		// Initialize the state in the constructor, not here
		StateVar stateVar;
		stateVar.sourceLine = stmt->firstSourceLine;
		stateVar.name = stmt->decl.name;
		stateVar.type = stmt->decl.type;
		stateVar.initializer = stmt->decl.initializer;
		stateVar.initializerConstructorSyntax = stmt->decl.initializerConstructorSyntax;
		state.push_back(stateVar);
	} else {
		// State variables declared elsewhere must have a default constructor
		StateVar stateVar;
		stateVar.sourceLine = stmt->firstSourceLine;
		stateVar.name = stmt->decl.name;
		stateVar.type = stmt->decl.type;
		stateVar.initializer = nullptr;
		state.push_back(stateVar);

		if (!stmt->decl.initializer.empty()) {
			LineNumber(cx.target.value(), stmt->firstSourceLine);
			if (stmt->decl.initializerConstructorSyntax || stmt->decl.initializer == "") {
				cx.target.value().WriteLine(
				    std::format("{0} = {1}({2});", stmt->decl.name, stmt->decl.type, stmt->decl.initializer));
			} else {
				cx.target.value().WriteLine(std::format("{0} = {1};", stmt->decl.name, stmt->decl.initializer));
			}
		}
	}
}

void ActorCompiler::CompileStatement(const std::shared_ptr<ForStatement> stmt, Context& cx) {
	// for( initExpression; condExpression; nextExpression ) body;
	bool noCondition = stmt->condExpression.empty() || stmt->condExpression == "true" || stmt->condExpression == "1";

	if (!WillContinue(stmt->body)) {
		// We can write this loop without anything fancy, because there are no wait statements in it
		std::string loopHeader =
		    "for(" + stmt->initExpression + ";" + stmt->condExpression + ";" + stmt->nextExpression + ")";

		if (EmitNativeLoop(stmt->firstSourceLine, loopHeader, stmt->body, cx) && noCondition)
			cx.unreachable();
	} else {
		// First compile the initExpression
		std::shared_ptr<PlainOldCodeStatement> initStmt =
		    std::make_shared<PlainOldCodeStatement>(stmt->initExpression + ";");
		initStmt->firstSourceLine = stmt->firstSourceLine;
		CompileStatement(initStmt, cx);

		// fullBody = { if (!(condExpression)) break; body; }
		std::shared_ptr<Statement> fullBody;

		if (noCondition) {
			fullBody = stmt->body;
		} else {
			// Create if statement for the condition check
			std::shared_ptr<BreakStatement> breakStmt = std::make_shared<BreakStatement>();
			breakStmt->firstSourceLine = stmt->firstSourceLine;

			std::shared_ptr<IfStatement> ifStmt =
			    std::make_shared<IfStatement>("!(" + stmt->condExpression + ")", false, breakStmt, nullptr);
			ifStmt->firstSourceLine = stmt->firstSourceLine;

			// Create a code block containing the if statement followed by the loop body
			std::shared_ptr<CodeBlock> codeBlock = std::make_shared<CodeBlock>();
			codeBlock->firstSourceLine = stmt->firstSourceLine;

			// Convert the body to a CodeBlock
			std::shared_ptr<CodeBlock> bodyAsBlock = AsCodeBlock(stmt->body);

			// Concatenate the if statement with the body statements
			std::vector<std::shared_ptr<Statement>> allStatements = { ifStmt };
			allStatements.insert(allStatements.end(), bodyAsBlock->statements.begin(), bodyAsBlock->statements.end());

			codeBlock->statements = allStatements;
			fullBody = codeBlock;
		}

		Function loopF = getFunction(cx.target.value().name, "loopHead", { loopDepth });
		Function loopBody = getFunction(cx.target.value().name, "loopBody", { loopDepth });
		Function breakF = getFunction(cx.target.value().name, "break", { loopDepth });
		Function continueF =
		    stmt->nextExpression.empty() ? loopF : getFunction(cx.target.value().name, "continue", { loopDepth });

		// TODO: Could we use EmitNativeLoop() here?
		loopF.WriteLine("int oldLoopDepth = ++loopDepth;");
		loopF.WriteLine(std::format("while (loopDepth == oldLoopDepth) loopDepth = {0};", loopBody.call("loopDepth")));

		Context loopContext = cx.LoopContext(loopBody, breakF, continueF, +1);
		auto result = Compile(AsCodeBlock(fullBody), loopContext, true);
		std::optional<Function> endLoop = result.target;

		if (endLoop.has_value() && endLoop.value() != loopBody) {
			if (!stmt->nextExpression.empty()) {
				std::shared_ptr<PlainOldCodeStatement> nextStmt =
				    std::make_shared<PlainOldCodeStatement>(stmt->nextExpression + ";");
				nextStmt->firstSourceLine = stmt->firstSourceLine;
				CompileStatement(nextStmt, cx.WithTarget(endLoop.value()));
			}
			endLoop->WriteLine(std::format("if (loopDepth == 0) return {0};", loopF.call("0")));
		}

		cx.target.value().WriteLine(std::format("loopDepth = {0};", loopF.call("loopDepth")));

		if (continueF != loopF && continueF.wasCalled) {
			std::shared_ptr<PlainOldCodeStatement> nextStmt =
			    std::make_shared<PlainOldCodeStatement>(stmt->nextExpression + ";");
			nextStmt->firstSourceLine = stmt->firstSourceLine;
			CompileStatement(nextStmt, cx.WithTarget(continueF));
			continueF.WriteLine(std::format("if (loopDepth == 0) return {0};", loopF.call("0")));
		}

		if (breakF.wasCalled) {
			auto newCx = cx.WithTarget(breakF);
			TryCatch(newCx, cx.catchFErr, cx.tryLoopDepth, [&]() {
				breakF.WriteLine(std::format("return {0};", cx.next.value().call("loopDepth")));
			});
		} else {
			cx.unreachable();
		}
	}
}

std::string ActorCompiler::getIteratorName(Context cx) {
	std::string name = "RangeFor" + cx.target.value().name + "Iterator";
	if (iterators.find(name) == iterators.end())
		iterators[name] = 0;
	return std::format("{0}{1}", name, iterators[name]++);
}

void ActorCompiler::CompileStatement(const std::shared_ptr<RangeForStatement> stmt, Context& cx) {
	// If stmt does not contain a wait statement, rewrite the original c++11 range-based for loop
	// If there is a wait, we need to rewrite the loop as:
	// for(a:b) c; ==> for(__iter=std::begin(b); __iter!=std::end(b); ++__iter) { a = *__iter; c; }
	// where __iter is stored as a state variable
	if (WillContinue(stmt->body)) {
		StateVar* container = nullptr;
		for (auto& s : state) {
			if (s.name == stmt->rangeExpression) {
				container = &s;
				break;
			}
		}

		if (container == nullptr) {
			throw Error(stmt->firstSourceLine,
			            "container of range-based for with continuation must be a state variable");
		}

		std::string iter = getIteratorName(cx);
		StateVar newState;
		newState.sourceLine = stmt->firstSourceLine;
		newState.name = iter;
		newState.type = "decltype(std::begin(std::declval<" + container->type + ">()))";
		state.push_back(newState);

		std::shared_ptr<ForStatement> equivalent =
		    std::make_shared<ForStatement>(iter + " = std::begin(" + stmt->rangeExpression + ")",
		                                   iter + " != std::end(" + stmt->rangeExpression + ")",
		                                   "++" + iter,
		                                   nullptr);
		equivalent->firstSourceLine = stmt->firstSourceLine;

		std::shared_ptr<CodeBlock> block;
		std::vector<std::shared_ptr<Statement>> statements;

		std::shared_ptr<PlainOldCodeStatement> codeStmt =
		    std::make_shared<PlainOldCodeStatement>(stmt->rangeDecl + " = *" + iter + ";");
		codeStmt->firstSourceLine = stmt->firstSourceLine;
		statements.push_back(codeStmt);
		statements.push_back(stmt->body);

		block->statements = statements;
		equivalent->body = block;

		CompileStatement(equivalent, cx);
	} else {
		EmitNativeLoop(
		    stmt->firstSourceLine, "for( " + stmt->rangeDecl + " : " + stmt->rangeExpression + " )", stmt->body, cx);
	}
}

void ActorCompiler::CompileStatement(const std::shared_ptr<WhileStatement> stmt, Context& cx) {
	// Compile while (x) { y } as for(;x;) { y }
	auto equivalent = std::make_shared<ForStatement>("", stmt->expression, "", stmt->body);
	equivalent->firstSourceLine = stmt->firstSourceLine;

	CompileStatement(equivalent, cx);
}

void ActorCompiler::CompileStatement(const std::shared_ptr<LoopStatement> stmt, Context& cx) {
	// Compile loop { body } as for(;;;) { body }
	auto equivalent = std::make_shared<ForStatement>("", "", "", stmt->body);
	equivalent->firstSourceLine = stmt->firstSourceLine;

	CompileStatement(equivalent, cx);
}

// Writes out a loop in native C++ (with no continuation passing)
// Returns true if the loop is known to have no normal exit (is unreachable)
bool ActorCompiler::EmitNativeLoop(int sourceLine,
                                   const std::string& head,
                                   std::shared_ptr<Statement> body,
                                   Context& cx) {
	LineNumber(cx.target.value(), sourceLine);
	cx.target.value().WriteLine(head + " {");
	cx.target.value().Indent(+1);

	LiteralBreak literalBreak;
	Compile(AsCodeBlock(body), cx.LoopContext(cx.target.value(), literalBreak, LiteralContinue(), 0), true);

	cx.target.value().Indent(-1);
	cx.target.value().WriteLine("}");

	return !literalBreak.wasCalled;
}

void ActorCompiler::CompileStatement(const std::shared_ptr<BreakStatement>& stmt, Context& cx) {
	if (!cx.breakF.has_value())
		throw Error(stmt->firstSourceLine, "break outside loop");

	if (dynamic_cast<LiteralBreak*>(&cx.breakF.value())) {
		cx.target.value().WriteLine(cx.breakF.value().call() + ";");
	} else {
		cx.target.value().WriteLine(std::format("return {0}; // break", cx.breakF->call("loopDepth==0?0:loopDepth-1")));
	}
	cx.unreachable();
}

void ActorCompiler::CompileStatement(const std::shared_ptr<ContinueStatement>& stmt, Context& cx) {
	if (!cx.continueF.has_value())
		throw Error(stmt->firstSourceLine, "continue outside loop");

	if (dynamic_cast<LiteralContinue*>(&cx.continueF.value())) {
		cx.target->WriteLine(cx.continueF->call() + ";");
	} else {
		cx.target->WriteLine(std::format("return {0}; // continue", cx.continueF->call("loopDepth")));
	}
	cx.unreachable();
}

void ActorCompiler::CompileStatement(const std::shared_ptr<WaitStatement>& stmt, Context& cx) {
	std::vector<std::shared_ptr<Statement>> statements;
	std::shared_ptr<WhenStatement> whenStmt = std::make_shared<WhenStatement>(stmt, nullptr);
	whenStmt->firstSourceLine = stmt->firstSourceLine;
	statements.push_back(whenStmt);

	std::shared_ptr<ChooseStatement> equiv = std::make_shared<ChooseStatement>(std::make_shared<CodeBlock>(statements));
	equiv->firstSourceLine = stmt->firstSourceLine;

	if (!stmt->resultIsState) {
		cx.next.value().formalParameters =
		    std::vector<std::string>{ std::string(stmt->result.type) + " const& " + stmt->result.name + loopDepth };
		cx.next.value().addOverload(std::string(stmt->result.type) + " && " + stmt->result.name + loopDepth);
	}
	CompileStatement(equiv, cx);
}

void ActorCompiler::CompileStatement(const std::shared_ptr<CodeBlock>& stmt, Context& cx) {
	cx.target.value().WriteLine("{");
	cx.target.value().Indent(+1);
	Context end = Compile(stmt, cx, true);
	cx.target.value().Indent(-1);
	cx.target.value().WriteLine("}");

	if (!end.target.has_value())
		cx.unreachable();
	else if (end.target.value() != cx.target.value())
		end.target->WriteLine(std::format("loopDepth = {0};", cx.next->call("loopDepth")));
}

void ActorCompiler::CompileStatement(const std::shared_ptr<ReturnStatement>& stmt, Context& cx) {
	LineNumber(cx.target.value(), stmt->firstSourceLine);

	if ((stmt->expression == "") != (actor.returnType.empty()))
		throw Error(stmt->firstSourceLine, "Return statement does not match actor declaration");

	if (!actor.returnType.empty()) {
		if (stmt->expression == "Never()") {
			// `return Never();` destroys state immediately but never returns to the caller
			cx.target->WriteLine(std::format("this->~{0}();", stateClassName));
			cx.target->WriteLine(std::format("{0}->sendAndDelPromiseRef(Never());", This));
		} else {
			// Short circuit if there are no futures outstanding, but still evaluate the expression
			// if it has side effects
			cx.target->WriteLine(
			    std::format("if (!{0}->SAV<{1}>::futures) {{ (void)({2}); this->~{3}(); {0}->destroy(); return 0; }}",
			                This,
			                actor.returnType,
			                stmt->expression,
			                stateClassName));

			// Build the return value directly in SAV<T>::value_storage
			// If the expression is exactly the name of a state variable, std::move() it
			auto it =
			    std::find_if(state.begin(), state.end(), [&](const auto& s) { return s.name == stmt->expression; });
			if (it != state.end()) {
				cx.target->WriteLine(
				    std::format("new (&{0}->SAV< {1} >::value()) {1}(std::move({2})); // state_var_RVO",
				                This,
				                actor.returnType,
				                stmt->expression));
			} else {
				cx.target->WriteLine(
				    std::format("new (&{0}->SAV< {1} >::value()) {1}({2});", This, actor.returnType, stmt->expression));
			}

			// Destruct state
			cx.target->WriteLine("this->~" + stateClassName + "();");
			// Tell SAV<T> to return the value we already constructed in value_storage
			cx.target->WriteLine(This + "->finishSendAndDelPromiseRef();");
		}
	} else {
		cx.target->WriteLine("delete " + This + ";");
	}

	cx.target->WriteLine("return 0;");
	cx.unreachable();
}

void ActorCompiler::CompileStatement(const std::shared_ptr<IfStatement>& stmt, Context& cx) {
	bool useContinuation = WillContinue(stmt->ifBody) || WillContinue(stmt->elseBody);
	LineNumber(cx.target.value(), stmt->firstSourceLine);

	assert(cx.target.has_value());
	cx.target.value().WriteLine(std::format("if {}({})", stmt->_constexpr ? "constexpr " : "", stmt->expression));
	cx.target.value().WriteLine("{");
	cx.target.value().Indent(+1);

	std::optional<Function> ifTarget = Compile(AsCodeBlock(stmt->ifBody), cx, useContinuation).target;
	if (useContinuation && ifTarget.has_value()) {
		ifTarget->WriteLine(std::format("loopDepth = {0};", cx.next->call("loopDepth")));
	}
	cx.target->Indent(-1);
	cx.target->WriteLine("}");

	std::optional<Function> elseTarget;
	if (stmt->elseBody != nullptr || useContinuation) {
		cx.target->WriteLine("else");
		cx.target->WriteLine("{");
		cx.target->Indent(+1);
		elseTarget = cx.target;

		if (stmt->elseBody != nullptr) {
			elseTarget = Compile(AsCodeBlock(stmt->elseBody), cx, useContinuation).target;
		}

		if (useContinuation && elseTarget.has_value())
			elseTarget.value().WriteLine("loopDepth = " + cx.next->call("loopDepth") + ";");

		cx.target->Indent(-1);
		cx.target->WriteLine("}");
	}

	if (!ifTarget.has_value() && stmt->elseBody != nullptr && !elseTarget.has_value())
		cx.unreachable();
	else if (!cx.next.value().wasCalled && useContinuation) {
		assert(false);
		throw std::runtime_error("Internal error: IfStatement: next not called?");
	}
}

void ActorCompiler::CompileStatement(const std::shared_ptr<TryStatement>& stmt, Context& cx) {
	bool reachable = false;
	if (stmt->catches.size() != 1)
		throw Error(stmt->firstSourceLine, "try statement must have exactly one catch clause");

	auto& c = stmt->catches[0];
	std::string catchErrorParameterName = "";

	if (c.expression != "...") {
		std::string exp = c.expression;
		exp.erase(std::remove(exp.begin(), exp.end(), ' '), exp.end());

		if (exp.substr(0, 6) != "Error&")
			throw Error(c.firstSourceLine, "Only type 'Error' or '...' may be caught in an actor function");

		catchErrorParameterName = exp.substr(6);
	}

	if (catchErrorParameterName == "")
		catchErrorParameterName = "__current_error";

	Function catchFErr =
	    getFunction(cx.target->name, "Catch", { "const Error& " + catchErrorParameterName }, { loopDepth0 });
	catchFErr.exceptionParameterIs = catchErrorParameterName;
	Context catchCx = cx.WithCatch(catchFErr);

	Context end = TryCatchCompile(AsCodeBlock(stmt->tryBody), catchCx);
	if (end.target.has_value()) {
		reachable = true;
		TryCatch(end, cx.catchFErr, cx.tryLoopDepth, [&]() {
			end.target->WriteLine("loopDepth = " + cx.next->call("loopDepth") + ";");
		});
	}

	// Now to write the catch function
	Context catchFErrCx = cx.WithTarget(catchFErr);
	TryCatch(catchFErrCx, cx.catchFErr, cx.tryLoopDepth, [&]() {
		Context cend = Compile(AsCodeBlock(c.body), cx.WithTarget(catchFErr), true);
		if (cend.target.has_value()) {
			cend.target->WriteLine("loopDepth = " + cx.next->call("loopDepth") + ";");
			reachable = true;
		}
	});

	if (!reachable)
		cx.unreachable();
}

void ActorCompiler::CompileStatement(const std::shared_ptr<ThrowStatement>& stmt, Context& cx) {
	LineNumber(cx.target.value(), stmt->firstSourceLine);

	if (stmt->expression == "") {
		if (!cx.target.value().exceptionParameterIs.empty()) {
			cx.target->WriteLine(
			    "return " +
			    cx.catchFErr->call(cx.target.value().exceptionParameterIs, AdjustLoopDepth(cx.tryLoopDepth)) + ";");
		} else {
			throw Error(stmt->firstSourceLine, "throw statement with no expression has no current exception in scope");
		}
	} else {
		cx.target->WriteLine("return " + cx.catchFErr->call(stmt->expression, AdjustLoopDepth(cx.tryLoopDepth)) + ";");
	}

	cx.unreachable();
}

void ActorCompiler::CompileStatement(const std::shared_ptr<Statement>& stmt, const Context& cx) {
	// Since C++ doesn't have reflection like C#, we need a different approach
	// Using a visitor pattern or function dispatch map

	// TODO: missing cases
	if (auto ifStmt = dynamic_pointer_cast<IfStatement>(stmt)) {
		CompileStatement(ifStmt, cx);
	} else if (auto tryStmt = dynamic_pointer_cast<TryStatement>(stmt)) {
		CompileStatement(tryStmt, cx);
	} else if (auto throwStmt = dynamic_pointer_cast<ThrowStatement>(stmt)) {
		CompileStatement(throwStmt, cx);
	} else if (auto loopStmt = dynamic_pointer_cast<LoopStatement>(stmt)) {
		CompileStatement(loopStmt, cx);
	} else if (auto chooseStmt = dynamic_pointer_cast<ChooseStatement>(stmt)) {
		CompileStatement(chooseStmt, cx);
	} else if (auto whileStmt = dynamic_pointer_cast<WhileStatement>(stmt)) {
		CompileStatement(whileStmt, cx);
	} else if (auto forStmt = dynamic_pointer_cast<ForStatement>(stmt)) {
		CompileStatement(forStmt, cx);
	} else if (auto forStmt = dynamic_pointer_cast<PlainOldCodeStatement>(stmt)) {
		CompileStatement(forStmt, cx);
	} else if (auto forStmt = dynamic_pointer_cast<StateDeclarationStatement>(stmt)) {
		CompileStatement(forStmt, cx);
	} else if (auto rangeForStmt = dynamic_pointer_cast<RangeForStatement>(stmt)) {
		CompileStatement(rangeForStmt, cx);
	} else if (auto whenStmt = dynamic_pointer_cast<WhenStatement>(stmt)) {
		CompileStatement(whenStmt, cx);
	} else if (auto returnStmt = dynamic_pointer_cast<ReturnStatement>(stmt)) {
		CompileStatement(returnStmt, cx);
	} else if (auto waitStmt = dynamic_pointer_cast<WaitStatement>(stmt)) {
		CompileStatement(waitStmt, cx);
	} else if (auto contStmt = dynamic_pointer_cast<ContinueStatement>(stmt)) {
		CompileStatement(contStmt, cx);
	} else if (auto breakStmt = dynamic_pointer_cast<BreakStatement>(stmt)) {
		CompileStatement(breakStmt, cx);
	} else if (auto cb = dynamic_pointer_cast<CodeBlock>(stmt)) {
		CompileStatement(cb, cx);
	} else {
		throw Error(stmt->firstSourceLine, std::format("Statement type {} not supported yet.", stmt->toString()));
	}
}

Context ActorCompiler::Compile(std::shared_ptr<CodeBlock> block, const Context& context, bool okToContinue) {
	Context cx = context.Clone();
	cx.next = std::nullopt;

	for (auto& stmt : block->statements) {
		if (cx.target == std::nullopt) {
			throw Error(stmt->firstSourceLine, "Unreachable code.");
			// std::cerr << "\t(WARNING) Unreachable code at line " << stmt->firstSourceLine << "." << std::endl;
			// break;
		}

		if (cx.next == std::nullopt) {
			cx.next = getFunction(cx.target.value().name, "cont", { loopDepth });
		}

		CompileStatement(stmt, cx);

		if (cx.next.value().wasCalled) {
			if (cx.target == std::nullopt) {
				assert(false);
				throw std::runtime_error("Unreachable continuation called?");
			}
			if (!okToContinue) {
				assert(false);
				throw std::runtime_error("Unexpected continuation");
			}
			cx.target = cx.next;
			cx.next = std::nullopt;
		}
	}

	return cx;
}

void ActorCompiler::WriteFunctions(std::ostream& writer) {
	for (auto& [name, func] : functions) {
		std::string body = func.BodyText();
		if (!body.empty()) {
			WriteFunction(writer, func, body);
		}

		if (func.overload != nullptr) {
			std::string overloadBody = func.overload->BodyText();
			if (!overloadBody.empty()) {
				WriteFunction(writer, *func.overload, overloadBody);
			}
		}
	}
}

void ActorCompiler::WriteFunction(std::ostream& writer, Function& func, const std::string& body) {
	// Construct the function signature
	std::string returnTypeStr = func.returnType.empty() ? "" : func.returnType + " ";
	std::string parametersStr = join(func.formalParameters, ",");
	std::string specifiersStr = func.specifiers.empty() ? "" : " " + func.specifiers;

	writer << memberIndentStr << returnTypeStr << func.useByName() << "(" << parametersStr << ")" << specifiersStr
	       << std::endl;

	if (!func.returnType.empty()) {
		writer << memberIndentStr << "{" << std::endl;
	}

	writer << body << std::endl;

	if (!func.endIsUnreachable) {
		writer << memberIndentStr << "\treturn loopDepth;" << std::endl;
	}

	writer << memberIndentStr << "}" << std::endl;
}

Function ActorCompiler::getFunction(const std::string& baseName,
                                    const std::string& addName,
                                    const std::vector<std::string>& formalParameters,
                                    const std::vector<std::string>& overloadFormalParameters) {
	std::string proposedName;
	if (addName == "cont" && baseName.length() >= 5 && baseName.substr(baseName.length() - 5, 4) == "cont") {
		proposedName = baseName.substr(0, baseName.length() - 1);
	} else {
		proposedName = baseName + addName;
	}

	int i = 0;
	std::string functionName;
	do {
		i++;
		functionName = proposedName + std::to_string(i);
	} while (functions.find(functionName) != functions.end());

	Function f;
	f.name = functionName;
	f.returnType = "int";
	f.formalParameters = formalParameters;

	if (!overloadFormalParameters.empty()) {
		f.addOverload(overloadFormalParameters);
	}

	f.Indent(codeIndent);
	functions[f.name] = f;
	return f;
}

Function ActorCompiler::getFunction(const std::string& baseName,
                                    const std::string& addName,
                                    const std::vector<std::string>& formalParameters) {
	return getFunction(baseName, addName, formalParameters, {});
}

std::vector<std::string> ActorCompiler::ParameterList() {
	std::vector<std::string> result;
	for (const auto& p : actor.parameters) {
		std::string paramDecl;
		// SOMEDAY: pass small built in types by value
		if (!p.initializer.empty()) {
			paramDecl = p.type + " const& " + p.name + " = " + p.initializer;
		} else {
			paramDecl = p.type + " const& " + p.name;
		}
		result.push_back(paramDecl);
	}
	return result;
}

void ActorCompiler::WriteCancelFunc(std::ostream& writer) {
	if (actor.isCancellable()) {
		Function cancelFunc;
		cancelFunc.name = "cancel";
		cancelFunc.returnType = "void";
		cancelFunc.endIsUnreachable = true;
		cancelFunc.publicName = true;
		cancelFunc.specifiers = "override";

		cancelFunc.Indent(codeIndent);
		cancelFunc.WriteLine("auto wait_state = this->actor_wait_state;");
		cancelFunc.WriteLine("this->actor_wait_state = -1;");
		cancelFunc.WriteLine("switch (wait_state) {");

		int lastGroup = -1;

		// Sort callbacks by CallbackGroup and process them
		// This assumes you've implemented a sorting mechanism in C++
		std::vector<CallbackVar> sortedCallbacks = callbacks;
		std::sort(sortedCallbacks.begin(), sortedCallbacks.end(), [](const CallbackVar& a, const CallbackVar& b) {
			return a.callbackGroup < b.callbackGroup;
		});

		for (const auto& cb : sortedCallbacks) {
			if (cb.callbackGroup != lastGroup) {
				lastGroup = cb.callbackGroup;
				std::string line = "case " + std::to_string(cb.callbackGroup) + ": this->a_callback_error((" + cb.type +
				                   "*)0, actor_cancelled()); break;";
				cancelFunc.WriteLine(line);
			}
		}

		cancelFunc.WriteLine("}");
		WriteFunction(writer, cancelFunc, cancelFunc.BodyText());
	}
}

void ActorCompiler::WriteConstructor(Function& body, std::ostream& writer, const std::string& fullStateClassName) {
	Function constructor;
	constructor.name = className;
	constructor.returnType = "";
	constructor.endIsUnreachable = true;
	constructor.publicName = true;

	// Initializes class member variables
	constructor.Indent(codeIndent);

	std::string returnType = actor.returnType.empty() ? "void" : actor.returnType;
	constructor.WriteLine(" : Actor<" + returnType + ">(),");

	// Create parameter list from actor.parameters
	std::vector<std::string> paramNames;
	for (const auto& p : actor.parameters) {
		paramNames.push_back(p.name);
	}
	std::string paramList = join(paramNames, ", ");

	constructor.WriteLine(" " + fullStateClassName + "(" + paramList + "),");
	constructor.WriteLine(" activeActorHelper(__actorIdentifier)");

	constructor.Indent(-1);
	constructor.WriteLine("{");
	constructor.Indent(+1);

	ProbeEnter(constructor, actor.name);

	constructor.WriteLine("#ifdef ENABLE_SAMPLING");
	constructor.WriteLine("this->lineage.setActorName(\"" + actor.name + "\");");
	constructor.WriteLine("LineageScope _(&this->lineage);");
	// constructor.WriteLine("getCurrentLineage()->modify(&StackLineage::actorName) = \"" + actor.name + "\"_sr;");
	constructor.WriteLine("#endif");

	constructor.WriteLine("this->" + body.call() + ";");

	ProbeExit(constructor, actor.name);

	WriteFunction(writer, constructor, constructor.BodyText());
}

void ActorCompiler::WriteStateConstructor(std::ostream& writer) {
	Function constructor;
	constructor.name = stateClassName;
	constructor.returnType = "";
	constructor.endIsUnreachable = true;
	constructor.publicName = true;

	constructor.Indent(codeIndent);

	std::string ini; // Initialize as empty string
	int line = actor.sourceLine;

	for (const auto& s : state) {
		if (!s.initializer.empty()) {
			LineNumber(constructor, line);

			if (!ini.empty()) {
				constructor.WriteLine(ini + ",");
				ini = "   ";
			} else {
				ini = " : ";
			}

			ini += s.name + "(" + s.initializer + ")";
			line = s.sourceLine;
		}
	}

	LineNumber(constructor, line);
	if (!ini.empty()) {
		constructor.WriteLine(ini);
	}

	constructor.Indent(-1);
	constructor.WriteLine("{");
	constructor.Indent(1);

	ProbeCreate(constructor, actor.name);
	WriteFunction(writer, constructor, constructor.BodyText());
}

void ActorCompiler::WriteStateDestructor(std::ostream& writer) {
	Function destructor;
	destructor.name = "~" + stateClassName;
	destructor.returnType = "";
	destructor.endIsUnreachable = true;
	destructor.publicName = true;

	destructor.Indent(codeIndent);
	destructor.Indent(-1);
	destructor.WriteLine("{");
	destructor.Indent(+1);
	ProbeDestroy(destructor, actor.name);
	WriteFunction(writer, destructor, destructor.BodyText());
}

std::vector<std::shared_ptr<Statement>> ActorCompiler::Flatten(const std::shared_ptr<Statement>& stmt) {
	if (!stmt)
		return {};

	std::vector<std::shared_ptr<Statement>> result;
	std::vector<std::shared_ptr<Statement>> flattened;

	// Handle different statement types
	if (auto loop = std::dynamic_pointer_cast<LoopStatement>(stmt)) {
		flattened = Flatten(loop->body);
	} else if (auto whileStmt = std::dynamic_pointer_cast<WhileStatement>(stmt)) {
		flattened = Flatten(whileStmt->body);
	} else if (auto forStmt = std::dynamic_pointer_cast<ForStatement>(stmt)) {
		flattened = Flatten(forStmt->body);
	} else if (auto rangeForStmt = std::dynamic_pointer_cast<RangeForStatement>(stmt)) {
		flattened = Flatten(rangeForStmt->body);
	} else if (auto codeBlock = std::dynamic_pointer_cast<CodeBlock>(stmt)) {
		for (const auto& s : codeBlock->statements) {
			auto nested = Flatten(s);
			flattened.insert(flattened.end(), nested.begin(), nested.end());
		}
	} else if (auto ifStmt = std::dynamic_pointer_cast<IfStatement>(stmt)) {
		auto ifFlattened = Flatten(ifStmt->ifBody);
		auto elseFlattened = Flatten(ifStmt->elseBody);

		flattened.insert(flattened.end(), ifFlattened.begin(), ifFlattened.end());
		flattened.insert(flattened.end(), elseFlattened.begin(), elseFlattened.end());
	} else if (auto chooseStmt = std::dynamic_pointer_cast<ChooseStatement>(stmt)) {
		flattened = Flatten(chooseStmt->body);
	} else if (auto whenStmt = std::dynamic_pointer_cast<WhenStatement>(stmt)) {
		flattened = Flatten(whenStmt->body);
	} else if (auto tryStmt = std::dynamic_pointer_cast<TryStatement>(stmt)) {
		auto tryFlattened = Flatten(tryStmt->tryBody);
		flattened.insert(flattened.end(), tryFlattened.begin(), tryFlattened.end());

		for (const auto& catchClause : tryStmt->catches) {
			auto catchFlattened = Flatten(catchClause.body);
			flattened.insert(flattened.end(), catchFlattened.begin(), catchFlattened.end());
		}
	}
	// Default case for any other Statement type

	// Add the statement itself first, then the flattened children
	result.push_back(stmt);
	result.insert(result.end(), flattened.begin(), flattened.end());

	return result;
}

std::string FormatString(const std::string& format, const std::vector<std::string>& args) {
	std::string result = format;
	for (size_t i = 0; i < args.size(); i++) {
		std::string placeholder = "{" + std::to_string(i) + "}";
		size_t pos = result.find(placeholder);
		while (pos != std::string::npos) {
			result.replace(pos, placeholder.length(), args[i]);
			pos = result.find(placeholder);
		}
	}
	return result;
}

void ActorCompiler::Write(std::ostream& writer) {
	std::string fullReturnType = actor.returnType.empty() ? "void" : "Future<" + actor.returnType + ">";
	for (int i = 0;; i++) {
		className = actor.name.substr(0, 1);
		std::transform(className.begin(), className.end(), className.begin(), ::toupper);
		className += actor.name.substr(1) + (i != 0 ? std::to_string(i) : "");

		if (!actor.enclosingClass.empty() && actor.isForwardDeclaration) {
			std::string prefix = actor.enclosingClass;
			std::replace(prefix.begin(), prefix.end(), ':', '_');
			className = prefix + "_" + className;
		} else if (!actor.nameSpace.empty()) {
			std::string prefix = actor.nameSpace;
			std::replace(prefix.begin(), prefix.end(), ':', '_');
			className = prefix + "_" + className;
		}

		if (actor.isForwardDeclaration || usedClassNames.insert(className).second)
			break;
	}
	// e.g. SimpleTimerActor
	fullClassName = className + GetTemplateActuals({});
	VarDeclaration actorClassFormal;
	actorClassFormal.name = className;
	actorClassFormal.type = "class";

	This = "static_cast<" + actorClassFormal.name + "*>(this)";
	// e.g. SimpleTimerActorState
	stateClassName = className + "State";
	// e.g. SimpleTimerActorState<SimpleTimerActor>
	VarDeclaration tempVar;
	tempVar.type = "class";
	tempVar.name = fullClassName;
	std::string fullStateClassName = stateClassName + GetTemplateActuals({ tempVar });

	if (actor.isForwardDeclaration) {
		for (const std::string& attribute : actor.attributes) {
			writer << attribute << " ";
		}
		if (actor.isStatic)
			writer << "static ";
		writer << fullReturnType << " " << (actor.nameSpace.empty() ? "" : actor.nameSpace + "::") << actor.name << "( "
		       << join(ParameterList(), ", ") << " );\n";

		if (!actor.enclosingClass.empty()) {
			writer << "template <class> friend class " << stateClassName << ";\n";
		}
		return;
	}

	auto body = getFunction("", "body", { loopDepth0 });
	Context bodyContext;
	bodyContext.target = body;
	bodyContext.catchFErr = getFunction(body.name, "Catch", { "Error error" }, { loopDepth0 });

	auto endContext = TryCatchCompile(actor.body, bodyContext);
	if (endContext.target.has_value()) {
		if (actor.returnType.empty()) {
			auto returnStmt = std::make_shared<ReturnStatement>("");
			returnStmt->firstSourceLine = actor.sourceLine;
			CompileStatement(returnStmt, endContext);
		} else {
			throw Error(actor.sourceLine, std::format("Actor {} fails to return a value", actor.name));
		}
	}

	if (!actor.returnType.empty()) {
		bodyContext.catchFErr->WriteLine("this->~" + stateClassName + "();");
		bodyContext.catchFErr->WriteLine(This + "->sendErrorAndDelPromiseRef(error);");
	} else {
		bodyContext.catchFErr->WriteLine("delete " + This + ";");
	}

	bodyContext.catchFErr->WriteLine("loopDepth = 0;");

	if (isTopLevel && actor.nameSpace.empty())
		writer << "namespace {\n";

	// The "State" class contains all state and user code, to make sure that state names are accessible to user code but
	// inherited members of Actor, Callback etc are not.
	writer << "// This generated class is to be used only via " << actor.name << "()\n";
	WriteTemplate(writer, { actorClassFormal });
	LineNumber(writer, actor.sourceLine);
	writer << "class " << stateClassName << " {\n";
	writer << "public:\n";
	LineNumber(writer, actor.sourceLine);
	WriteStateConstructor(writer);
	WriteStateDestructor(writer);
	WriteFunctions(writer);

	for (const auto& st : state) {
		LineNumber(writer, st.sourceLine);
		writer << "\t" << st.type << " " << st.name << ";\n";
	}

	writer << "};\n";
	WriteActorClass(writer, fullStateClassName, body);

	if (isTopLevel && actor.nameSpace.empty())
		writer << "} // namespace\n";

	WriteActorFunction(writer, fullReturnType);

	if (!actor.testCaseParameters.empty()) {
		writer << "ACTOR_TEST_CASE(" << actor.name << ", " << actor.testCaseParameters << ")\n";
	}

	std::cout << "\tCompiled ACTOR " << actor.name << " (line " << actor.sourceLine << ")\n";
}

void ActorCompiler::ProbeEnter(Function& fun, const std::string& name, int index) {
	if (generateProbes) {
		fun.WriteLine("fdb_probe_actor_enter(\"" + name + "\", " + thisAddress + ", " + std::to_string(index) + ");");
	}

	auto blockIdentifier = GetUidFromString(fun.name);
	fun.WriteLine("#ifdef WITH_ACAC");
	fun.WriteLine("static constexpr ActorBlockIdentifier __identifier = UID(" + std::to_string(blockIdentifier.first) +
	              "UL, " + std::to_string(blockIdentifier.second) + "UL);");
	fun.WriteLine("ActorExecutionContextHelper __helper(static_cast<" + className +
	              "*>(this)->activeActorHelper.actorID, __identifier);");
	fun.WriteLine("#endif // WITH_ACAC");
}

void ActorCompiler::ProbeExit(Function& fun, const std::string& name, int index) {
	if (generateProbes) {
		fun.WriteLine("fdb_probe_actor_exit(\"" + name + "\", " + thisAddress + ", " + std::to_string(index) + ");");
	}
}

void ActorCompiler::ProbeCreate(Function& fun, const std::string& name) {
	if (generateProbes) {
		fun.WriteLine("fdb_probe_actor_create(\"" + name + "\", " + thisAddress + ");");
	}
}

void ActorCompiler::ProbeDestroy(Function& fun, const std::string& name) {
	if (generateProbes) {
		fun.WriteLine("fdb_probe_actor_destroy(\"" + name + "\", " + thisAddress + ");");
	}
}

void ActorCompiler::LineNumber(std::ostream& writer, int SourceLine) {
	if (SourceLine == 0) {
		assert(false);
		throw std::runtime_error("Internal error: Invalid source line (0)");
	}
	if (LineNumbersEnabled)
		writer << "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line " << SourceLine << " \"" << sourceFile << "\"";
}

void ActorCompiler::LineNumber(Function& writer, int SourceLine) {
	if (SourceLine == 0) {
		assert(false);
		throw std::runtime_error("Internal error: Invalid source line (0)");
	}
	if (LineNumbersEnabled) {
		writer.WriteLineUnindented(
		    std::format("\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} \"{1}\"", SourceLine, sourceFile));
	}
}

} // namespace actorcompiler
