# Step 5 Status: Code Generation Implementation

## What We've Done

### 1. Foundation Code Generation Infrastructure (COMPLETE)

#### Function Helper Class (`Function.h` / `Function.cpp`)
- **Purpose**: Manage generated code text with indentation and function call tracking
- **Key Methods**:
  - `indent()` / `dedent()`: Control indentation level
  - `writeLine(text)`: Write a line of code with proper indentation
  - `call(name, args...)`: Format function calls and track usage via `wasCalled` flag
  - Supports multiple overloads for variadic arguments

#### Context Helper Class (`Context.h` / `Context.cpp`)
- **Purpose**: Track continuation flow in actor state machines
- **Key Methods**:
  - `withTarget(label)`: Create context targeting a specific continuation label
  - `loopContext(break, continue)`: Create context for loop statements with break/continue labels
  - `withCatch(error, errorCode, handler)`: Create context for try/catch error handling
  - `clone()`: Duplicate context for branching control flow
  - Special `unreachableTarget()` singleton for terminal states

### 2. ActorCompiler Scaffolding (IN PROGRESS)

#### Current Implementation
- **Constructor**: Initializes actor metadata, derives class names, computes UID mappings via SHA256
- **Helper Functions**:
  - `bytesToU64()`: Convert byte array to 64-bit integer
  - `getUidFromString()`: Generate deterministic UID from string using OpenSSL SHA256
  - `join()`: Concatenate strings with separator
  - `paramList()`: Format parameter declarations with defaults
  - `writeTemplate()`: Emit template declarations with #line directives

#### Minimal write() Implementation (Scaffold Phase)
Currently emits:
1. Forward declarations for forward-declared actors (with friend template)
2. Stub actor wrapper function:
   - Template parameters (if any)
   - Attributes and static keyword
   - Return type as `Future<T>` or `void`
   - Parameter list with const references
   - Empty body with placeholder return
3. ACTOR_TEST_CASE macro invocation for test cases

**Status**: This is a minimal scaffold to verify structure. **Full implementation pending.**

### 3. DescrCompiler Implementation (BASIC COMPLETE)

#### Current Implementation
- Emits simple struct with fields and optional base classes
- Respects brace depth for proper indentation
- Handles field comments inline
- Tracks line count for caller

**Status**: Sufficient for basic DESCR blocks. May need extension for complex cases.

### 4. Build System Updates (COMPLETE)
- Added `Function.cpp` and `Context.cpp` to CMakeLists.txt sources
- Linked OpenSSL::Crypto for SHA256 support
- Updated parser smoke test to include Function and Context objects

## What Remains for Full Step 5 Completion

### Critical Code Generation Methods (TO IMPLEMENT)

Based on C# ActorCompiler reference, these methods drive the full actor state machine generation:

#### 1. State Discovery
```cpp
void FindState(Statement* stmt, std::set<std::string>& stateVars);
```
- Traverse AST to collect all `state` variable declarations
- Track variables that persist across `wait()` boundaries

#### 2. Function Infrastructure
```cpp
Function* getFunction(const std::string& label);
```
- Lazily create or retrieve continuation functions by label
- Manage a map of label → Function objects

#### 3. Statement Compilation Dispatch
```cpp
void Compile(Function* func, Statement* stmt, const Context& ctx);
void CompileStatement(Function* func, PlainOldCodeStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, StateDeclarationStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, ReturnStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, BreakStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, ContinueStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, WhileStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, ForStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, RangeForStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, LoopStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, IfStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, WaitStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, ChooseStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, TryStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, ThrowStatement* stmt, const Context& ctx);
void CompileStatement(Function* func, CodeBlock* stmt, const Context& ctx);
```
- Recursively compile each statement type into continuation-based code
- Use Context to track current target label and error handlers
- Generate `SAV<T>` assignments for waits
- Generate callback lambdas for choose/when
- Handle break/continue via `goto` to loop context labels

#### 4. Choose/When Callback Generation
```cpp
void CompileChoose(Function* func, ChooseStatement* stmt, const Context& ctx);
```
- For each `when` clause:
  - Generate callback lambda capturing state
  - Compile body in continuation context
  - Wire callbacks to `chooseTwoFuture` / `chooseThreeFuture` / etc.

#### 5. Try/Catch Wiring
```cpp
void TryCatchCompile(Function* func, TryStatement* stmt, const Context& ctx);
```
- Compile try body with catch context
- For each catch:
  - Generate catch handler function
  - Emit error code checks and variable binding
- Generate `a_body1catch1` style functions

#### 6. Wait Continuation
```cpp
void CompileWaitStatement(Function* func, WaitStatement* stmt, const Context& ctx);
```
- Emit `SAV<T>::addYieldedCallbackAndClear()` or `wait()` call
- Generate label for next continuation
- Compile subsequent statements in new function with that label

#### 7. Actor Class and Function Writers
```cpp
void writeActorFunction(std::ostream& writer, const std::string& fullReturnType);
void writeActorClass(std::ostream& writer, const std::string& fullStateClassName);
void WriteStateConstructor(std::ostream& writer);
void WriteStateDestructor(std::ostream& writer);
void WriteConstructor(std::ostream& writer);
void WriteCancelFunc(std::ostream& writer);
void WriteFunctions(std::ostream& writer);
void WriteFunction(std::ostream& writer, Function* func, int lineNumber);
```
- Emit actor wrapper function calling constructor
- Emit state class with:
  - Member variables: parameters + state vars
  - Constructor initializing members
  - Destructor (if cancellable)
  - `a_body1()` and continuation functions
  - Cancel function (if cancellable)
- Emit probe hooks (ProbeEnter, ProbeExit, ProbeCreate, ProbeDestroy) if `generateProbes` is true

#### 8. Template Handling
- Expand template formals into `<class T1, ...>` syntax
- Emit friend declarations for enclosing classes
- Handle full class names with template parameters

#### 9. Line Number Tracking
```cpp
int LineNumber(Function* func);
```
- Map function to source line for #line directives

### Testing Strategy

#### Incremental Smoke Tests
1. **Simple Actor**: Single wait, single return
   - Input: `ACTOR Future<int> foo() { int x = wait(getFuture()); return x; }`
   - Verify: State class created, wait generates callback, return emits `actor_wait_state`
2. **Choose/When**: Basic choose with two when clauses
3. **Try/Catch**: Actor with error handling
4. **Loop with Break**: Test loop context and goto generation
5. **State Variables**: Multiple state declarations and usage

#### Full Integration Test
- Run parser + codegen on a real FDB .actor.cpp snippet
- Compare structure to C# output (not exact match, but logical equivalence)

## Build and Test Plan

### Phase 1: Implement Core Dispatch
- Add `FindState()` and state discovery
- Implement `Compile()` dispatcher and simple statement compilers:
  - PlainOldCode
  - Return
  - Break/Continue
- Add test: single return actor

### Phase 2: Wait and Continuations
- Implement `CompileWaitStatement()`
- Implement `getFunction()` and label management
- Add test: actor with one wait and return

### Phase 3: Loops and Control Flow
- Implement loop statement compilers (While, For, Loop)
- Implement If/Else
- Add test: actor with loop and break

### Phase 4: Choose/When
- Implement `CompileChoose()`
- Generate callback lambdas
- Add test: actor with choose/when

### Phase 5: Try/Catch
- Implement `TryCatchCompile()`
- Add test: actor with try/catch

### Phase 6: Full Codegen Writers
- Implement `WriteActorClass()` and related methods
- Emit complete state class structure
- Emit actor wrapper function
- Add test: full end-to-end actor compilation

### Phase 7: Templates and Probes
- Handle template expansion
- Emit probe hooks
- Add test: template actor

## Current Compilation Status

- **Tokenizer**: ✅ Passing smoke test
- **Parser**: ✅ Passing smoke test
- **Codegen Helpers (Function/Context)**: ✅ Implemented
- **ActorCompiler Constructor**: ✅ Implemented (UID generation, class name setup)
- **ActorCompiler write() scaffold**: ⚠️ Minimal stub; needs full implementation
- **DescrCompiler**: ✅ Basic implementation complete
- **Build System**: ✅ Updated CMakeLists.txt

## Next Immediate Steps

1. **Verify Build**: Compile actorcompiler_cpp with current changes
   - Fix any remaining include/link errors
   - Ensure parser smoke test still passes

2. **Add State Discovery**: Implement `FindState()` to collect state variables from AST

3. **Implement getFunction()**: Add function registry and label-based retrieval

4. **Start Compile Dispatch**: Implement `Compile()` and simple statement compilers (PlainOldCode, Return)

5. **Add First Real Codegen Test**: Create a minimal actor and verify output structure

## Notes

- The current scaffold is intentionally minimal to avoid breaking the build
- Once the full codegen is implemented, we'll replace the stub `write()` body
- The C# ActorCompiler.cs serves as the reference; C++ port must maintain logic parity
- OpenSSL SHA256 is now linked and ready for UID generation
- Function and Context classes are ready to use in codegen methods

---

**Status Summary**: Foundation complete; core codegen dispatch and continuation logic remain to implement.
