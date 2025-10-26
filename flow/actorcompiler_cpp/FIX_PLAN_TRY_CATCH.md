# Fix Plan: Try/Catch Error Handling for C++ Actor Compiler

## Problem Summary

The C++ actor compiler generates incorrect code for actors with try/catch blocks. Comparison with the C# reference implementation reveals critical differences in error handling strategy.

## Source Files

- **C# Reference**: `flow/actorcompiler/try_catch.actor.g.cpp`
- **C++ Output**: `flow/actorcompiler_cpp/try_catch.actor.g.cpp`
- **Test Actor**: `flow/actorcompiler_cpp/tests/runtime_test_actors/try_catch.actor.cpp`

## Source Actor Code

```cpp
ACTOR Future<int> tryCatch(Future<int> f) {
    try {
        state int x = wait(f);
        return x;
    } catch (Error& e) {
        return -1;
    }
}
```

## Detailed Comparison

### Issue 1: Goto-Based vs Method-Based Catch Compilation

**C# Reference Pattern** (CORRECT - lines 30-54):
```cpp
int a_body1(int loopDepth=0)
{
    try {
        // BEGIN try block
        try {
            StrictFuture<int> __when_expr_0 = f;
            if (static_cast<TryCatchActor*>(this)->actor_wait_state < 0)
                return a_body1Catch1(actor_cancelled(), loopDepth);
            if (__when_expr_0.isReady()) {
                if (__when_expr_0.isError())
                    return a_body1Catch1(__when_expr_0.getError(), loopDepth);
                else
                    return a_body1when1(__when_expr_0.get(), loopDepth);
            };
            static_cast<TryCatchActor*>(this)->actor_wait_state = 1;
            __when_expr_0.addCallbackAndClear(static_cast<ActorCallback< TryCatchActor, 0, int >*>(static_cast<TryCatchActor*>(this)));
            loopDepth = 0;
        }
        catch (Error& e) {
            goto cont1;
        }
        catch (...) {
            e = unknown_error();
            goto cont1;
        }

        cont1:
        {
            if (!static_cast<TryCatchActor*>(this)->SAV<int>::futures) { (void)(-1); this->~TryCatchActorState(); static_cast<TryCatchActor*>(this)->destroy(); return 0; }
            new (&static_cast<TryCatchActor*>(this)->SAV< int >::value()) int(std::move(-1)); // state_var_RVO
            this->~TryCatchActorState();
            static_cast<TryCatchActor*>(this)->finishSendAndDelPromiseRef();
            return 0;
        }
        // END try block
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

Wait, I need to re-read the C# reference more carefully. Let me check the actual structure again.

Actually, looking at lines 30-86 of the C# reference:

```cpp
int a_body1(int loopDepth=0)
{
    try {
        try {
            StrictFuture<int> __when_expr_0 = f;
            if (static_cast<TryCatchActor*>(this)->actor_wait_state < 0)
                return a_body1Catch2(actor_cancelled(), loopDepth);  // Routes to INNER catch
            if (__when_expr_0.isReady()) {
                if (__when_expr_0.isError())
                    return a_body1Catch2(__when_expr_0.getError(), loopDepth);  // Routes to INNER catch
                else
                    return a_body1when1(__when_expr_0.get(), loopDepth);
            };
            // ... callback setup
        }
        catch (Error& error) {
            loopDepth = a_body1Catch2(error, loopDepth);  // Routes to a_body1Catch2 METHOD
        } catch (...) {
            loopDepth = a_body1Catch2(unknown_error(), loopDepth);  // Routes to a_body1Catch2 METHOD
        }
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);  // Outer catch
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}

int a_body1Catch2(const Error& e,int loopDepth=0)   // SEPARATE METHOD for inner catch
{
    try {
        if (!static_cast<TryCatchActor*>(this)->SAV<int>::futures) {
            (void)(-1);
            this->~TryCatchActorState();
            static_cast<TryCatchActor*>(this)->destroy();
            return 0;
        }
        new (&static_cast<TryCatchActor*>(this)->SAV< int >::value()) int(-1);
        this->~TryCatchActorState();
        static_cast<TryCatchActor*>(this)->finishSendAndDelPromiseRef();
        return 0;
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);  // Can propagate to outer catch
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

**C++ Current Pattern** (INCORRECT - lines 27-71):
```cpp
int a_body1(int loopDepth=0)
{
    try {
        // BEGIN try block
        try {
            StrictFuture<int> __when_expr_0 = f;
            if (static_cast<TryCatchActor*>(this)->actor_wait_state < 0)
                return a_body1Catch1(actor_cancelled(), loopDepth);  // WRONG: routes to outer catch
            if (__when_expr_0.isReady()) {
                if (__when_expr_0.isError())
                    return a_body1Catch1(__when_expr_0.getError(), loopDepth);  // WRONG: routes to outer catch
                else
                    return a_body1when1(__when_expr_0.get(), loopDepth);
            };
            // ... callback setup
        }
        catch (Error& e) {
            goto cont1;  // PROBLEM 1: Uses goto instead of method call
        }
        catch (...) {
            e = unknown_error();  // PROBLEM 2: 'e' not declared as state variable
            goto cont1;
        }

        cont1:  // PROBLEM 3: Inline label instead of separate method
        {
            if (!static_cast<TryCatchActor*>(this)->SAV<int>::futures) {
                (void)(-1);
                this->~TryCatchActorState();
                static_cast<TryCatchActor*>(this)->destroy();
                return 0;
            }
            new (&static_cast<TryCatchActor*>(this)->SAV< int >::value()) int(std::move(-1));
            this->~TryCatchActorState();
            static_cast<TryCatchActor*>(this)->finishSendAndDelPromiseRef();
            return 0;
        }
        // END try block
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}

// PROBLEM 4: Missing a_body1Catch2 method entirely
```

**Root Cause**: In `ActorCompiler::compileStatement(TryStatement*)` (ActorCompiler.cpp:717-778), the implementation generates goto-based code with inline labels instead of creating a catch continuation method.

**Key Differences**:
1. C# generates `a_body1Catch2` method for inner catch handler
2. C# routes errors in try block to `a_body1Catch2`, not `a_body1Catch1`
3. C# wraps `a_body1Catch2` body in try-catch to allow propagation to outer handler
4. C++ uses goto/labels which can't be referenced from callbacks
5. C++ doesn't track the error variable from catch clause

### Issue 2: Callback Routing to Wrong Catch Handler

**C# Reference** (line 132):
```cpp
void a_callback_error(ActorCallback< TryCatchActor, 0, int >*,Error err)
{
    #ifdef WITH_ACAC
    // ...
    #endif // WITH_ACAC
    a_exitChoose1();
    try {
        a_body1Catch2(err, 0);  // Routes to INNER catch handler (Catch2)
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);  // Falls back to outer if Catch2 throws
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}
```

**C++ Current** (line 141):
```cpp
void a_callback_error(ActorCallback< TryCatchActor, 0, int >*,Error err)
{
    #ifdef WITH_ACAC
    // ...
    #endif // WITH_ACAC
    a_exitChoose1();
    try {
        a_body1Catch1(err, 0);  // WRONG: Routes to OUTER catch handler (Catch1)
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}
```

**Root Cause**: In `ActorCompiler::compileStatement(WaitStatement*)`, callbacks are set up with `cb.errorHandler = "a_body1Catch1"` unconditionally, without checking if there's an inner catch handler.

### Issue 3: Error Variable Not Declared as State Variable

**C# Reference** (line 151):
```cpp
class TryCatchActorState {
public:
    // ... constructor, methods ...

    Future<int> f;  // Parameter
    int x;          // State variable from wait
    Error e;        // ERROR VARIABLE from catch clause
};
```

Wait, let me re-check the C# reference. Looking at lines 150-152 in the summary...

Actually, I need to look at the full state class. Let me check what state variables the C# version has.

Looking at the structure, the error variable `e` is used in the catch block. In the C# version, when it catches `catch (Error& e)` or uses `e = unknown_error()`, this `e` needs to be accessible.

The C# version uses it as a parameter to `a_body1Catch2(const Error& e, ...)`, so it doesn't need to be a state variable - it's passed as a parameter!

So this isn't actually an issue - the error is passed as a method parameter in the C# version.

### Issue 4: Missing Nested Try-Catch in Catch Handler

**C# Reference** - `a_body1Catch2` is wrapped in try-catch (lines 68-86):
```cpp
int a_body1Catch2(const Error& e,int loopDepth=0)
{
    try {
        // User's catch handler code (return -1;)
        // ...
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);  // Propagates to outer
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

**C++ Current**: No method exists, and inline code isn't wrapped in try-catch.

**Reason**: If the user's catch handler code throws an exception, it should propagate to the outer catch handler (a_body1Catch1). The nested try-catch enables this propagation.

## Implementation Plan

### Phase 1: Generate Catch Continuation Methods

**Files to modify**:
- `flow/actorcompiler_cpp/ActorCompiler.h`
- `flow/actorcompiler_cpp/ActorCompiler.cpp`

**Changes**:

1. Modify `compileStatement(TryStatement*)` to generate catch continuation methods instead of goto-based code.

Current implementation (ActorCompiler.cpp:717-778):
```cpp
void ActorCompiler::compileStatement(Function* func, TryStatement* stmt, const Context& ctx) {
    // ... validation ...

    std::string errorVarName = "__current_error";
    // ... parse catch expression ...

    std::string catchLabel = generateLabel();  // ← Generates LABEL

    func->writeLine("// BEGIN try block");
    func->writeLine("try {");
    // ...
    func->writeLine("catch (Error& " + errorVarName + ") {");
    func->indent(+1);
    func->writeLine("goto " + catchLabel + ";");  // ← Uses GOTO
    // ...
    func->writeLine(catchLabel + ":");  // ← Inline LABEL
    func->writeLine("{");
    // ... inline catch body ...
}
```

New implementation:
```cpp
void ActorCompiler::compileStatement(Function* func, TryStatement* stmt, const Context& ctx) {
    // Flow actors only support a single catch clause
    if (stmt->catches.size() != 1) {
        throw Error(stmt->firstSourceLine, "try statement must have exactly one catch clause");
    }

    const auto& catchClause = stmt->catches[0];

    // Parse error variable name from catch expression
    std::string errorVarName = "e";
    std::string catchExpr = catchClause.expression;
    catchExpr.erase(std::remove(catchExpr.begin(), catchExpr.end(), ' '), catchExpr.end());

    if (catchExpr != "...") {
        if (catchExpr.find("Error&") == 0) {
            errorVarName = catchExpr.substr(6);
        } else {
            throw Error(catchClause.firstSourceLine,
                "Only type 'Error&' or '...' may be caught in an actor function");
        }
    }

    // Generate catch continuation method name
    // Pattern: a_body1Catch2, a_body1Catch3, etc. (Catch1 is always outer handler)
    std::string catchMethodName = func->name + "Catch2";  // TODO: increment for nested catches

    // Create the catch continuation method
    Function* catchFunc = getFunction(catchMethodName);
    catchFunc->returnType = "int";
    catchFunc->formalParameters = {"const Error& " + errorVarName, "int loopDepth=0"};

    // Wrap catch body in try-catch to allow propagation to outer handler
    catchFunc->writeLine("try {");
    catchFunc->indent(+1);

    // Compile the catch body into the catch method
    Context catchCtx = ctx;  // Inherit context but no inner catch handler
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
    catchFunc->writeLine("return loopDepth;");

    // Now generate the try block in the main function
    func->writeLine("try {");
    func->indent(+1);

    // Compile try body with context pointing to inner catch handler
    Context tryCtx = ctx.withCatch(errorVarName, "unused", catchMethodName);
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
```

2. Update `compileStatement(WaitStatement*)` to route errors to correct catch handler:

Current code (ActorCompiler.cpp:476):
```cpp
cb.errorHandler = "a_body1Catch1";  // Always routes to outer handler
```

New code:
```cpp
// Use the catch handler from context if available, otherwise outer handler
cb.errorHandler = ctx.catchHandler.empty() ? "a_body1Catch1" : ctx.catchHandler;
```

3. Update error checks in wait statement to route to correct handler:

Current code (ActorCompiler.cpp:489):
```cpp
func->writeLine("if (static_cast<" + className +
    "*>(this)->actor_wait_state < 0) return a_body1Catch1(actor_cancelled(), loopDepth);");
```

New code:
```cpp
std::string errorHandler = ctx.catchHandler.empty() ? "a_body1Catch1" : ctx.catchHandler;
func->writeLine("if (static_cast<" + className +
    "*>(this)->actor_wait_state < 0) return " + errorHandler + "(actor_cancelled(), loopDepth);");
```

And similar for ready-check error path (ActorCompiler.cpp:492):
```cpp
func->writeLine("if (" + futureVar + ".isReady()) { if (" + futureVar +
    ".isError()) return " + errorHandler + "(" + futureVar + ".getError(), loopDepth); else return " +
    whenMethodName + "(" + futureVar + ".get(), loopDepth); };");
```

### Phase 2: Update Context Structure

**Files to modify**:
- `flow/actorcompiler_cpp/Context.h` (if catchHandler isn't already there)

The `Context` struct should already have `catchHandler` from previous work. Verify it's being used correctly.

Current Context structure should have:
```cpp
struct Context {
    std::string breakLabel;
    std::string continueLabel;
    std::string catchHandler;     // Name of catch continuation method
    std::string errorVarName;     // Name of error variable in catch
    std::string errorCodeVar;     // Unused in current implementation

    static Context createUnreachable();
    Context loopContext(const std::string& breakLabel, const std::string& continueLabel) const;
    Context withCatch(const std::string& errorVarName,
                     const std::string& errorCodeVar,
                     const std::string& catchHandler) const;
};
```

### Phase 3: Remove Goto-Based Code Generation

**Files to modify**:
- `flow/actorcompiler_cpp/ActorCompiler.cpp`

Remove the goto/label generation code from `compileStatement(TryStatement*)`:
- Remove `generateLabel()` call for catch handler
- Remove `goto` statements
- Remove label generation

The new implementation from Phase 1 already replaces this.

### Phase 4: Handle Nested Try-Catch

**Consideration**: If an actor has nested try-catch blocks, we need to number them correctly:
- Outer try: `a_body1Catch2`
- Inner try: `a_body1Catch3`
- etc.

**Implementation**: Track catch handler numbering similar to how we track callback indices.

Add to ActorCompiler class:
```cpp
private:
    int nextCatchHandlerIndex = 2;  // Start at 2 (Catch1 is always outer)

public:
    int getNextCatchHandlerIndex() { return nextCatchHandlerIndex++; }
```

Update Phase 1 code:
```cpp
int catchIndex = getNextCatchHandlerIndex();
std::string catchMethodName = "a_body1Catch" + std::to_string(catchIndex);
```

## Testing

After implementing these changes, run:
```bash
cd /Users/jzhou/src/foundationdb/flow/actorcompiler_cpp
ninja
./actorcompiler_cpp_runtime_actors
```

Expected output should match C# reference:
1. ✅ `a_body1Catch2` method generated
2. ✅ Error routing in `a_body1` goes to `a_body1Catch2`, not `a_body1Catch1`
3. ✅ Callbacks route to `a_body1Catch2`
4. ✅ `a_body1Catch2` body wrapped in try-catch for propagation
5. ✅ No goto statements or inline labels

Compare line-by-line with `flow/actorcompiler/try_catch.actor.g.cpp`.

## Summary of Key Changes

### ActorCompiler.cpp

1. **`compileStatement(TryStatement*)`** - Complete rewrite:
   - Generate catch continuation method instead of goto/label
   - Wrap catch body in try-catch for propagation
   - Pass error as method parameter
   - Route try block to inner catch handler

2. **`compileStatement(WaitStatement*)`** - Error routing:
   - Use `ctx.catchHandler` instead of hardcoded "a_body1Catch1"
   - Update error checks to route to correct handler

3. **ActorCompiler class** - State tracking:
   - Add `nextCatchHandlerIndex` member
   - Add `getNextCatchHandlerIndex()` method

## Open Questions

1. **Nested try-catch blocks**: How should we handle actors with nested try statements?
   - Current plan: Increment catch handler index (Catch2, Catch3, etc.)
   - Need to verify this matches C# behavior

2. **Error variable scope**: The error variable is passed as a parameter to the catch method. Does this work for all catch body patterns?
   - C# uses `const Error& e` parameter
   - Should work for all cases since catch body is compiled into method

3. **Catch-all vs Error&**: How to handle `catch (...)` without a named variable?
   - Current plan: Use `unknown_error()` when calling catch method
   - Matches C# pattern

4. **Context propagation**: Does the catch body need access to loop break/continue labels?
   - Yes, catch body inherits context from try statement
   - Already handled by passing `catchCtx` to compile()
