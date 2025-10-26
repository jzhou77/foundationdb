# Fix Plan: Multiple Waits Support for C++ Actor Compiler

## ✅ STATUS: COMPLETED

All phases have been successfully implemented and verified. The C++ actor compiler now generates output that matches the C# reference implementation for actors with multiple sequential waits.

## Problem Summary

The C++ actor compiler generates incorrect code for actors with multiple sequential waits. Comparison with the C# reference implementation reveals two critical issues:

1. **Incorrect method naming for nested waits** - Uses flat naming instead of hierarchical
2. **Callback methods missing from state class** - Only appear in actor class

## Source Files

- **C# Reference**: `flow/actorcompiler/multiple_waits.actor.g.cpp`
- **C++ Output**: `flow/actorcompiler_cpp/multiple_waits.actor.g.cpp`
- **Test Actor**: `flow/actorcompiler_cpp/tests/runtime_test_actors/multiple_waits.actor.cpp`

## Detailed Comparison

### Issue 1: Method Naming for Nested Waits

**C# Reference Pattern** (CORRECT):
```cpp
// First wait (at a_body1 level)
int a_body1when1(int const& __x, int loopDepth) {  // line 79
    x = __x;
    loopDepth = a_body1cont1(loopDepth);  // continues to a_body1cont1
    return loopDepth;
}

// Second wait (inside a_body1cont1, so named a_body1cont1when1)
int a_body1cont1when1(int const& __y, int loopDepth) {  // line 164
    y = __y;
    loopDepth = a_body1cont2(loopDepth);  // continues to a_body1cont2
    return loopDepth;
}
```

**C++ Current Pattern** (INCORRECT):
```cpp
// First wait
int a_body1when1(int const& __x, int loopDepth) {  // line 70
    x = __x;
    loopDepth = a_body1cont1(loopDepth);
    return loopDepth;
}

// Second wait - WRONG: should be a_body1cont1when1, not a_body1when2
int a_body1when2(int const& __y, int loopDepth) {  // line 82
    y = __y;
    loopDepth = a_body1cont2(loopDepth);
    return loopDepth;
}
```

**Root Cause**: In `ActorCompiler::compileStatement(WaitStatement*)`, the when method name is generated as:
```cpp
std::string whenMethodName = "a_body1when" + std::to_string(cbIndex + 1);
```

This always uses `a_body1` as the prefix, even when inside a continuation function.

**Required Fix**: Track the current function context and generate nested method names:
- If compiling inside `a_body1`, generate `a_body1when1`
- If compiling inside `a_body1cont1`, generate `a_body1cont1when1`
- If compiling inside `a_body1cont2`, generate `a_body1cont2when1`
- etc.

### Issue 2: Callback Methods in State Class

**C# Reference** (CORRECT):
The state class contains all callback methods:
```cpp
class MultipleWaitsActorState {
public:
    // ... constructor, destructor, body methods ...

    // First callback set (lines 101-151)
    void a_callback_fire(ActorCallback< MultipleWaitsActor, 0, int >*, int const& value) {
        a_exitChoose1();
        try {
            a_body1when1(value, 0);
        }
        catch (Error& error) {
            a_body1Catch1(error, 0);
        }
    }
    void a_callback_error(ActorCallback< MultipleWaitsActor, 0, int >*, Error err) { ... }

    // Second callback set (lines 186-236)
    void a_callback_fire(ActorCallback< MultipleWaitsActor, 1, int >*, int const& value) {
        a_exitChoose2();
        try {
            a_body1cont1when1(value, 0);  // Calls nested when method!
        }
        catch (Error& error) {
            a_body1Catch1(error, 0);
        }
    }
    void a_callback_error(ActorCallback< MultipleWaitsActor, 1, int >*, Error err) { ... }

    // State variables
    Future<int> f1;
    Future<int> f2;
    int x;
    int y;
};
```

**C++ Current** (INCORRECT):
State class has no callback methods - they're only in the actor class (lines 153-248).

**Root Cause**: In `ActorCompiler::write()`, the callback methods are generated in `writeActorClass()` but not in the state class.

**Required Fix**: Generate callback methods in the state class (in `writeFunctions()`) instead of the actor class.

### Issue 3: Callback Invocation

**C# Reference**: Callbacks invoke the correctly nested when methods:
- Callback 0 calls `a_body1when1` (line 109)
- Callback 1 calls `a_body1cont1when1` (line 194) ← nested name!

**C++ Current**: Callbacks invoke flat when methods:
- Callback 0 calls `a_body1when1` (line 160)
- Callback 1 calls `a_body1when2` (line 208) ← flat name!

This is a consequence of Issue 1.

## Implementation Plan

### Phase 1: Track Function Context for Nested Naming

**Files to modify**:
- `flow/actorcompiler_cpp/ActorCompiler.h`
- `flow/actorcompiler_cpp/ActorCompiler.cpp`

**Changes**:

1. Add context tracking to ActorCompiler:
```cpp
// In ActorCompiler class (ActorCompiler.h)
private:
    std::string currentFunctionPrefix = "a_body1";  // Track current function context
```

2. Update `compileStatement(WaitStatement*)` to use context:
```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    int cbIndex = nextCallbackIndex();

    // Use current function's name as prefix, not always "a_body1"
    std::string prefix = func->name;  // e.g., "a_body1" or "a_body1cont1"
    std::string whenMethodName = prefix + "when" + std::to_string(cbIndex + 1);
    std::string contMethodName = prefix + "cont" + std::to_string(cbIndex + 1);

    // ... rest of implementation
}
```

**Expected Result**:
- First wait in `a_body1`: generates `a_body1when1` → `a_body1cont1` ✓
- Second wait in `a_body1cont1`: generates `a_body1cont1when1` → `a_body1cont1cont1`

**Wait, there's an issue**: The second wait should generate `a_body1cont1when1` → `a_body1cont2`, NOT `a_body1cont1cont1`.

The pattern is:
- When in `a_body1`: next continuation is `a_body1cont1`
- When in `a_body1cont1`: next continuation is `a_body1cont2`
- When in `a_body1cont2`: next continuation is `a_body1cont3`

So the continuation numbering is global (1, 2, 3...) but the when numbering is local to each function (always 1 for the first wait in that function).

**Revised Fix**:

```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    int cbIndex = nextCallbackIndex();

    // When method: use current function prefix + "when" + local index
    // For first wait in any function, use when1
    std::string prefix = func->name;  // e.g., "a_body1" or "a_body1cont1"
    std::string whenMethodName = prefix + "when1";  // Always when1 for first wait in function

    // Continuation: use global counter
    std::string contMethodName = "a_body1cont" + std::to_string(cbIndex + 1);

    // ... rest of implementation
}
```

**But this won't work for multiple waits in the same function!**

Let me reconsider. Looking at the C# output more carefully:

In `a_body1`:
- First wait generates: `a_body1when1` → `a_body1cont1`

In `a_body1cont1`:
- Second wait generates: `a_body1cont1when1` → `a_body1cont2`

So the pattern is:
- `whenMethodName` = `<current_function_prefix>` + `when` + `<local_wait_index>`
- `contMethodName` = `a_body1cont` + `<global_cont_index>`

But how do we track the local wait index within each function?

**Solution**: Each Function object should track how many waits it has encountered.

```cpp
// In Function class (Function.h)
private:
    int waitCount = 0;  // Number of waits in this function

public:
    int getNextWaitIndex() { return ++waitCount; }
```

Then in `compileStatement(WaitStatement*)`:
```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    int cbIndex = nextCallbackIndex();
    int localWaitIndex = func->getNextWaitIndex();  // 1 for first wait in this function

    // When method: current function + when + local index
    std::string whenMethodName = func->name + "when" + std::to_string(localWaitIndex);

    // Continuation: global counter
    std::string contMethodName = "a_body1cont" + std::to_string(cbIndex + 1);

    // ... rest of implementation
}
```

### Phase 2: Move Callback Methods to State Class

**Files to modify**:
- `flow/actorcompiler_cpp/ActorCompiler.cpp`

**Changes**:

1. Generate callback methods in state class (`writeFunctions()`):
```cpp
void ActorCompiler::writeFunctions(std::ostream& writer) {
    for (const auto& pair : functions) {
        Function* func = pair.second;
        if (func->getBodyText().length() > 0) {
            writeFunction(writer, func);
        }
    }

    // NEW: Generate callback methods in state class
    for (const auto& cb : callbacks) {
        writeStateCallbackMethods(writer, cb);
    }
}

void ActorCompiler::writeStateCallbackMethods(std::ostream& writer, const CallbackInfo& cb) {
    std::string exitMethodName = "a_exitChoose" + std::to_string(cb.index + 1);
    std::string whenMethodName = cb.continueLabel;
    std::string catchMethodName = cb.errorHandler;

    // a_callback_fire - const& overload
    writer << "\tvoid a_callback_fire(ActorCallback< " << className << ", " << cb.index << ", "
           << cb.type << " >*, " << cb.type << " const& value) {\n";
    writer << "\t\t#ifdef WITH_ACAC\n";
    // ... ACAC code
    writer << "\t\t#endif // WITH_ACAC\n";
    writer << "\t\t" << exitMethodName << "();\n";
    writer << "\t\ttry {\n";
    writer << "\t\t\t" << whenMethodName << "(value, 0);\n";
    writer << "\t\t}\n";
    writer << "\t\tcatch (Error& error) {\n";
    writer << "\t\t\t" << catchMethodName << "(error, 0);\n";
    writer << "\t\t} catch (...) {\n";
    writer << "\t\t\t" << catchMethodName << "(unknown_error(), 0);\n";
    writer << "\t\t}\n\n";
    writer << "\t}\n";

    // a_callback_fire - && overload
    // ... (similar to const& but with std::move)

    // a_callback_error
    // ... (similar pattern)
}
```

2. Remove callback methods from actor class (`writeActorClass()`):
```cpp
void ActorCompiler::writeActorClass(...) {
    // ... existing code ...

    // REMOVE the loop that generates callback methods (lines 1011-1085)
    // Don't generate callbacks here anymore - they're in state class now

    writer << "};\n";
}
```

### Phase 3: Add Friend Declarations

**Changes**:
In `writeActorClass()`, add friend declarations after the class opening:
```cpp
writer << "friend struct ActorCallback< " << className << ", 0, int >;\n";
writer << "friend struct ActorCallback< " << className << ", 1, int >;\n";
// ... for each callback
```

## Testing

After implementing these changes, run:
```bash
cd /Users/jzhou/src/foundationdb/flow/actorcompiler_cpp
ninja
./actorcompiler_cpp_runtime_actors
```

Verify the output matches the C# reference for:
1. Method naming: `a_body1cont1when1` instead of `a_body1when2`
2. Callback location: Methods appear in state class, not actor class
3. Structure matches C# line-by-line

## Open Questions

1. **Function.h modification**: Do we need to add `waitCount` tracking to the Function class, or can we track this differently?

2. **ACAC UIDs**: Should callback method UIDs be different when generated in state class vs actor class?

3. **Multiple waits in same function**: If a function has 2+ waits (e.g., in a loop), the local naming needs to increment: `when1`, `when2`, etc.

## Summary of Required Code Changes

### ActorCompiler.h
- [ ] Add `waitCount` to Function class (or track differently)
- [ ] Add `writeStateCallbackMethods()` declaration

### ActorCompiler.cpp
- [ ] Modify `compileStatement(WaitStatement*)` to use function-local naming
- [ ] Implement `writeStateCallbackMethods()`
- [ ] Move callback generation from `writeActorClass()` to state class
- [ ] Add friend declarations in state class

### Function.h (if needed)
- [ ] Add `int waitCount = 0;` member
- [ ] Add `int getNextWaitIndex()` method
