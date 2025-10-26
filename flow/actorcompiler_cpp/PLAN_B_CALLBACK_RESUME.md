# Plan B: Complete Phase 6 TODOs - Callback & Resume Logic

**Date:** October 25, 2025  
**Goal:** Implement remaining critical pieces for functional actor code generation before comprehensive testing

---

## Overview

Before proceeding to full Step 8 validation, we need to complete the core runtime mechanics that make actors actually work. Currently, we generate syntactically correct actor classes, but they lack the callback infrastructure and state machine resume logic needed for asynchronous execution.

---

## Current State Assessment

### What Works ✅
- All 15 statement types compile to continuation-based code
- State variable discovery and typed emission
- Template support
- Probe instrumentation
- Constructor kicks off body continuation
- Cancel() method skeleton exists
- 13 smoke tests validate code structure

### Critical Gaps 🔴
1. **Callback Classes:** No ActorCallback generation for async waits
2. **Resume Logic:** No state machine switch/case for continuation index
3. **Cancellation Propagation:** No wiring to outstanding waits
4. **Wait Completion:** Callbacks not registered with futures

### Why These Matter
Without these pieces, generated actors will:
- Compile but not execute correctly
- Fail to resume after wait() completes
- Leak resources on cancellation
- Not integrate with Flow's Future/Promise system

---

## Task 1: Callback Generation Infrastructure

### 1.1 Understand Callback Pattern (Research Phase)

**Reference Implementation:** C# ActorCompiler.cs callback generation

**Key Concepts:**
- Each wait statement needs a callback to resume execution
- Callbacks are templated on the future's return type
- Actor class inherits from multiple ActorCallback<T> bases
- Callbacks know their continuation index to resume

**Example Pattern from C# output:**
```cpp
// Actor with wait statement needs callback
template <class T>
class MyActorCallback : public ActorCallback<MyActor<T>, 0, T> {
    using ActorCallback::ActorCallback;
};
```

### 1.2 Track Callback Requirements

**File:** `ActorCompiler.h`
```cpp
struct CallbackInfo {
    std::string type;           // T from Future<T>
    int index;                  // Callback index (0, 1, 2, ...)
    std::string continueLabel;  // Continuation function to invoke
};

class ActorCompiler {
    // Add:
    std::vector<CallbackInfo> callbacks;
    int callbackIndex = 0;
    
    // Method to generate next callback index
    int nextCallbackIndex() { return callbackIndex++; }
};
```

### 1.3 Modify Wait Statement Compiler

**File:** `ActorCompiler.cpp` - `compileStatement(WaitStatement*)`

**Current Code (simplified):**
```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    std::string contLabel = generateLabel();
    Function* contFunc = getFunction(contLabel);
    
    func->writeLine("StrictFuture<" + stmt->result.type + "> __when_expr = " + stmt->futureExpression + ";");
    func->writeLine("if (__when_expr.isReady()) {");
    func->indent(+1);
    // Fast path: future already ready
    func->indent(-1);
    func->writeLine("} else {");
    func->indent(+1);
    func->writeLine("// TODO: Set up ActorCallback and register with future");
    func->indent(-1);
    func->writeLine("}");
}
```

**New Code (with callback):**
```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    std::string contLabel = generateLabel();
    Function* contFunc = getFunction(contLabel);
    int cbIndex = nextCallbackIndex();
    
    // Track callback for later class generation
    CallbackInfo cb;
    cb.type = stmt->result.type;
    cb.index = cbIndex;
    cb.continueLabel = contLabel;
    callbacks.push_back(cb);
    
    func->writeLine("StrictFuture<" + stmt->result.type + "> __when_expr_" + std::to_string(cbIndex) + " = " + stmt->futureExpression + ";");
    func->writeLine("if (__when_expr_" + std::to_string(cbIndex) + ".isReady()) {");
    func->indent(+1);
    // Fast path
    if (stmt->resultIsState) {
        func->writeLine(stmt->result.name + " = __when_expr_" + std::to_string(cbIndex) + ".get();");
    } else {
        func->writeLine(stmt->result.type + " " + stmt->result.name + " = __when_expr_" + std::to_string(cbIndex) + ".get();");
    }
    func->writeLine("loopDepth = a_body1cont" + std::to_string(cbIndex) + "(loopDepth);");
    func->indent(-1);
    func->writeLine("} else {");
    func->indent(+1);
    
    // Slow path: register callback
    func->writeLine("actor_wait_state = " + std::to_string(cbIndex + 1) + ";");
    func->writeLine("__when_expr_" + std::to_string(cbIndex) + ".addCallbackAndClear(static_cast<ActorCallback< " + className + ", " + std::to_string(cbIndex) + ", " + stmt->result.type + " >*>(this));");
    func->writeLine("loopDepth = 0;");
    
    func->indent(-1);
    func->writeLine("}");
    
    // Emit continuation label
    func->writeLine("");
    func->writeLine("return loopDepth;");
}
```

### 1.4 Generate Callback Classes in Actor Class

**File:** `ActorCompiler.cpp` - `writeActorClass()`

**Add after class declaration:**
```cpp
void ActorCompiler::writeActorClass(...) {
    // ... existing code ...
    
    // Generate callback base classes
    writer << "class " << className << " final : public Actor<" << returnType << ">";
    for (const auto& cb : callbacks) {
        writer << ", public ActorCallback< " << className << ", " << cb.index << ", " << cb.type << " >";
    }
    writer << ", public FastAllocated<" << fullClassName << ">, public " << fullStateClassName << " {\n";
    
    // ... rest of class ...
    
    // Emit callback_fire methods
    for (const auto& cb : callbacks) {
        writer << "\tvoid a_callback_fire(ActorCallback< " << className << ", " << cb.index << ", " << cb.type << " >* cb, " << cb.type << " const& value) {\n";
        writer << "\t\tfreeAfter(static_cast<Actor<" << returnType << ">*>(this));\n";
        writer << "\t\tactor_wait_state = 0;\n";
        writer << "\t\t" << cb.continueLabel << "(std::move(const_cast<" << cb.type << "&>(value)));\n";
        writer << "\t}\n";
        
        writer << "\tvoid a_callback_error(ActorCallback< " << className << ", " << cb.index << ", " << cb.type << " >* cb, Error err) {\n";
        writer << "\t\tfreeAfter(static_cast<Actor<" << returnType << ">*>(this));\n";
        writer << "\t\tactor_wait_state = 0;\n";
        writer << "\t\ta_body1Catch1(err, 0);\n";  // Jump to error handler
        writer << "\t}\n";
    }
}
```

**Estimated Time:** 8-12 hours

---

## Task 2: State Machine Resume Logic

### 2.1 Add Continuation Index Tracking

**File:** `ActorCompiler.cpp` - State class generation

**Add member variable:**
```cpp
void ActorCompiler::write(std::ostream& writer) {
    // ... in state class ...
    writer << "\tint actor_wait_state;\n";
}
```

**Initialize in constructor:**
```cpp
void ActorCompiler::writeStateConstructor(std::ostream& writer) {
    writer << "\t" << stateClassName << "(" << join(parameterList(), ", ") << ")";
    writer << " : actor_wait_state(-1)";  // -1 means not waiting
    // ... rest of initializers ...
}
```

### 2.2 Implement Continuation Switch Logic

**File:** `ActorCompiler.cpp` - Body function generation

**Current pattern:**
```cpp
int body(int loopDepth) {
    // Linear execution
    statement1;
    statement2;
    return loopDepth;
}
```

**New pattern with resume:**
```cpp
int body(int loopDepth) {
    try {
        loopDepth = a_body1(loopDepth);
    } catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch2(unknown_error(), loopDepth);
    }
    return loopDepth;
}

int a_body1(int loopDepth) {
    // Resume from wait point if needed
    if (actor_wait_state > 0) {
        switch (actor_wait_state) {
            case 1: goto resume_1;
            case 2: goto resume_2;
            // ... one per wait statement
        }
    }
    
    // Fresh execution
    statement1;
    
    // Wait point 1
    if (future1.isReady()) {
        // Fast path
    } else {
        actor_wait_state = 1;
        return 0;  // Suspend
    }
resume_1:
    actor_wait_state = 0;
    
    statement2;
    
    // Wait point 2
    if (future2.isReady()) {
        // Fast path
    } else {
        actor_wait_state = 2;
        return 0;  // Suspend
    }
resume_2:
    actor_wait_state = 0;
    
    return loopDepth;
}
```

### 2.3 Modify Function Generation

**File:** `ActorCompiler.cpp` - `writeFunction()`

**Add resume switch at function start:**
```cpp
void ActorCompiler::writeFunction(std::ostream& writer, Function* func) {
    // ... function signature ...
    writer << " {\n";
    
    // If this is a body function with waits, add resume switch
    if (func->name == "body" && !callbacks.empty()) {
        writer << "\t\tif (actor_wait_state > 0) {\n";
        writer << "\t\t\tswitch (actor_wait_state) {\n";
        for (size_t i = 0; i < callbacks.size(); ++i) {
            writer << "\t\t\t\tcase " << (i + 1) << ": goto a_body1_resume_" << (i + 1) << ";\n";
        }
        writer << "\t\t\t}\n";
        writer << "\t\t}\n\n";
    }
    
    // Function body
    // ... existing code ...
}
```

### 2.4 Update Wait Compiler to Emit Resume Labels

**File:** `ActorCompiler.cpp` - `compileStatement(WaitStatement*)`

**Add label emission:**
```cpp
void ActorCompiler::compileStatement(Function* func, WaitStatement* stmt, const Context& ctx) {
    // ... setup ...
    
    func->writeLine("} else {");
    func->indent(+1);
    func->writeLine("actor_wait_state = " + std::to_string(cbIndex + 1) + ";");
    func->writeLine("__when_expr.addCallbackAndClear(...);");
    func->writeLine("return 0;");
    func->indent(-1);
    func->writeLine("}");
    
    // Emit resume label
    func->writeLine("a_body1_resume_" + std::to_string(cbIndex + 1) + ":");
    func->writeLine("if (__when_expr.isError()) {");
    func->indent(+1);
    // Error handling
    func->indent(-1);
    func->writeLine("}");
    func->writeLine(stmt->result.name + " = __when_expr.pop();");
    func->writeLine("actor_wait_state = 0;");
    func->writeLine("");
    
    // Continue with rest of function
}
```

**Estimated Time:** 10-14 hours

---

## Task 3: Cancellation Propagation

### 3.1 Track Outstanding Waits

**File:** `ActorCompiler.cpp` - State class

**Add member:**
```cpp
writer << "\tstd::vector<SAV<void>*> outstandingWaits;\n";
```

### 3.2 Register Waits on Slow Path

**File:** `ActorCompiler.cpp` - Wait compiler

**Modify slow path:**
```cpp
func->writeLine("__when_expr.addCallbackAndClear(static_cast<ActorCallback<...>*>(this));");
func->writeLine("outstandingWaits.push_back(&__when_expr);");
```

### 3.3 Implement Cancel Propagation

**File:** `ActorCompiler.cpp` - `writeActorClass()`

**Update cancel() method:**
```cpp
writer << "\tvoid cancel() override {\n";
if (generateProbes) {
    writer << "\t\t// PROBE_CANCEL(\"" << actor.name << "\")\n";
}
writer << "\t\tfor (auto* wait : outstandingWaits) {\n";
writer << "\t\t\tif (wait) wait->cancel();\n";
writer << "\t\t}\n";
writer << "\t\toutstandingWaits.clear();\n";
writer << "\t}\n";
```

**Estimated Time:** 4-6 hours

---

## Task 4: Error Handling Integration

### 4.1 Generate Catch Functions

**File:** `ActorCompiler.cpp` - Function generation

**For each try/catch, generate catch handler:**
```cpp
void ActorCompiler::compileStatement(Function* func, TryStatement* stmt, const Context& ctx) {
    std::string catchLabel = generateLabel();
    Function* catchFunc = getFunction(catchLabel);
    
    // Generate a_body1Catch1 function
    catchFunc->writeLine("int a_body1Catch1(Error error, int loopDepth) {");
    catchFunc->indent(+1);
    catchFunc->writeLine("this->~" + stateClassName + "();");
    catchFunc->writeLine("static_cast<Actor<" + actor.returnType + ">*>(this)->sendError(error);");
    catchFunc->writeLine("loopDepth = 0;");
    catchFunc->indent(-1);
    catchFunc->writeLine("return loopDepth;");
    catchFunc->writeLine("}");
    
    // ... rest of try/catch compilation ...
}
```

### 4.2 Wire Error Callbacks

**File:** `ActorCompiler.cpp` - Callback generation

**Already shown in Task 1.4:**
```cpp
writer << "\tvoid a_callback_error(...) {\n";
writer << "\t\ta_body1Catch1(err, 0);\n";
writer << "\t}\n";
```

**Estimated Time:** 6-8 hours

---

## Task 5: Testing & Validation

### 5.1 Create Test Actors

**Create:** `flow/actorcompiler_cpp/tests/runtime_test_actors/`

**Test 1: Simple Wait**
```cpp
// simple_wait.actor.cpp
ACTOR Future<int> simpleWait() {
    int x = wait(delay(0, 42));
    return x;
}
```

**Test 2: Multiple Waits**
```cpp
// multiple_waits.actor.cpp
ACTOR Future<int> multipleWaits() {
    int x = wait(delay(0, 1));
    int y = wait(delay(0, 2));
    return x + y;
}
```

**Test 3: Choose/When**
```cpp
// choose_when.actor.cpp
ACTOR Future<int> chooseWhen(Future<int> a, Future<int> b) {
    choose {
        when(int x = wait(a)) { return x; }
        when(int y = wait(b)) { return y; }
    }
}
```

**Test 4: Try/Catch**
```cpp
// try_catch.actor.cpp
ACTOR Future<int> tryCatch() {
    try {
        int x = wait(throwingFuture());
        return x;
    } catch (Error& e) {
        return -1;
    }
}
```

### 5.2 Compile Test Actors

```bash
cd flow/actorcompiler_cpp
./build/actorcompiler_cpp tests/runtime_test_actors/simple_wait.actor.cpp tests/runtime_test_actors/simple_wait.cpp
```

### 5.3 Verify Generated Code Compiles

```bash
# Create minimal test harness
cat > test_harness.cpp << 'EOF'
#include "flow/flow.h"
#include "tests/runtime_test_actors/simple_wait.cpp"

int main() {
    simpleWait();
    return 0;
}
EOF

g++ -std=c++17 -I. test_harness.cpp -o test_harness
```

### 5.4 Update Smoke Tests

**Add to:** `flow/actorcompiler_cpp/tests/codegen_smoke_test.cpp`

```cpp
void testCallbackGeneration() {
    std::cout << "Test: Callback generation for wait statements\n";
    
    std::string sourceCode = R"(
ACTOR Future<int> withCallback() {
    int x = wait(getFuture());
    return x;
}
)";
    
    ErrorMessagePolicy policy;
    ActorParser parser(sourceCode, "test.actor.cpp", policy, false);
    
    std::ostringstream output;
    parser.write(output, "out.cpp");
    
    std::string code = output.str();
    
    // Verify callback class inheritance
    assert(code.find("ActorCallback") != std::string::npos);
    // Verify a_callback_fire method
    assert(code.find("a_callback_fire") != std::string::npos);
    // Verify a_callback_error method
    assert(code.find("a_callback_error") != std::string::npos);
    // Verify actor_wait_state
    assert(code.find("actor_wait_state") != std::string::npos);
    
    std::cout << "✓ Test passed\n\n";
}

void testResumeLogic() {
    std::cout << "Test: Resume logic with switch/case\n";
    
    std::string sourceCode = R"(
ACTOR Future<int> withResume() {
    int x = wait(getFuture());
    int y = wait(getAnother());
    return x + y;
}
)";
    
    ErrorMessagePolicy policy;
    ActorParser parser(sourceCode, "test.actor.cpp", policy, false);
    
    std::ostringstream output;
    parser.write(output, "out.cpp");
    
    std::string code = output.str();
    
    // Verify resume switch exists
    assert(code.find("switch") != std::string::npos);
    assert(code.find("case 1:") != std::string::npos);
    assert(code.find("case 2:") != std::string::npos);
    // Verify resume labels
    assert(code.find("resume_") != std::string::npos);
    
    std::cout << "✓ Test passed\n\n";
}
```

**Estimated Time:** 8-10 hours

---

## Timeline & Deliverables

| Task | Description | Time Estimate | Priority |
|------|-------------|---------------|----------|
| 1 | Callback Generation | 8-12 hours | 🔴 Critical |
| 2 | State Machine Resume | 10-14 hours | 🔴 Critical |
| 3 | Cancellation Propagation | 4-6 hours | 🟡 High |
| 4 | Error Handling | 6-8 hours | 🟡 High |
| 5 | Testing & Validation | 8-10 hours | 🟢 Medium |
| **Total** | | **36-50 hours** | |

**Estimated Calendar Time:** 5-7 days of focused work, or 2 weeks part-time.

---

## Success Criteria

### Minimum Viable (Can proceed to Step 8)
- ✅ Callbacks generated for each wait statement
- ✅ Callback base classes in actor class inheritance
- ✅ Resume switch/case at function start
- ✅ Resume labels after wait points
- ✅ actor_wait_state tracking
- ✅ Generated code compiles with Flow headers
- ✅ 2 new smoke tests pass

### Full Success (Production Ready)
- ✅ All above + cancellation propagation
- ✅ Error handlers generated and wired
- ✅ Multiple waits in sequence work
- ✅ Choose/when callbacks work
- ✅ Try/catch error routing works
- ✅ Test actors compile and link
- ✅ 4+ new smoke tests pass

---

## Risk Mitigation

### Risk 1: Callback Template Complexity
**Risk:** Callback templating may be more complex than anticipated  
**Mitigation:** Start with simplest case (single wait), iterate to complex

### Risk 2: Resume Logic Interaction with Loops
**Risk:** Loop statements + waits may have complex interactions  
**Mitigation:** Test loop + wait case specifically; refer to C# output

### Risk 3: Error Propagation Through Callbacks
**Risk:** Error callbacks may not route to correct catch handlers  
**Mitigation:** Unit test error cases; trace C# implementation

### Risk 4: Memory Safety with Outstanding Waits
**Risk:** Dangling pointers to cancelled futures  
**Mitigation:** Use weak pointers or careful lifetime management

---

## Next Steps After Plan B

Once Plan B is complete:
1. **Validation:** Run extended tests on real FDB actors
2. **Step 8:** Full testing & validation from original plan
3. **Step 9:** Cleanup & documentation
4. **Production:** Replace C# actorcompiler in FDB build

---

## Notes

- This plan focuses on the **minimum viable callback/resume logic** needed for actors to work
- More sophisticated optimizations (inlining, dead code elimination) deferred to future
- Goal is **functional correctness** first, **performance** second
- Will reference C# ActorCompiler.cs extensively for patterns

---

*Plan created: October 25, 2025*  
*Target completion: ~1-2 weeks*
