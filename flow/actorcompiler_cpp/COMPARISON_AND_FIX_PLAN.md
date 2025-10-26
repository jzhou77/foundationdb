# Actor Compiler Comparison: C# vs C++ Implementation

## Overview
This document compares the output of the C# actor compiler (flow/actorcompiler) with the C++ actor compiler (flow/actorcompiler_cpp) for the `simple_wait.actor.cpp` test case, and provides a detailed plan to align the C++ implementation with the C# version.

## Current Status Summary

**Progress:** 🟡 Partial (Actor Infrastructure Complete, State Machine Needs Work)

The C++ actor compiler has made significant progress:
- ✅ **Actor class infrastructure** is complete (inheritance, lifecycle, callbacks)
- ❌ **State machine implementation** fundamentally differs (goto-based vs method-based)
- ❌ **State class** is not templated
- ❌ **Continuation methods** are missing (uses goto instead)
- ❌ **Error handling** uses exceptions instead of structured propagation
- ❌ **State variables** are not declared as class members

**Key Insight:** The C++ version has the correct actor *runtime* structure but needs a complete rewrite of the *state machine* code generation to use method-based continuations like the C# version.

## Key Differences

### C# Version (flow/actorcompiler/simple_wait.actor.g.cpp)

**Architecture:**
- Template-based state class: `SimpleWaitActorState<SimpleWaitActor>`
- Two-class design: State logic in `SimpleWaitActorState`, runtime in `SimpleWaitActor`
- Method-based control flow with continuation functions
- Structured error handling with explicit catch handlers

**Generated Methods:**
- `a_body1()` - Main body entry point
- `a_body1Catch1()` - Error handler
- `a_body1cont1()` - Continuation after wait completes
- `a_body1when1()` - Wait completion handler (const& and && overloads)
- `a_exitChoose1()` - Callback cleanup
- `a_callback_fire()` - Callback invocation (const& and && overloads)
- `a_callback_error()` - Error callback handler

**State Management:**
- State variables declared as class members: `Future<int> f;`, `int x;`
- Proper actor_wait_state tracking
- loopDepth parameter threading through all methods

**Actor Class Features:**
- Inherits from: `Actor<int>`, `ActorCallback<...>`, `FastAllocated<...>`, `SimpleWaitActorState<SimpleWaitActor>`
- ActiveActorHelper member for instrumentation
- Proper `destroy()` method with destructor calls
- Proper `cancel()` method with state machine switch
- ACAC instrumentation support (`#ifdef WITH_ACAC`)
- Lineage support (`#ifdef ENABLE_SAMPLING`)

**Error Handling:**
- Try-catch blocks in continuation methods
- `sendErrorAndDelPromiseRef()` for error propagation
- `actor_cancelled()` handling

**Return Value Handling:**
- SAV (Send And Value) optimization check
- Placement new with RVO: `new (&SAV<T>::value()) T(std::move(val))`
- `finishSendAndDelPromiseRef()` call
- Proper state destructor calls

### C++ Version (flow/actorcompiler_cpp/simple_wait.actor.g.cpp)

**Architecture:**
- ✅ Two-class design (state + actor)
- ✅ Complete actor class with proper inheritance
- ❌ Non-template state class: `SimpleWaitActorState` (should be `template <class SimpleWaitActor>`)
- ❌ Single method: `intbody(int loopDepth)` (should be `a_body1()`)
- ❌ Goto-based control flow (`goto resume_1`, `goto cont1`)
- ❌ Exception-based error handling (`throw __when_expr.getError()`)

**What's Working:**
- ✅ `SimpleWaitActor` class with proper inheritance structure
- ✅ `destroy()` method with proper cleanup
- ✅ `cancel()` method with state machine switch
- ✅ `a_callback_fire()` method (const& version)
- ✅ `a_callback_error()` method
- ✅ `ActiveActorHelper` member
- ✅ `ActorIdentifier` constant
- ✅ FastAllocated support
- ✅ Factory function

**Critical Issues:**
- ❌ State class not templated (no `template <class ActorType>`)
- ❌ Missing state variable declarations as members (`Future<int> f;`, `int x;`)
- ❌ Missing continuation methods: `a_body1()`, `a_body1Catch1()`, `a_body1cont1()`, `a_body1when1()`
- ❌ Missing `a_exitChoose1()` cleanup method
- ❌ Missing rvalue overload for `a_callback_fire(T &&)`
- ❌ No try-catch blocks in callback methods
- ❌ `a_callback_error()` throws exception instead of calling `a_body1Catch1()`
- ❌ Callbacks use `freeAfter()` instead of `a_exitChoose1()`
- ❌ `a_callback_error()` uses `delete` instead of `sendErrorAndDelPromiseRef()`
- ❌ Missing ACAC instrumentation (`#ifdef WITH_ACAC`) in callbacks and constructor
- ❌ Missing lineage support (`#ifdef ENABLE_SAMPLING`) in constructor
- ❌ Constructor doesn't initialize `activeActorHelper(__actorIdentifier)`
- ❌ Constructor calls `body()` instead of `a_body1()`
- ❌ Body method uses goto instead of method-based continuations
- ❌ No try-catch wrapper in body method
- ❌ Direct return instead of SAV optimization check
- ❌ No actor_cancelled() check before wait

## Fix Plan for C++ Actor Compiler

### Phase 1: Architecture Transformation

**1.1 Two-Class Design**
- Generate separate state class and actor class
- Make state class a template: `template <class ActorType> class SimpleWaitActorState`
- Actor class inherits from state class and runtime base classes

**1.2 Control Flow Redesign**
- Replace goto-based state machine with method-based continuations
- Generate `a_bodyN()` methods for each continuation point
- Thread `loopDepth` parameter through all methods
- Use method calls instead of goto for transitions

### Phase 2: State Machine Generation

**2.1 Continuation Methods**
- **Entry point**: `a_body1(int loopDepth=0)` - Main body
- **Continuations**: `a_bodyNcontM(int loopDepth)` - Post-wait continuations
- **Wait handlers**: `a_bodyNwhenM(T const& value, int loopDepth)` and rvalue overload
- **Exit handlers**: `a_exitChooseN()` - Cleanup before state transition

**2.2 Error Handling Methods**
- **Catch handlers**: `a_bodyNCatchM(Error error, int loopDepth=0)`
- Wrap each continuation in try-catch blocks
- Catch both `Error&` and `...` (unknown_error)
- Call appropriate catch handler on error

**2.3 State Variable Tracking**
- Parse actor parameters and state variables
- Declare them as class members with proper types
- Track variable lifetime and scope
- Generate line number annotations

### Phase 3: Callback Infrastructure

**3.1 Callback Fire Methods**
```cpp
void a_callback_fire(ActorCallback<ActorType, N, T>*, T const& value)
void a_callback_fire(ActorCallback<ActorType, N, T>*, T && value)
```
- Add ACAC instrumentation wrapper
- Call `a_exitChooseN()` to cleanup
- Call `a_bodyNwhenM()` in try-catch block

**3.2 Callback Error Methods**
```cpp
void a_callback_error(ActorCallback<ActorType, N, T>*, Error err)
```
- Add ACAC instrumentation wrapper
- Call `a_exitChooseN()` to cleanup
- Call `a_bodyNCatchM()` in try-catch block

**3.3 Exit Choose Methods**
```cpp
void a_exitChooseN()
```
- Reset actor_wait_state if needed
- Remove ActorCallback registration
- Clean up any temporary state

### Phase 4: Actor Class Generation

**4.1 Class Declaration**
```cpp
class SimpleWaitActor final :
    public Actor<ReturnType>,
    public ActorCallback<SimpleWaitActor, 0, CallbackType1>,
    public ActorCallback<SimpleWaitActor, 1, CallbackType2>,
    ...,
    public FastAllocated<SimpleWaitActor>,
    public SimpleWaitActorState<SimpleWaitActor>
```

**4.2 Required Members**
- `static constexpr ActorIdentifier __actorIdentifier` - Unique actor ID
- `ActiveActorHelper activeActorHelper` - For instrumentation
- FastAllocated operator new/delete
- Friend declarations for ActorCallback templates

**4.3 Constructor**
```cpp
SimpleWaitActor(params...)
    : Actor<ReturnType>(),
      SimpleWaitActorState<SimpleWaitActor>(params...),
      activeActorHelper(__actorIdentifier)
{
    #ifdef WITH_ACAC
    ActorExecutionContextHelper __helper(...);
    #endif
    #ifdef ENABLE_SAMPLING
    this->lineage.setActorName("actorName");
    LineageScope _(&this->lineage);
    #endif
    this->a_body1();
}
```

**4.4 Lifecycle Methods**
```cpp
void destroy() override {
    activeActorHelper.~ActiveActorHelper();
    static_cast<Actor<ReturnType>*>(this)->~Actor();
    operator delete(this);
}

void cancel() override {
    auto wait_state = this->actor_wait_state;
    this->actor_wait_state = -1;
    switch (wait_state) {
        case 1: this->a_callback_error((ActorCallback<...>*)0, actor_cancelled()); break;
        case 2: this->a_callback_error((ActorCallback<...>*)0, actor_cancelled()); break;
        ...
    }
}
```

### Phase 5: Error Handling Transformation

**5.1 Replace Exception Throwing**
- Instead of `throw __when_expr.getError()`
- Use `return a_bodyNCatchM(__when_expr.getError(), loopDepth)`

**5.2 Add Try-Catch Wrappers**
```cpp
int a_body1(int loopDepth=0) {
    try {
        // ... body code ...
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

**5.3 Error Propagation**
```cpp
int a_bodyNCatchM(Error error, int loopDepth=0) {
    this->~SimpleWaitActorState();
    static_cast<SimpleWaitActor*>(this)->sendErrorAndDelPromiseRef(error);
    loopDepth = 0;
    return loopDepth;
}
```

**5.4 Actor Cancellation**
- Check `actor_wait_state < 0` before wait
- `if (actor_wait_state < 0) return a_bodyNCatchM(actor_cancelled(), loopDepth);`

### Phase 6: Return Value Handling

**6.1 SAV Optimization Check**
```cpp
if (!static_cast<ActorType*>(this)->SAV<ReturnType>::futures) {
    (void)(returnValue);
    this->~SimpleWaitActorState();
    static_cast<ActorType*>(this)->destroy();
    return 0;
}
```

**6.2 RVO with Placement New**
```cpp
new (&static_cast<ActorType*>(this)->SAV<ReturnType>::value())
    ReturnType(std::move(returnValue)); // state_var_RVO
```

**6.3 Promise Completion**
```cpp
this->~SimpleWaitActorState();
static_cast<ActorType*>(this)->finishSendAndDelPromiseRef();
return 0;
```

### Phase 7: Wait Statement Transformation

**7.1 StrictFuture Extraction**
```cpp
StrictFuture<T> __when_expr_N = futureExpression;
```

**7.2 Ready Check Path**
```cpp
if (__when_expr_N.isReady()) {
    if (__when_expr_N.isError())
        return a_bodyNCatchM(__when_expr_N.getError(), loopDepth);
    else
        return a_bodyNwhenM(__when_expr_N.get(), loopDepth);
}
```

**7.3 Async Path**
```cpp
static_cast<ActorType*>(this)->actor_wait_state = N;
__when_expr_N.addCallbackAndClear(
    static_cast<ActorCallback<ActorType, N, T>*>(
        static_cast<ActorType*>(this)));
loopDepth = 0;
```

**7.4 State Variable Assignment**
- Generate `a_bodyNwhenM(T const& __value, int loopDepth)` that assigns to state var
- Generate move overload: `a_bodyNwhenM(T && __value, int loopDepth)`

### Phase 8: Code Generation Implementation

**8.1 Parser Enhancements**
- Track all continuation points (after wait, after loop, etc.)
- Build hierarchical structure of bodies, continuations, catches
- Track state variable declarations and their scopes
- Number wait statements for callback indexing

**8.2 AST Representation**
```
ActorAST
├── StateClass
│   ├── Members (state variables)
│   ├── Constructor
│   ├── Destructor
│   └── Methods
│       ├── a_body1
│       │   ├── a_body1Catch1
│       │   ├── a_body1cont1
│       │   └── a_body1when1
│       ├── a_exitChoose1
│       ├── a_callback_fire (x2)
│       └── a_callback_error
└── ActorClass
    ├── Inheritance
    ├── Members (actorID, activeActorHelper)
    ├── Constructor
    ├── destroy()
    └── cancel()
```

**8.3 Template Parameter Threading**
- State class takes `template <class ActorType>`
- All casts use `static_cast<ActorType*>(this)`
- Actor class passes itself to state class template

**8.4 Line Number Annotations**
- Preserve #line directives for debugging
- Match C# compiler's annotation style
- Track source line for each generated construct

## Implementation Status and Priority

### ✅ Completed (as of current C++ version)
- Two-class design (state class + actor class)
- Actor class inheritance structure
- Basic callback methods (a_callback_fire, a_callback_error)
- Lifecycle methods (destroy, cancel)
- ActiveActorHelper and ActorIdentifier
- Factory function generation

### 🔴 Critical - Must Fix First

**1. Make State Class a Template (Phase 1.1)**
- Change `class SimpleWaitActorState` to `template <class SimpleWaitActor> class SimpleWaitActorState`
- Update actor class to inherit from `SimpleWaitActorState<SimpleWaitActor>`
- Update all casts to use `static_cast<SimpleWaitActor*>(this)` in state methods

**2. Add State Variable Member Declarations (Phase 2.3)**
- Declare `Future<int> f;` and `int x;` as class members in state class
- Track all actor parameters and state variables
- Generate proper line number annotations

**3. Replace Goto with Method-Based Continuations (Phase 1.2 + 2.1)**
- Rename `intbody()` to `a_body1()`
- Generate `a_body1cont1()` for continuation after wait
- Generate `a_body1when1(const& and &&)` for wait completion
- Generate `a_body1Catch1()` for error handling
- Replace goto jumps with method calls

**4. Add Try-Catch Wrappers (Phase 2.2 + 5.2)**
- Wrap body and continuation methods in try-catch
- Catch `Error&` and `...` (unknown_error)
- Replace `throw` with return to catch handler

### 🟡 High Priority - Fix Next

**5. Implement a_exitChoose1() (Phase 3.3)**
- Generate cleanup method for callback removal
- Replace `freeAfter()` calls in callbacks
- Reset actor_wait_state properly
- Call `ActorCallback::remove()`

**6. Fix Callback Methods (Phase 3.1 + 3.2)**
- Add rvalue overload for `a_callback_fire(T &&)`
- Add try-catch blocks around callback body
- Call `a_exitChoose1()` before handling value/error
- Call `a_body1when1()` or `a_body1Catch1()` instead of direct manipulation

**7. Fix Error Propagation (Phase 5.3)**
- Replace `delete` with `sendErrorAndDelPromiseRef(error)` in catch handlers
- Add state destructor call before error propagation
- Remove throw from `a_callback_error()`

**8. Add Instrumentation (Phase 4.3)**
- Add ACAC instrumentation in constructor, callbacks
- Add lineage support in constructor
- Initialize `activeActorHelper(__actorIdentifier)` in constructor initializer list

### 🟢 Medium Priority - Polish

**9. Implement SAV Optimization (Phase 6)**
- Add SAV::futures check for early return optimization
- Use placement new with RVO for return value
- Call `finishSendAndDelPromiseRef()` instead of direct return
- Add state destructor before promise completion

**10. Add Actor Cancellation Check (Phase 5.4)**
- Check `actor_wait_state < 0` before wait
- Return to catch handler with `actor_cancelled()` error

**11. Code Generation Infrastructure (Phase 8)**
- Improve parser to track continuation points
- Build hierarchical AST structure
- Better state variable tracking
- Line number annotation matching

## Testing Strategy

1. Start with simple_wait.actor.cpp
2. Compare generated output line-by-line with C# version
3. Add more complex test cases incrementally:
   - Multiple waits
   - Loops with waits
   - Choose/when statements
   - Error handling
   - State variables of different types
4. Run runtime tests to validate correctness
5. Performance benchmarking against C# version

## Expected Outcomes

- C++ actor compiler generates identical structure to C# version
- All runtime tests pass
- Proper error handling and cancellation support
- Full instrumentation support (ACAC, lineage)
- Memory safety with proper lifetime management

## Quick Reference: Side-by-Side Method Comparison

### State Class Structure

**C# Version:**
```cpp
template <class SimpleWaitActor>
class SimpleWaitActorState {
public:
    // Constructor
    SimpleWaitActorState(Future<int> const& f) : f(f) { }

    // State variables
    Future<int> f;
    int x;

    // Main body with try-catch
    int a_body1(int loopDepth=0) {
        try {
            StrictFuture<int> __when_expr_0 = f;
            if (static_cast<SimpleWaitActor*>(this)->actor_wait_state < 0)
                return a_body1Catch1(actor_cancelled(), loopDepth);
            if (__when_expr_0.isReady()) {
                if (__when_expr_0.isError())
                    return a_body1Catch1(__when_expr_0.getError(), loopDepth);
                else
                    return a_body1when1(__when_expr_0.get(), loopDepth);
            }
            static_cast<SimpleWaitActor*>(this)->actor_wait_state = 1;
            __when_expr_0.addCallbackAndClear(...);
            loopDepth = 0;
        } catch (Error& error) {
            loopDepth = a_body1Catch1(error, loopDepth);
        } catch (...) {
            loopDepth = a_body1Catch1(unknown_error(), loopDepth);
        }
        return loopDepth;
    }

    // Error handler
    int a_body1Catch1(Error error, int loopDepth=0) {
        this->~SimpleWaitActorState();
        static_cast<SimpleWaitActor*>(this)->sendErrorAndDelPromiseRef(error);
        loopDepth = 0;
        return loopDepth;
    }

    // Continuation after wait
    int a_body1cont1(int loopDepth) {
        if (!static_cast<SimpleWaitActor*>(this)->SAV<int>::futures) {
            (void)(x);
            this->~SimpleWaitActorState();
            static_cast<SimpleWaitActor*>(this)->destroy();
            return 0;
        }
        new (&static_cast<SimpleWaitActor*>(this)->SAV<int>::value()) int(std::move(x));
        this->~SimpleWaitActorState();
        static_cast<SimpleWaitActor*>(this)->finishSendAndDelPromiseRef();
        return 0;
    }

    // Wait completion handlers
    int a_body1when1(int const& __x, int loopDepth) {
        x = __x;
        loopDepth = a_body1cont1(loopDepth);
        return loopDepth;
    }
    int a_body1when1(int && __x, int loopDepth) {
        x = std::move(__x);
        loopDepth = a_body1cont1(loopDepth);
        return loopDepth;
    }

    // Cleanup method
    void a_exitChoose1() {
        if (static_cast<SimpleWaitActor*>(this)->actor_wait_state > 0)
            static_cast<SimpleWaitActor*>(this)->actor_wait_state = 0;
        static_cast<SimpleWaitActor*>(this)->ActorCallback<...>::remove();
    }

    // Callback handlers
    void a_callback_fire(ActorCallback<...>*, int const& value) {
        #ifdef WITH_ACAC
        ActorExecutionContextHelper __helper(...);
        #endif
        a_exitChoose1();
        try {
            a_body1when1(value, 0);
        } catch (Error& error) {
            a_body1Catch1(error, 0);
        } catch (...) {
            a_body1Catch1(unknown_error(), 0);
        }
    }
    void a_callback_fire(ActorCallback<...>*, int && value) { /* rvalue version */ }
    void a_callback_error(ActorCallback<...>*, Error err) { /* similar */ }
};
```

**C++ Version (Current):**
```cpp
class SimpleWaitActorState {  // ❌ Not templated
public:
    SimpleWaitActorState(Future<int> const& f) { }  // ❌ Doesn't store f

    // ❌ No state variable declarations

    intbody(int loopDepth) {  // ❌ Wrong name, no try-catch
        if (static_cast<Actor<int>*>(this)->actor_wait_state > 0) {
            switch (...) {
            case 1: goto resume_1;  // ❌ Uses goto
            }
        }

        StrictFuture<int> __when_expr = f;  // ❌ f is undeclared
        if (__when_expr.isReady()) {
            if (__when_expr.isError()) {
                throw __when_expr.getError();  // ❌ Throws instead of catch handler
            } else {
                x = __when_expr.get();  // ❌ x is undeclared
                goto cont1;  // ❌ Uses goto
            }
        } else {
            static_cast<Actor<int>*>(this)->actor_wait_state = 1;
            __when_expr.addCallbackAndClear(...);
            return 0;
        }

    resume_1:
        static_cast<Actor<int>*>(this)->actor_wait_state = 0;
    cont1:
        return x;  // ❌ Direct return, no SAV optimization
    }

    // ❌ Missing: a_body1Catch1(), a_body1cont1(), a_body1when1(), a_exitChoose1()
};
```

### Actor Class Constructor

**C# Version:**
```cpp
SimpleWaitActor(Future<int> const& f)
    : Actor<int>(),
      SimpleWaitActorState<SimpleWaitActor>(f),
      activeActorHelper(__actorIdentifier)
{
    #ifdef WITH_ACAC
    ActorExecutionContextHelper __helper(...);
    #endif
    #ifdef ENABLE_SAMPLING
    this->lineage.setActorName("simpleWait");
    LineageScope _(&this->lineage);
    #endif
    this->a_body1();
}
```

**C++ Version (Current):**
```cpp
SimpleWaitActor(Future<int> const& f)
    : SimpleWaitActorState(f)  // ❌ Missing Actor<int>() and activeActorHelper init
{
    // ❌ No ACAC or lineage instrumentation
    this->body(0);  // ❌ Wrong method name
}
```

This side-by-side comparison shows that the fundamental code generation strategy needs to change from goto-based to method-based continuation passing.
