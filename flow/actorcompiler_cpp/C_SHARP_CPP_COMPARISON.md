# C# vs C++ Actor Compiler Output Comparison

## Overview
Comparing C# generated `simple_wait.actor.g.cpp` with C++ version output for `simple_wait.actor.cpp`.

**Source Actor:**
```cpp
ACTOR Future<int> simpleWait(Future<int> f) {
    state int x = wait(f);
    return x;
}
```

---

## Key Differences

### 1. **Body Method Structure**

**C++ Version (Your Output):**
```cpp
int body(int loopDepth) {
    if (static_cast<Actor<int>*>(this)->actor_wait_state > 0) {
        switch (static_cast<Actor<int>*>(this)->actor_wait_state) {
            case 1: goto resume_1;
        }
    }
    
    StrictFuture<int> __when_expr = f;
    if (__when_expr.isReady()) {
        if (__when_expr.isError()) {
            throw __when_expr.getError();
        } else {
            x = __when_expr.get();
            goto cont1;
        }
    } else {
        static_cast<Actor<int>*>(this)->actor_wait_state = 1;
        __when_expr.addCallbackAndClear(static_cast<ActorCallback< SimpleWaitActor, 0, int >*>(this));
        return 0; // Suspend until callback fires
    }
    
    resume_1:
    static_cast<Actor<int>*>(this)->actor_wait_state = 0;
    cont1:
    return x;
}
```

**C# Version (Expected):**
```cpp
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
        };
        
        static_cast<SimpleWaitActor*>(this)->actor_wait_state = 1;
        __when_expr_0.addCallbackAndClear(static_cast<ActorCallback< SimpleWaitActor, 0, int >*>(static_cast<SimpleWaitActor*>(this)));
        loopDepth = 0;
    }
    catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

**Differences:**
- ❌ C++ uses `body()` method name → ✅ C# uses `a_body1()`
- ❌ C++ uses goto/labels for resume → ✅ C# uses continuation methods
- ❌ C++ has explicit switch/goto for resume → ✅ C# handles resume in callbacks
- ❌ C++ returns value directly → ✅ C# returns via continuation methods
- ❌ C++ no try/catch wrapper → ✅ C# wraps in try/catch
- ❌ C++ no cancellation check → ✅ C# checks `actor_wait_state < 0` for cancellation

---

### 2. **Continuation Methods** ⭐ **MAJOR DIFFERENCE**

**C++ Version:**
- Uses goto labels: `resume_1:`, `cont1:`
- No separate continuation methods
- Resume logic inline in body()

**C# Version:**
```cpp
int a_body1cont1(int loopDepth) {
    if (!static_cast<SimpleWaitActor*>(this)->SAV<int>::futures) { 
        (void)(x); 
        this->~SimpleWaitActorState(); 
        static_cast<SimpleWaitActor*>(this)->destroy(); 
        return 0; 
    }
    new (&static_cast<SimpleWaitActor*>(this)->SAV< int >::value()) int(std::move(x)); // state_var_RVO
    this->~SimpleWaitActorState();
    static_cast<SimpleWaitActor*>(this)->finishSendAndDelPromiseRef();
    return 0;
}

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
```

**Key Pattern:**
- `a_body1when1()` - receives the wait result, stores in state variable
- `a_body1cont1()` - continues after wait, handles return value
- Two overloads: const& and && for move semantics

---

### 3. **Exit Choose Methods** (NEW)

**C++ Version:**
- Missing entirely

**C# Version:**
```cpp
void a_exitChoose1() {
    if (static_cast<SimpleWaitActor*>(this)->actor_wait_state > 0) 
        static_cast<SimpleWaitActor*>(this)->actor_wait_state = 0;
    static_cast<SimpleWaitActor*>(this)->ActorCallback< SimpleWaitActor, 0, int >::remove();
}
```

**Purpose:**
- Reset actor_wait_state to 0
- Remove callback from futures
- Called before invoking when/catch methods

---

### 4. **Callback Fire Method**

**C++ Version:**
```cpp
// Likely missing or incomplete
```

**C# Version:**
```cpp
void a_callback_fire(ActorCallback< SimpleWaitActor, 0, int >*, int const& value) {
    #ifdef WITH_ACAC
    static constexpr ActorBlockIdentifier __identifier = UID(...);
    ActorExecutionContextHelper __helper(...);
    #endif
    a_exitChoose1();
    try {
        a_body1when1(value, 0);
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}

void a_callback_fire(ActorCallback< SimpleWaitActor, 0, int >*, int && value) {
    // Move version
}
```

**Key Points:**
- Two overloads (const& and &&)
- Calls `a_exitChoose1()` first
- Invokes `a_body1when1()` with result
- Wrapped in try/catch

---

### 5. **Callback Error Method**

**C++ Version:**
```cpp
// Likely simpler or missing proper structure
```

**C# Version:**
```cpp
void a_callback_error(ActorCallback< SimpleWaitActor, 0, int >*, Error err) {
    #ifdef WITH_ACAC
    static constexpr ActorBlockIdentifier __identifier = UID(...);
    ActorExecutionContextHelper __helper(...);
    #endif
    a_exitChoose1();
    try {
        a_body1Catch1(err, 0);
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}
```

**Key Points:**
- Calls `a_exitChoose1()`
- Invokes error handler `a_body1Catch1()`
- Wrapped in try/catch

---

### 6. **Error Handler (Catch Method)** ⭐ **MAJOR DIFFERENCE**

**C++ Version:**
- Missing or incomplete

**C# Version:**
```cpp
int a_body1Catch1(Error error, int loopDepth=0) {
    this->~SimpleWaitActorState();
    static_cast<SimpleWaitActor*>(this)->sendErrorAndDelPromiseRef(error);
    loopDepth = 0;
    return loopDepth;
}
```

**Key Points:**
- Named `a_body1Catch1()` (Catch + nesting level)
- Destructs state
- Calls `sendErrorAndDelPromiseRef(error)`
- Returns loopDepth = 0

---

### 7. **Return Value Handling** ⭐ **CRITICAL**

**C++ Version:**
```cpp
return x;  // Direct return
```

**C# Version:**
```cpp
if (!static_cast<SimpleWaitActor*>(this)->SAV<int>::futures) { 
    (void)(x); 
    this->~SimpleWaitActorState(); 
    static_cast<SimpleWaitActor*>(this)->destroy(); 
    return 0; 
}
new (&static_cast<SimpleWaitActor*>(this)->SAV< int >::value()) int(std::move(x)); // state_var_RVO
this->~SimpleWaitActorState();
static_cast<SimpleWaitActor*>(this)->finishSendAndDelPromiseRef();
return 0;
```

**Key Pattern:**
1. Check if `SAV<int>::futures` is null (no one waiting)
   - If null: destroy actor, return 0
2. Otherwise: placement new into `SAV<int>::value()`
3. Destruct state
4. Call `finishSendAndDelPromiseRef()`
5. Return 0 (not the actual value!)

**Critical Insight:** Actor methods return loopDepth (int), not the actual actor return type!

---

### 8. **State Variable Initialization**

**C++ Version:**
```cpp
SimpleWaitActorState(Future<int> const& f) {
}
```

**C# Version:**
```cpp
SimpleWaitActorState(Future<int> const& f) 
    : f(f)
{
}
```

**Difference:** C# initializes member variables in initializer list.

---

### 9. **Cancellation Method**

**C++ Version (from earlier):**
```cpp
void cancel() override {
    auto wait_state = this->actor_wait_state;
    this->actor_wait_state = -1;
    switch (wait_state) {
        case 1: this->a_callback_error((ActorCallback< SimpleWaitActor, 0, int >*)0, actor_cancelled()); break;
    }
}
```

**C# Version:**
```cpp
void cancel() override {
    auto wait_state = this->actor_wait_state;
    this->actor_wait_state = -1;
    switch (wait_state) {
    case 1: this->a_callback_error((ActorCallback< SimpleWaitActor, 0, int >*)0, actor_cancelled()); break;
    }
}
```

**Status:** ✅ This is correct! C++ matches C#.

---

## Architecture Comparison

### C++ Approach (Current)
```
body() method with:
- Resume switch at top
- Wait setup
- goto resume labels
- Direct return
```

### C# Approach (Correct)
```
a_body1() method:
- Try/catch wrapper
- Wait setup
- Return via continuation

Continuation methods:
- a_body1when1(value) - stores result, calls cont
- a_body1cont1() - continues execution, handles return

Exit methods:
- a_exitChoose1() - cleanup before resume

Callbacks:
- a_callback_fire() - calls exitChoose, then when method
- a_callback_error() - calls exitChoose, then catch method

Error handlers:
- a_body1Catch1(error) - handles errors, calls sendErrorAndDelPromiseRef
```

---

## Critical Missing Features in C++ Version

### ❌ 1. Continuation Method Generation
C++ needs to generate `a_body1contN()` and `a_body1whenN()` methods instead of using goto labels.

### ❌ 2. Exit Choose Methods
C++ needs `a_exitChooseN()` methods to cleanup before resuming.

### ❌ 3. Proper Return Value Handling
C++ needs to use SAV placement new pattern, not direct returns.

### ❌ 4. Try/Catch Wrapper
C++ needs to wrap body code in try/catch, generate catch methods.

### ❌ 5. Method Naming
C++ uses `body()` instead of `a_body1()`.

### ❌ 6. Cancellation Check
C++ needs to check `actor_wait_state < 0` at wait points.

---

## Fix Plan Summary

### Phase 1: Method Structure
1. Rename `body()` → `a_body1()` (and nested bodies: `a_body2()`, etc.)
2. Add try/catch wrapper to body methods
3. Generate `a_body1Catch1()` error handler methods

### Phase 2: Continuation Methods
4. Generate `a_body1contN()` methods for code after each wait
5. Generate `a_body1whenN()` methods (const& and && overloads) to receive wait results
6. Replace goto/label pattern with method calls

### Phase 3: Exit Methods
7. Generate `a_exitChooseN()` methods for cleanup

### Phase 4: Callback Methods
8. Update `a_callback_fire()` to call exitChoose then when method
9. Update `a_callback_error()` to call exitChoose then catch method

### Phase 5: Return Handling
10. Implement SAV placement new pattern for return values
11. Call `finishSendAndDelPromiseRef()` instead of direct return

### Phase 6: Cancellation
12. Add `actor_wait_state < 0` checks at wait points

---

## Expected Final Structure

```cpp
class SimpleWaitActorState {
public:
    // Constructor with initializer list
    SimpleWaitActorState(Future<int> const& f) : f(f) {}
    
    // Main body with try/catch
    int a_body1(int loopDepth=0) {
        try {
            // Cancellation check
            if (actor_wait_state < 0) return a_body1Catch1(actor_cancelled(), loopDepth);
            
            // Wait setup
            StrictFuture<int> __when_expr_0 = f;
            if (__when_expr_0.isReady()) {
                if (__when_expr_0.isError()) 
                    return a_body1Catch1(__when_expr_0.getError(), loopDepth);
                else 
                    return a_body1when1(__when_expr_0.get(), loopDepth);
            }
            
            // Suspend
            actor_wait_state = 1;
            __when_expr_0.addCallbackAndClear(...);
            loopDepth = 0;
        } catch (Error& error) {
            loopDepth = a_body1Catch1(error, loopDepth);
        } catch (...) {
            loopDepth = a_body1Catch1(unknown_error(), loopDepth);
        }
        return loopDepth;
    }
    
    // Continuation after wait
    int a_body1cont1(int loopDepth) {
        // Return value handling with SAV
        if (!SAV<int>::futures) {
            (void)(x);
            this->~SimpleWaitActorState();
            destroy();
            return 0;
        }
        new (&SAV<int>::value()) int(std::move(x));
        this->~SimpleWaitActorState();
        finishSendAndDelPromiseRef();
        return 0;
    }
    
    // When methods (receive wait result)
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
    
    // Exit choose
    void a_exitChoose1() {
        if (actor_wait_state > 0) actor_wait_state = 0;
        ActorCallback< SimpleWaitActor, 0, int >::remove();
    }
    
    // Callbacks
    void a_callback_fire(ActorCallback< SimpleWaitActor, 0, int >*, int const& value) {
        a_exitChoose1();
        try {
            a_body1when1(value, 0);
        } catch (Error& error) {
            a_body1Catch1(error, 0);
        } catch (...) {
            a_body1Catch1(unknown_error(), 0);
        }
    }
    
    void a_callback_error(ActorCallback< SimpleWaitActor, 0, int >*, Error err) {
        a_exitChoose1();
        try {
            a_body1Catch1(err, 0);
        } catch (Error& error) {
            a_body1Catch1(error, 0);
        } catch (...) {
            a_body1Catch1(unknown_error(), 0);
        }
    }
    
    // Error handler
    int a_body1Catch1(Error error, int loopDepth=0) {
        this->~SimpleWaitActorState();
        sendErrorAndDelPromiseRef(error);
        loopDepth = 0;
        return loopDepth;
    }
    
    // State variables
    Future<int> f;
    int x;
};
```

---

## Implementation Priority

1. **HIGH**: Continuation methods (a_body1contN, a_body1whenN) - Core to making callbacks work
2. **HIGH**: Return value handling - Essential for actors to work correctly
3. **HIGH**: Exit choose methods - Required for proper cleanup
4. **MEDIUM**: Try/catch wrapper and catch methods - Error handling
5. **MEDIUM**: Method naming (body → a_body1) - Consistency
6. **LOW**: Cancellation checks - Already working via cancel() method

---

## Next Steps

1. Analyze ActorCompiler.cpp to understand current code generation
2. Find where body() method is generated
3. Implement continuation method generation
4. Replace goto/label pattern with method calls
5. Add exit choose method generation
6. Update return handling to use SAV pattern
7. Test with simple_wait.actor.cpp
