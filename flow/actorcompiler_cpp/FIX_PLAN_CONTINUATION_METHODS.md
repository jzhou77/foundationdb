# Fix Plan: Implement C# Continuation Method Pattern

Remember, check C# implementation for reference.

## Problem Summary

The current C++ actor compiler generates code with goto/label pattern for resume, while the C# version (which is the correct reference) uses a **continuation method pattern**. This causes fundamental architectural differences.

## Critical Differences

### Current C++ Pattern (WRONG)
```cpp
int body(int loopDepth) {
    // Resume switch
    if (actor_wait_state > 0) {
        switch (actor_wait_state) {
            case 1: goto resume_1;
        }
    }
    
    // Wait setup
    StrictFuture<int> __when_expr = f;
    if (__when_expr.isReady()) {
        x = __when_expr.get();
        goto cont1;
    }
    actor_wait_state = 1;
    __when_expr.addCallbackAndClear(...);
    return 0;
    
    // Resume label
    resume_1:
    actor_wait_state = 0;
    cont1:
    return x;  // Direct return
}
```

### C# Pattern (CORRECT)
```cpp
int a_body1(int loopDepth=0) {
    try {
        if (actor_wait_state < 0) return a_body1Catch1(actor_cancelled(), loopDepth);
        
        StrictFuture<int> __when_expr_0 = f;
        if (__when_expr_0.isReady()) {
            if (__when_expr_0.isError()) return a_body1Catch1(__when_expr_0.getError(), loopDepth);
            else return a_body1when1(__when_expr_0.get(), loopDepth);
        }
        
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

int a_body1when1(int const& __x, int loopDepth) {
    x = __x;
    loopDepth = a_body1cont1(loopDepth);
    return loopDepth;
}

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

void a_exitChoose1() {
    if (actor_wait_state > 0) actor_wait_state = 0;
    ActorCallback<...>::remove();
}

void a_callback_fire(..., int const& value) {
    a_exitChoose1();
    try {
        a_body1when1(value, 0);
    } catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}
```

## Key Insight: The Continuation Method Pattern

The C# compiler doesn't use goto/labels. Instead:

1. **Split code at each wait point** into separate methods:
   - `a_body1()` - code up to first wait
   - `a_body1when1()` - receives wait result
   - `a_body1cont1()` - code after first wait
   - Pattern repeats for nested waits: `a_body2()`, `a_body2when1()`, `a_body2cont1()`, etc.

2. **Callbacks invoke continuation methods**:
   - `a_callback_fire()` calls `a_exitChoose1()`, then `a_body1when1()`
   - No resume switch needed - callback directly invokes the right method

3. **Exit methods handle cleanup**:
   - `a_exitChoose1()` resets state and removes callbacks

4. **Returns use SAV pattern**:
   - Placement new into `SAV<T>::value()`
   - Call `finishSendAndDelPromiseRef()`
   - Methods return loopDepth, not actual values

## Implementation Plan

### Phase 1: Understand Code Generation Flow (Investigation)

**Goal:** Map out how ActorCompiler.cpp currently generates code.

**Tasks:**
1. Find where `body()` method is generated
2. Identify where wait statements are compiled
3. Locate goto/label generation code
4. Understand how resume logic works currently
5. Map the AST structure for wait statements

**Files to examine:**
- `ActorCompiler.cpp`: Main compilation logic
- Look for: "body", "resume", "goto", "label", "wait"

**Expected findings:**
- Method that generates body() function
- Code that emits goto/labels for resume
- How wait expressions are transformed

### Phase 2: Design Continuation Method Architecture

**Goal:** Design how continuation methods will be generated.

**Key Design Questions:**

1. **Method Naming:**
   - Body levels: `a_body1`, `a_body2`, ... (based on nesting depth)
   - When methods: `a_body1when1`, `a_body1when2`, ... (one per wait in that body)
   - Cont methods: `a_body1cont1`, `a_body1cont2`, ... (code after each wait)
   - Exit methods: `a_exitChoose1`, `a_exitChoose2`, ...

2. **Code Splitting:**
   ```
   Original actor code:
   {
       statement1;
       state int x = wait(future1);  // Split point 1
       statement2;
       state int y = wait(future2);  // Split point 2
       statement3;
       return result;
   }
   
   Generated methods:
   a_body1() {
       statement1;
       // Setup wait for future1
       return (ready) ? a_body1when1(value) : suspend;
   }
   
   a_body1when1(int x) {
       this->x = x;
       return a_body1cont1();
   }
   
   a_body1cont1() {
       statement2;
       // Setup wait for future2
       return (ready) ? a_body1when2(value) : suspend;
   }
   
   a_body1when2(int y) {
       this->y = y;
       return a_body1cont2();
   }
   
   a_body1cont2() {
       statement3;
       // Return handling
       new (&SAV<T>::value()) T(result);
       finishSendAndDelPromiseRef();
       return 0;
   }
   ```

3. **When Method Pattern:**
   - Two overloads: `const&` and `&&`
   - Store result in state variable
   - Call continuation method
   
4. **Exit Method Pattern:**
   - Reset `actor_wait_state` to 0
   - Call `ActorCallback<...>::remove()`

5. **Callback Updates:**
   - `a_callback_fire()`: call exit, then when method, wrapped in try/catch
   - `a_callback_error()`: call exit, then catch method, wrapped in try/catch

### Phase 3: Implement Body Method Transformation

**Goal:** Change body() to a_body1() with try/catch wrapper.

**Changes:**

1. **Rename method:**
   - `body()` → `a_body1()`
   - Default parameter: `int loopDepth=0`

2. **Add try/catch wrapper:**
   ```cpp
   int a_body1(int loopDepth=0) {
       try {
           // Actor code here
       } catch (Error& error) {
           loopDepth = a_body1Catch1(error, loopDepth);
       } catch (...) {
           loopDepth = a_body1Catch1(unknown_error(), loopDepth);
       }
       return loopDepth;
   }
   ```

3. **Remove resume switch/goto:**
   - No more `if (actor_wait_state > 0) { switch... goto... }`
   - Resume happens via callbacks calling continuation methods

4. **Add cancellation check:**
   ```cpp
   if (static_cast<ActorType*>(this)->actor_wait_state < 0)
       return a_body1Catch1(actor_cancelled(), loopDepth);
   ```

5. **Transform wait expressions:**
   ```cpp
   // OLD:
   if (__when_expr.isReady()) {
       x = __when_expr.get();
       goto cont1;
   }
   
   // NEW:
   if (__when_expr_0.isReady()) {
       if (__when_expr_0.isError())
           return a_body1Catch1(__when_expr_0.getError(), loopDepth);
       else
           return a_body1when1(__when_expr_0.get(), loopDepth);
   }
   ```

### Phase 4: Generate Continuation Methods (CRITICAL)

**Goal:** Generate a_body1contN() methods for code after each wait.

**Algorithm:**

For each wait statement in a body:
1. Create `a_body{level}cont{waitNum}(int loopDepth)` method
2. Method contains code from after the wait to the next wait (or end)
3. If this is the last continuation (no more waits), handle return value

**Example generation:**

```cpp
// Input actor:
ACTOR Future<int> example(Future<int> a, Future<int> b) {
    state int x = wait(a);
    state int y = wait(b);
    return x + y;
}

// Generated:
int a_body1(int loopDepth=0) {
    try {
        // Setup wait for 'a'
        StrictFuture<int> __when_expr_0 = a;
        if (__when_expr_0.isReady()) {
            if (__when_expr_0.isError()) return a_body1Catch1(__when_expr_0.getError(), loopDepth);
            else return a_body1when1(__when_expr_0.get(), loopDepth);
        }
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

int a_body1cont1(int loopDepth) {
    // Code after first wait
    try {
        // Setup wait for 'b'
        StrictFuture<int> __when_expr_1 = b;
        if (__when_expr_1.isReady()) {
            if (__when_expr_1.isError()) return a_body1Catch1(__when_expr_1.getError(), loopDepth);
            else return a_body1when2(__when_expr_1.get(), loopDepth);
        }
        actor_wait_state = 2;
        __when_expr_1.addCallbackAndClear(...);
        loopDepth = 0;
    } catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}

int a_body1cont2(int loopDepth) {
    // Final continuation - handle return
    try {
        if (!static_cast<ExampleActor*>(this)->SAV<int>::futures) {
            (void)(x + y);
            this->~ExampleActorState();
            static_cast<ExampleActor*>(this)->destroy();
            return 0;
        }
        new (&static_cast<ExampleActor*>(this)->SAV<int>::value()) int(x + y);
        this->~ExampleActorState();
        static_cast<ExampleActor*>(this)->finishSendAndDelPromiseRef();
        return 0;
    } catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

**Key Implementation Points:**
- Track which continuation number we're on (1, 2, 3, ...)
- Split code at each wait boundary
- Final continuation handles return value with SAV pattern

### Phase 5: Generate When Methods

**Goal:** Generate a_body1whenN() methods that receive wait results.

**Pattern:**
```cpp
// const& overload
int a_body1when{N}({Type} const& __{varName}, int loopDepth) {
    {varName} = __{varName};
    loopDepth = a_body1cont{N}(loopDepth);
    return loopDepth;
}

// && overload
int a_body1when{N}({Type} && __{varName}, int loopDepth) {
    {varName} = std::move(__{varName});
    loopDepth = a_body1cont{N}(loopDepth);
    return loopDepth;
}
```

**When to generate:**
- One pair of when methods per wait statement
- Type comes from the Future<Type> being waited on
- Variable name comes from the state variable being assigned

**Example:**
```cpp
// For: state int x = wait(f);

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

### Phase 6: Generate Exit Choose Methods

**Goal:** Generate a_exitChooseN() cleanup methods.

**Pattern:**
```cpp
void a_exitChoose{N}() {
    if (static_cast<ActorType*>(this)->actor_wait_state > 0) 
        static_cast<ActorType*>(this)->actor_wait_state = 0;
    static_cast<ActorType*>(this)->ActorCallback< ActorType, {callbackIndex}, {Type} >::remove();
}
```

**When to generate:**
- One per wait statement (same as when methods)
- callbackIndex matches the callback used for that wait

### Phase 7: Update Callback Methods

**Goal:** Modify a_callback_fire and a_callback_error to use continuation pattern.

**a_callback_fire pattern:**
```cpp
void a_callback_fire(ActorCallback< ActorType, {idx}, {Type} >*, {Type} const& value) {
    #ifdef WITH_ACAC
    static constexpr ActorBlockIdentifier __identifier = UID(...);
    ActorExecutionContextHelper __helper(...);
    #endif
    a_exitChoose{N}();
    try {
        a_body1when{N}(value, 0);
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}

// Move overload
void a_callback_fire(ActorCallback< ActorType, {idx}, {Type} >*, {Type} && value) {
    #ifdef WITH_ACAC
    static constexpr ActorBlockIdentifier __identifier = UID(...);
    ActorExecutionContextHelper __helper(...);
    #endif
    a_exitChoose{N}();
    try {
        a_body1when{N}(std::move(value), 0);
    }
    catch (Error& error) {
        a_body1Catch1(error, 0);
    } catch (...) {
        a_body1Catch1(unknown_error(), 0);
    }
}
```

**a_callback_error pattern:**
```cpp
void a_callback_error(ActorCallback< ActorType, {idx}, {Type} >*, Error err) {
    #ifdef WITH_ACAC
    static constexpr ActorBlockIdentifier __identifier = UID(...);
    ActorExecutionContextHelper __helper(...);
    #endif
    a_exitChoose{N}();
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

**Key changes:**
- Call `a_exitChoose{N}()` first
- Call `a_body1when{N}()` with result (not body with goto)
- Wrap in try/catch

### Phase 8: Generate Catch Methods

**Goal:** Generate a_body1CatchN() error handler methods.

**Pattern:**
```cpp
int a_body1Catch{N}(Error error, int loopDepth=0) {
    this->~ActorState();
    static_cast<ActorType*>(this)->sendErrorAndDelPromiseRef(error);
    loopDepth = 0;
    return loopDepth;
}
```

**When to generate:**
- One per try block (usually one per body method)
- Catch level matches body level

### Phase 9: Implement SAV Return Pattern (CRITICAL)

**Goal:** Replace direct returns with SAV placement new pattern.

**Current (WRONG):**
```cpp
return x;  // Direct return of value
```

**Correct:**
```cpp
// Check if anyone is waiting
if (!static_cast<ActorType*>(this)->SAV<ReturnType>::futures) { 
    (void)(returnExpr);
    this->~ActorState(); 
    static_cast<ActorType*>(this)->destroy(); 
    return 0; 
}

// Place return value in SAV
new (&static_cast<ActorType*>(this)->SAV<ReturnType>::value()) ReturnType(returnExpr);

// Cleanup and finish
this->~ActorState();
static_cast<ActorType*>(this)->finishSendAndDelPromiseRef();
return 0;
```

**Key points:**
- Actor body methods return `int` (loopDepth), not the actor's return type
- Actual return value placed in `SAV<T>::value()` via placement new
- Must check if `SAV<T>::futures` is null (no waiters)
- Call `finishSendAndDelPromiseRef()` to complete promise

**For Void actors:**
```cpp
if (!static_cast<ActorType*>(this)->SAV<Void>::futures) { 
    this->~ActorState(); 
    static_cast<ActorType*>(this)->destroy(); 
    return 0; 
}
this->~ActorState();
static_cast<ActorType*>(this)->finishSendAndDelPromiseRef();
return 0;
```

### Phase 10: Handle Special Cases

**Choose/When blocks:**
- Generate separate when methods for each choice
- exitChoose removes all callbacks from all branches

**Loops:**
- Loop bodies are separate a_bodyN methods
- Break becomes return from loop body
- Continue becomes recursive call to loop body

**Nested try/catch:**
- Multiple catch levels: a_body1Catch1, a_body1Catch2, ...
- Inner catches call outer catches on rethrow

## Implementation Order

### Step 1: Investigation (1-2 hours)
- Map current code generation in ActorCompiler.cpp
- Identify all places that need changes
- Create detailed checklist

### Step 2: Body Method Transform (2-3 hours)
- Rename body → a_body1
- Add try/catch wrapper
- Remove goto/label code
- Add cancellation check
- Transform wait expressions to call when methods

### Step 3: Continuation Methods (3-4 hours)
- Implement code splitting at wait points
- Generate a_body1contN methods
- Ensure proper code flow through continuations

### Step 4: When Methods (1-2 hours)
- Generate when methods with both overloads
- Store results in state variables
- Call continuation methods

### Step 5: Exit Methods (1 hour)
- Generate exitChoose methods
- Add cleanup code

### Step 6: Callback Updates (2 hours)
- Update callback_fire to call exit+when
- Update callback_error to call exit+catch
- Add try/catch wrappers

### Step 7: Catch Methods (1 hour)
- Generate catch methods
- Call sendErrorAndDelPromiseRef

### Step 8: SAV Return Pattern (2-3 hours)
- Replace all return statements
- Implement SAV placement new
- Handle Void returns

### Step 9: Testing (2-3 hours)
- Test with simple_wait.actor.cpp
- Compare output to C# version
- Fix any differences

### Step 10: Extended Testing (4-6 hours)
- Test with multiple_waits
- Test with choose_when
- Test with try_catch
- Test with loop_with_wait

## Total Estimated Effort: 20-30 hours

## Success Criteria

1. Generated code matches C# structure
2. simple_wait.actor.cpp generates identical output
3. All test actors compile successfully
4. Generated code compiles with Flow headers
5. Test harness links and runs

## Risks and Challenges

1. **Code splitting complexity:** Breaking code at wait points without losing context
2. **Variable scoping:** Ensuring state variables accessible in continuations
3. **Error propagation:** Try/catch across method boundaries
4. **Type information:** Need accurate types for when method parameters
5. **Line number mapping:** Maintaining source line info in #line directives

## Next Actions

1. ✅ Create this plan document
2. ⏳ Examine ActorCompiler.cpp code generation
3. ⏳ Create detailed implementation checklist
4. ⏳ Begin implementation Phase 2
