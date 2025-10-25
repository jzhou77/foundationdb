# Loop and Control Flow Compilation - Complete

## Overview
Implemented compilation for if/else statements and all loop types (while, for, loop, range-for). These implementations generate basic control flow structures with proper label-based break/continue support.

## Implementation Details

### 1. If/Else Statement (`compileStatement(IfStatement*)`)

#### Features
- Basic if/else structure emission
- Support for `constexpr if` (C++17)
- Proper indentation and bracing

#### Generated Pattern
```cpp
if (condition) {
    // if body
}
else {
    // else body (optional)
}
```

For `constexpr if`:
```cpp
if constexpr (condition) {
    // compile-time branching
}
```

### 2. While Loop (`compileStatement(WhileStatement*)`)

#### Implementation Strategy
Transforms `while(condition) { body }` into equivalent `for(;condition;) { body }` and delegates to ForStatement compiler.

### 3. Infinite Loop (`compileStatement(LoopStatement*)`)

#### Implementation Strategy
Transforms `loop { body }` into equivalent `for(;;;) { body }` and delegates to ForStatement compiler.

### 4. For Loop (`compileStatement(ForStatement*)`)

#### Features
- Full 3-part for loop support: init, condition, next
- Label-based break/continue via `loopContext()`
- Generates loop head, continue, and break labels

#### Generated Pattern
```cpp
initExpression;

loopHead:
if (!(condExpression))
    goto breakLabel;
{
    // body (compiled with loop context)
}

continueLabel:
nextExpression;
goto loopHead;

breakLabel:
```

#### Key Design
- Loop head label for re-entry point
- Break label for loop exit
- Continue label for iteration advance
- Context provides break/continue labels to body statements

### 5. Range-Based For Loop (`compileStatement(RangeForStatement*)`)

#### Current Implementation
Simplified version that emits native C++11 range-for:
```cpp
for (rangeDecl : rangeExpression) {
    // body
}
```

Full implementation would convert to iterator-based for loop when body contains waits.

## Loop Context Integration

### Context.loopContext(breakLabel, continueLabel)
Creates a context with break/continue targets, allowing:
- **Break statements**: `goto breakLabel;`
- **Continue statements**: `goto continueLabel;`

### Example Flow
```cpp
// Input Flow code:
for (int i = 0; i < 10; ++i) {
    if (i == 5) break;
    doWork(i);
}

// Generated C++ (simplified):
int i = 0;

cont1:  // loop head
if (!(i < 10))
    goto cont3;  // break label
{
    if (i == 5)
        goto cont3;  // break
    doWork(i);
}

cont2:  // continue label
++i;
goto cont1;

cont3:  // break label
```

## Testing

### Test Coverage (`loop_compilation_test.cpp`)

**Test 1: Simple If Statement**
- Basic if without else
- Verifies condition and body emission

**Test 2: If-Else Statement**
- Full if-else structure
- Verifies both branches present

**Test 3: Constexpr If**
- Tests `if constexpr` syntax
- Verifies "constexpr" keyword emission

**Test 4: For Loop**
- 3-part for loop with init, condition, next
- Verifies label generation and loop structure

**Test 5: While Loop**
- Basic while loop
- Verifies transformation to for loop pattern

**Test 6: Infinite Loop**
- `loop { }` construct
- Verifies infinite loop structure

**Test 7: Loop with Break**
- Tests break statement integration
- Verifies loop context provides break label

**Test 8: Range-Based For Loop**
- C++11 range-for syntax
- Verifies native emission

## Current Limitations

### 1. No Continuation Handling
Current implementations are **simplified** - they don't handle waits inside bodies:
- If/else: Doesn't create continuation functions for branches
- Loops: Doesn't split loop body into continuation functions

**Full implementation needed:**
- Check `stmt->containsWait()` to detect async operations
- Create separate continuation functions for loop bodies
- Handle loop depth tracking with `loopDepth` variable
- Generate callback-based loop continuation logic

### 2. No WillContinue Check
C# implementation checks `WillContinue(body)` to decide between:
- **Native loop**: Simple C++ loop if no waits
- **Continuation loop**: Complex state machine if waits present

Current C++ always generates label-based structure (simpler but not optimal).

### 3. Range-For with Waits
Range-for should convert to iterator-based for when body has waits:
```cpp
// Need to generate:
__iter = std::begin(container);
for(; __iter != std::end(container); ++__iter) {
    auto& item = *__iter;
    // body
}
```
And store `__iter` as state variable.

### 4. No Loop Depth Tracking
Full implementation needs:
- `int loopDepth` variable for nesting
- Loop head function that increments depth
- Continuation functions that check depth
- Proper loop exit handling

## Integration Points

### With Context System
- **loopContext()**: Provides break/continue labels to nested statements
- **Break/Continue**: Use context labels for goto targets
- **If/Else**: Receives context and passes to body compilation

### With Function Registry
- **generateLabel()**: Creates unique labels (cont1, cont2, ...)
- **getFunction()**: Would create loop continuation functions (not yet used)

### With Statement Dispatcher
- All loop types added to `compile()` dispatcher
- If statement added to dispatcher
- Each delegates to appropriate `compileStatement()` overload

## Files Modified

- **ActorCompiler.h**: Added 5 new `compileStatement()` declarations (If, While, For, Loop, RangeFor)
- **ActorCompiler.cpp**:
  - Updated `compile()` dispatcher with 5 new cases
  - Implemented `compileStatement(IfStatement*)` (~30 lines)
  - Implemented `compileStatement(WhileStatement*)` (~10 lines, delegates to For)
  - Implemented `compileStatement(LoopStatement*)` (~10 lines, delegates to For)
  - Implemented `compileStatement(ForStatement*)` (~60 lines)
  - Implemented `compileStatement(RangeForStatement*)` (~15 lines)
- **tests/loop_compilation_test.cpp**: Created comprehensive test suite (350+ lines, 8 tests)
- **CMakeLists.txt**: Added loop_compilation test target

## Next Steps

### Immediate (Phase 4)
1. **Choose/When Implementation**: Handle multiple futures with callbacks
2. **Try/Catch Implementation**: Error handling with catch blocks

### Future Enhancements
1. **Add WillContinue Check**:
   - Implement `stmt->containsWait()` traversal
   - Branch between native and continuation-based loops
   - Optimize simple loops without waits

2. **Full Loop Continuation Support**:
   - Generate loop head functions
   - Generate loop body continuation functions
   - Add loop depth tracking
   - Handle break/continue with continuation passing

3. **If/Else Continuation Support**:
   - Create continuation functions for branches
   - Handle cases where branches have different reachability
   - Merge continuations after if/else

4. **Range-For with Waits**:
   - Detect waits in range-for body
   - Convert to iterator-based loop
   - Store iterator as state variable
   - Handle iterator lifetime properly

## Testing Commands

```bash
cd build
cmake --build . --target actorcompiler_cpp_loop_compilation
./flow/actorcompiler_cpp/actorcompiler_cpp_loop_compilation
```

Expected output:
```
=== Loop and Control Flow Compilation Tests ===

Test 1: Simple if statement
[... generated code ...]
✓ Test passed

Test 2: If-else statement
[... generated code ...]
✓ Test passed

[... 6 more tests ...]

All tests passed!
```

## Status Summary

**Loop and control flow compilation is now functional** for basic cases:
- ✅ If/else statements (including constexpr if)
- ✅ While loops (via for loop transformation)
- ✅ For loops (with label-based control flow)
- ✅ Infinite loops (via for loop transformation)
- ✅ Range-based for loops (native emission)
- ✅ Break/continue integration via loop context
- ⚠️ No continuation function generation (simplified)
- ⚠️ No wait detection and optimization
- ❌ Loop depth tracking not implemented
- ❌ WillContinue branching not implemented

This provides the foundation for basic control flow in actors, with clear paths for enhancement to support full async/await semantics.
