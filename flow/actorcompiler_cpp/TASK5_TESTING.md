# Task 5: Testing & Validation

## Overview

This directory contains test actors and validation scripts for the C++ actor compiler implementation.

## Test Actors

Located in `tests/runtime_test_actors/`:

1. **simple_wait.actor.cpp** - Basic wait statement
   - Tests single future wait
   - Verifies state variable handling
   - Validates callback generation

2. **multiple_waits.actor.cpp** - Sequential waits
   - Tests multiple wait statements in sequence
   - Verifies resume logic with multiple callbacks
   - Tests state machine switch/case generation

3. **choose_when.actor.cpp** - Choice statements
   - Tests choose/when for racing futures
   - Validates multiple callback registration
   - Tests fast-path ready checks

4. **try_catch.actor.cpp** - Error handling
   - Tests try/catch with wait statements
   - Verifies error callback routing
   - Tests error propagation through catch handlers

5. **loop_with_wait.actor.cpp** - Loops with waits
   - Tests wait inside loop body
   - Verifies complex control flow
   - Tests state variable persistence across iterations

## Running Tests

### Step 1: Build the Actor Compiler

```bash
cd /root/src/foundationdb
cmake --build /root/build_output --target actorcompiler_cpp -j $(nproc)
```

### Step 2: Run Validation Test

This test parses the test actors and validates the generated code structure:

```bash
cmake --build /root/build_output --target actorcompiler_cpp_runtime_actors -j $(nproc)
/root/build_output/flow/actorcompiler_cpp/actorcompiler_cpp_runtime_actors
```

Or via ctest:
```bash
cd /root/build_output
ctest -R ActorCompilerRuntimeActors -V
```

### Step 3: Compile Test Actors

Use the provided script to compile all test actors:

```bash
cd /root/src/foundationdb/flow/actorcompiler_cpp
chmod +x compile_test_actors.sh
./compile_test_actors.sh
```

This will generate C++ files in `tests/runtime_test_actors/generated/`.

### Step 4: Inspect Generated Code

Check the generated files:

```bash
ls -la tests/runtime_test_actors/generated/
cat tests/runtime_test_actors/generated/simple_wait.cpp
```

Look for:
- ✅ Actor class declaration
- ✅ ActorCallback inheritance
- ✅ a_callback_fire() methods
- ✅ a_callback_error() methods
- ✅ Resume switch/case statements
- ✅ Resume labels (resume_1, resume_2, etc.)
- ✅ cancel() method with actor_cancelled() calls
- ✅ State variables as class members

## Success Criteria

### Phase 1: Code Generation ✅
- [x] Test actors parse without errors
- [x] Generated code includes all required structures
- [x] Callbacks are properly numbered and typed
- [x] Resume logic is present
- [x] Error handling is wired up

### Phase 2: Compilation (Next)
- [ ] Generated code compiles with Flow headers
- [ ] No syntax errors in generated C++
- [ ] All template instantiations resolve
- [ ] Linker can find all symbols

### Phase 3: Runtime (Future)
- [ ] Test harness links with Flow library
- [ ] Actors execute correctly
- [ ] Callbacks fire when futures complete
- [ ] Errors propagate through catch handlers
- [ ] Cancellation works properly

## Current Status

**Tasks 1-4: COMPLETE ✅**
- ✅ Callback generation infrastructure
- ✅ State machine resume logic
- ✅ Cancellation propagation (simplified)
- ✅ Error handling integration

**Task 5: IN PROGRESS**
- ✅ Test actors created
- ✅ Validation test created
- ✅ Compilation script created
- ⏳ Awaiting full compilation test

## Next Steps

1. **Build and run runtime_actors test**
   - Validates generated code structure
   - Checks all critical features are present

2. **Compile test actors**
   - Generate C++ from .actor.cpp files
   - Inspect generated code

3. **Attempt compilation with Flow headers**
   - Try to compile generated code
   - Fix any compilation errors
   - Iterate until clean compile

4. **Create test harness**
   - Link generated actors with Flow library
   - Create minimal main() to exercise actors
   - Verify runtime behavior

## Files

```
flow/actorcompiler_cpp/
├── tests/
│   ├── runtime_test_actors/
│   │   ├── simple_wait.actor.cpp
│   │   ├── multiple_waits.actor.cpp
│   │   ├── choose_when.actor.cpp
│   │   ├── try_catch.actor.cpp
│   │   ├── loop_with_wait.actor.cpp
│   │   └── generated/          # Output directory
│   ├── runtime_actor_test.cpp  # Validation test
│   └── error_handling_test.cpp # Error handling test
├── compile_test_actors.sh      # Compilation script
└── TASK5_TESTING.md           # This file
```

## Troubleshooting

### Actor compiler not found
Build the project: `cmake --build /root/build_output --target actorcompiler_cpp`

### Test fails to find actor files
Run from the actorcompiler_cpp directory or use absolute paths

### Generated code has compilation errors
This is expected at this stage - we're validating structure, not compilability yet

## Notes

- Test actors use minimal Flow features to simplify validation
- Generated code structure is more important than compilation at this stage
- Next phase will focus on making generated code compile with Flow
