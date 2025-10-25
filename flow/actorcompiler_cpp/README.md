# Actor Compiler C++ Implementation

The actor compiler is a source-to-source transpiler that converts Flow actor syntax into standard C++ state machines. This is the C++ reimplementation of the original C# version.

## Overview

**Input:** `.actor.cpp` and `.actor.h` files with Flow actor syntax  
**Output:** Standard C++ files with generated state machine code

The compiler performs the following transformations:
- `ACTOR` functions → State machine classes
- `wait()` statements → Callback-based continuations
- `state` variables → Class member variables
- `choose/when` statements → Callback registration

## Architecture

### Components

1. **Tokenizer** (`Tokenizer.h/cpp`)
   - Lexical analysis
   - Splits input into tokens using regex patterns
   - Tracks brace/paren depth for parsing

2. **Parser** (`ActorParser.h/cpp`)
   - Builds abstract syntax tree (AST) from tokens
   - Handles actor declarations, statements, expressions
   - Manages nested structures (loops, if/else, try/catch)

3. **Compiler** (`ActorCompiler.h/cpp`)
   - Generates C++ code from AST
   - Creates state machine classes
   - Manages continuations and callbacks
   - Handles error propagation

4. **AST** (`ParseTree.h`)
   - Abstract syntax tree node definitions
   - Statement types (loops, conditionals, waits, etc.)
   - Actor and function declarations

5. **Support** (`Error.h`, `Token.h`, `Function.h`, `Context.h`)
   - Error handling
   - Token and range utilities
   - Code generation helpers

## Usage

```bash
actorcompiler_cpp input.actor.cpp output.cpp [options]

Options:
  --disable-diagnostics  Suppress warning messages
  --generate-probes      Generate instrumentation probes
```

## Building

The actor compiler is built as part of the FoundationDB build process:

```bash
cmake -S src/foundationdb/flow/actorcompiler_cpp/ -B ac -G Ninja
ssh jzhou-dev.okteto ninja -C ac
```

### Requirements

- C++17 or later
- CMake 3.13+
- OpenSSL (for SHA256 hashing)

## Development Status

This is an incremental conversion from C#. Implementation steps:

- [x] Step 1: Project setup and skeleton (COMPLETE)
- [ ] Step 2: ParseTree implementation
- [ ] Step 3: Tokenization
- [ ] Step 4: Parsing logic
- [ ] Step 5: Code generation
- [ ] Step 6: Entry point finalization
- [ ] Step 7: Build integration
- [ ] Step 8: Testing & validation
- [ ] Step 9: Cleanup & documentation

See `CPP_CONVERSION_PLAN.md` for detailed conversion plan.

## Testing

To test the actor compiler:

```bash
# Compile a test actor
./actorcompiler_cpp tests/simple.actor.cpp tests/simple.cpp

# Compare with C# version
./actorcompiler tests/simple.actor.cpp tests/simple_cs.cpp
diff tests/simple.cpp tests/simple_cs.cpp
```

## Implementation Notes

### Memory Management
- AST nodes use `std::unique_ptr` for ownership
- Strings use `std::string` and `std::string_view`
- RAII for all resources (files, streams, etc.)

### String Processing
- `std::regex` for tokenization patterns
- `std::ostringstream` for code generation
- `std::filesystem` for file operations

### Error Handling
- Custom `Error` exception with line numbers
- Graceful cleanup on errors
- Detailed error messages for users

## See Also

- `flow/README.md` - Flow actor framework documentation
- `CPP_CONVERSION_PLAN.md` - Detailed conversion plan
- Original C# implementation in `flow/actorcompiler/`
