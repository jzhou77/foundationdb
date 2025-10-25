# C# to C++ Conversion Plan for FoundationDB Actor Compiler

**Date:** October 25, 2025  
**Purpose:** Convert the actorcompiler from C# to C++ to eliminate the .NET dependency

## Overview

The actorcompiler is a source-to-source transpiler that reads `.actor.h` and `.actor.cpp` files and translates Flow actor syntax into standard C++ with state machines. Currently ~3,400 lines of C# across 5 files.

### Current Project Structure

**C# Files:**
- `Program.cs` (~100 lines) - Entry point, file I/O, command-line handling
- `ActorParser.cs` (~1,200 lines) - Tokenization, parsing logic
- `ParseTree.cs` (~200 lines) - AST node definitions
- `ActorCompiler.cs` (~1,900 lines) - Code generation and compilation
- `Properties/AssemblyInfo.cs` - Metadata (will be removed)

### Target Project Structure

**C++ Files (proposed):**
- `main.cpp` - Entry point
- `ParseTree.h` - AST node definitions
- `Tokenizer.h/cpp` - Tokenization logic
- `ActorParser.h/cpp` - Parsing logic
- `ActorCompiler.h/cpp` - Code generation
- `Error.h` - Error handling
- `CMakeLists.txt` - Build configuration

---

## Step 1: Project Setup & Dependencies

**Goal:** Set up C++ project structure and choose libraries

### Tasks
1. Create new directory structure: `flow/actorcompiler_cpp/`
2. Choose key dependencies:
   - **String handling:** `std::string`, `std::string_view`
   - **Regex:** `std::regex` (C++11)
   - **File I/O:** `<fstream>`, `<filesystem>` (C++17)
   - **Hashing (SHA256):** OpenSSL or header-only lib like PicoSHA2
   - **Collections:** `std::vector`, `std::unordered_map`, `std::unordered_set`
3. Create initial `CMakeLists.txt` for the C++ actorcompiler
4. Decide on C++ standard: **C++17 minimum** (for `<filesystem>`)

### Key Decisions
- Use `std::unique_ptr` for AST node ownership
- Use `std::string` everywhere (C++17 has small string optimization)
- Target C++17 minimum, C++20 preferred
- Must compile with GCC, Clang, MSVC (all platforms FDB supports)

### Deliverable
- [ ] `flow/actorcompiler_cpp/` directory created
- [ ] `CMakeLists.txt` with basic executable target
- [ ] Empty skeleton files created
- [ ] SHA256 library decision made and integrated

**Estimated Time:** 4-8 hours

---

## Step 2: Port ParseTree.cs (Data Structures)

**Goal:** Convert AST node class definitions

### Tasks
1. Port all `Statement` subclasses:
   - `PlainOldCodeStatement`
   - `StateDeclarationStatement`
   - `WhileStatement`, `ForStatement`, `RangeForStatement`, `LoopStatement`
   - `BreakStatement`, `ContinueStatement`
   - `IfStatement`, `ReturnStatement`, `ThrowStatement`
   - `WaitStatement`, `ChooseStatement`, `WhenStatement`
   - `TryStatement`, `CodeBlock`
2. Port `VarDeclaration` struct
3. Port `Actor`, `Descr`, `Declaration` classes
4. Implement `containsWait()` virtual method for all statement types

### Key Technical Decisions
- Use inheritance hierarchy with abstract base `Statement` class
- Replace C# properties with member variables + getters
- Use `std::unique_ptr<Statement>` for ownership
- Use `std::vector<std::unique_ptr<Statement>>` for statement lists
- Consider `std::variant` for type-safe statement handling (optional)

### C# to C++ Patterns
```cpp
// C#: public virtual bool containsWait() { return false; }
// C++: virtual bool containsWait() const { return false; }

// C#: public Statement[] statements;
// C++: std::vector<std::unique_ptr<Statement>> statements;

// C#: public string name;
// C++: std::string name;
```

### Deliverable
- [ ] `ParseTree.h` with all AST node definitions
- [ ] Virtual methods properly declared
- [ ] Memory ownership clear via smart pointers

**Estimated Time:** 8-12 hours

---

## Step 3: Port Token & Tokenization (ActorParser.cs Part 1)

**Goal:** Convert tokenization logic

### Tasks
1. Port `Token` struct
   - Position, SourceLine, BraceDepth, ParenDepth
   - `IsWhitespace` property → member function
   - `Assert()` method with lambda predicates
   - `GetMatchingRangeIn()` for bracket matching
2. Port `TokenRange` class (iterator wrapper)
   - Begin/End indices
   - Iterator interface (`begin()`, `end()`)
   - LINQ-like methods: `Skip()`, `Take()`, `First()`, etc.
3. Port `Tokenize()` method
   - Convert C# `Regex.Match()` to `std::regex_search()`
   - Handle all token patterns (identifiers, operators, strings, comments)
4. Port `CountParens()` - track brace/paren depth
5. Port utility classes:
   - `AngleBracketParser::NotInsideAngleBrackets()`
   - `BracketParser::NotInsideBrackets()`

### Key Challenges

**Challenge 1: LINQ to STL Conversion**
```csharp
// C#: LINQ method chaining
var result = tokens.Where(t => t.Value == ";").Skip(1).First();

// C++: Use STL algorithms
auto it = std::find_if(tokens.begin() + 1, tokens.end(), 
                       [](const Token& t) { return t.value == ";"; });
auto& result = *it;
```

**Challenge 2: Regex Matching**
```csharp
// C#: Regex with \G anchor for position-based matching
var m = re.Match(text, pos);
if (m.Success) { yield return m.Value; }

// C++: Use regex_search with iterator
std::smatch match;
if (std::regex_search(text.begin() + pos, text.end(), match, re)) {
    tokens.push_back(match.str());
}
```

**Challenge 3: IEnumerable/yield return**
```csharp
// C#: yield return for lazy evaluation
IEnumerable<Token> Filter() {
    foreach (var tok in tokens) {
        if (tok.IsWhitespace) yield return tok;
    }
}

// C++: Return vector or use iterators
std::vector<Token> Filter() {
    std::vector<Token> result;
    std::copy_if(tokens.begin(), tokens.end(), 
                 std::back_inserter(result),
                 [](const Token& t) { return t.isWhitespace(); });
    return result;
}
```

### Deliverable
- [ ] `Token.h` with Token struct
- [ ] `TokenRange.h` with range/iterator utilities
- [ ] `Tokenizer.h/cpp` with tokenization logic
- [ ] Unit tests for tokenization (optional but recommended)

**Estimated Time:** 12-16 hours

---

## Step 4: Port Parsing Logic (ActorParser.cs Part 2)

**Goal:** Convert statement/actor parsing

### Tasks
1. Port `ActorParser` class constructor
   - Initialize with text, sourceFile, errorMessagePolicy
   - Call `Tokenize()` and `CountParens()`
2. Port parsing helper methods:
   - `SplitParameterList()` - split by delimiter respecting depth
   - `NormalizeWhitespace()` - collapse whitespace
   - `ParseDeclaration()` - extract type, name, initializer
   - `ParseVarDeclaration()` - parse variable declarations
3. Port statement parsers (each creates AST nodes):
   - `ParseLoopStatement()` - infinite loop
   - `ParseWhileStatement()` - while loop
   - `ParseForStatement()` - for loop and range-for
   - `ParseIfStatement()` / `ParseElseStatement()` - if/else
   - `ParseTryStatement()` / `ParseCatchStatement()` - try/catch
   - `ParseChooseStatement()` / `ParseWhenStatement()` - actor choose/when
   - `ParseWaitStatement()` - wait on futures
   - `ParseStateDeclaration()` - state variables
   - `ParseReturnStatement()` / `ParseThrowStatement()` - return/throw
4. Port `ParseCompoundStatement()` and `ParseCodeBlock()`
5. Port `ParseActor()` and `ParseDescr()`
6. Port actor/test case heading parsers:
   - `ParseActorHeading()` - extract return type, name, parameters
   - `ParseTestCaseHeading()` - handle TEST_CASE macro
   - `ParseClassContext()` - track enclosing class for nested actors
7. Port `Write()` method - main entry point that iterates tokens

### Key Challenges

**Challenge 1: Pattern Matching on Tokens**
```csharp
// C#: switch on token value
switch (toks.First().Value) {
    case "loop": Add(ParseLoopStatement(toks)); break;
    case "while": Add(ParseWhileStatement(toks)); break;
    // ...
}

// C++: Same pattern works
switch (hash(toks.first().value)) {
    case hash("loop"): statements.push_back(parseLoopStatement(toks)); break;
    case hash("while"): statements.push_back(parseWhileStatement(toks)); break;
}
// Or use if-else chain for string comparison
```

**Challenge 2: FirstOrDefault → std::optional**
```csharp
// C#: Returns null if not found
Token comma = tokens.FirstOrDefault(t => t.Value == ",");
if (comma != null) { /* use comma */ }

// C++: Use std::optional
std::optional<Token> comma = findFirst(tokens, 
                                       [](const Token& t) { return t.value == ","; });
if (comma) { /* use comma.value() */ }
```

**Challenge 3: Recursive AST Construction**
```csharp
// C#: Direct object construction
return new WhileStatement {
    expression = str(NormalizeWhitespace(expr)),
    body = ParseCompoundStatement(range(expr.End+1, toks.End))
};

// C++: Use make_unique
auto stmt = std::make_unique<WhileStatement>();
stmt->expression = str(normalizeWhitespace(expr));
stmt->body = parseCompoundStatement(range(expr.end()+1, toks.end()));
return stmt;
```

### Deliverable
- [ ] `ActorParser.h` with class declaration
- [ ] `ActorParser.cpp` with all parsing methods
- [ ] Error handling with `Error` exception class
- [ ] `Write()` method that drives the compilation

**Estimated Time:** 20-30 hours

---

## Step 5: Port Code Generation (ActorCompiler.cs)

**Goal:** Convert the most complex file - code generation

### Tasks
1. Port core classes:
   - `Context` struct - compilation context (target, next, break, continue functions)
   - `Function` class - represents generated C++ functions with body text
   - `StateVar` / `CallbackVar` - state variable tracking
   - `TypeSwitch<R>` - pattern matching helper (optional, can use std::visit)
2. Port `ActorCompiler` class constructor:
   - Initialize with Actor, sourceFile, flags
   - Call `FindState()` to extract state variables
3. Port state management:
   - `FindState()` - extract state from actor parameters and body
   - Track callbacks for choose/when statements
4. Port compilation dispatch:
   - `CompileStatement()` overloads for each statement type (use virtual dispatch or visitor)
   - `Compile()` - main compilation loop over CodeBlock
   - `TryCatchCompile()` - wrap in try/catch
5. Port statement compilers:
   - `CompileStatement(PlainOldCodeStatement)` - write code directly
   - `CompileStatement(StateDeclarationStatement)` - add to state list
   - `CompileStatement(ForStatement)` - loop compilation with continuations
   - `CompileStatement(WhileStatement)` / `CompileStatement(LoopStatement)`
   - `CompileStatement(IfStatement)` - conditional compilation
   - `CompileStatement(ChooseStatement)` - generate callbacks for when clauses
   - `CompileStatement(WaitStatement)` - wait statement handling
   - `CompileStatement(TryStatement)` / `CompileStatement(ThrowStatement)`
   - `CompileStatement(BreakStatement)` / `CompileStatement(ContinueStatement)`
   - `CompileStatement(ReturnStatement)` - actor return
6. Port output generation:
   - `Write()` - main entry point, generates full actor class
   - `WriteActorFunction()` - wrapper function that creates actor
   - `WriteActorClass()` - actor state machine class
   - `WriteStateConstructor()` / `WriteStateDestructor()`
   - `WriteConstructor()` / `WriteCancelFunc()`
   - `WriteFunctions()` - output all generated functions
   - `WriteTemplate()` - template declarations
7. Port helper methods:
   - `getFunction()` - create or reuse Function object
   - `LineNumber()` - output #line directives
   - `TryCatch()` - wrap code in try/catch
   - `EmitNativeLoop()` - emit C++ loop without continuations
   - `ProbeEnter()` / `ProbeExit()` - instrumentation probes
8. Port UID generation:
   - `GetUidFromString()` - SHA256 hash for actor identifiers
   - `ByteToLong()` - convert hash bytes to uint64

### Key Challenges

**Challenge 1: Reflection-Based Dispatch**
```csharp
// C#: Uses reflection to dispatch on statement type
var method = typeof(ActorCompiler).GetMethod("CompileStatement", 
    /* binding flags */, null, new Type[] { stmt.GetType(), typeof(Context) }, null);
method.Invoke(this, new object[] { stmt, cx });

// C++: Use virtual dispatch or std::visit
// Option 1: Virtual dispatch
stmt->compile(*this, cx);

// Option 2: Visitor pattern
class CompileVisitor {
    void operator()(PlainOldCodeStatement& stmt) { /* ... */ }
    void operator()(StateDeclarationStatement& stmt) { /* ... */ }
    // ...
};
std::visit(CompileVisitor{this, cx}, stmt);

// Option 3: Type-based dispatch (if using std::variant)
if (auto* plain = std::get_if<PlainOldCodeStatement>(&stmt)) {
    compileStatement(*plain, cx);
} else if (auto* state = std::get_if<StateDeclarationStatement>(&stmt)) {
    compileStatement(*state, cx);
}
```

**Challenge 2: StringWriter / MemoryStream**
```csharp
// C#: Write to memory, then read back
body = new StreamWriter(new MemoryStream());
body.WriteLine("code");
// Later...
body.Flush();
body.BaseStream.Position = 0;
return new StreamReader(body.BaseStream).ReadToEnd();

// C++: Use std::ostringstream
std::ostringstream body;
body << "code\n";
// Later...
return body.str();
```

**Challenge 3: Function Overloading for Move Semantics**
```csharp
// C#: Overload handling
func.addOverload("Type&&", "int loopDepth");

// C++: Generate both const& and && overloads
class Function {
    std::vector<std::string> formalParameters;
    std::unique_ptr<Function> overload;  // Move overload
    
    void addOverload(std::vector<std::string> params) {
        overload = std::make_unique<Function>();
        overload->formalParameters = std::move(params);
    }
};
```

**Challenge 4: SHA256 Hashing**
```csharp
// C#: Built-in SHA256
byte[] sha256Hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(str));

// C++: Use OpenSSL or PicoSHA2
#include <openssl/sha.h>
std::array<uint8_t, SHA256_DIGEST_LENGTH> hash;
SHA256(reinterpret_cast<const uint8_t*>(str.data()), str.size(), hash.data());

// Or with PicoSHA2 (header-only):
#include "picosha2.h"
std::vector<uint8_t> hash(picosha2::k_digest_size);
picosha2::hash256(str.begin(), str.end(), hash.begin(), hash.end());
```

### Deliverable
- [ ] `ActorCompiler.h` with class declaration
- [ ] `ActorCompiler.cpp` with all compilation methods
- [ ] `Function.h` for code generation helper
- [ ] `Context.h` for compilation context
- [ ] Correct code generation verified against C# output

**Estimated Time:** 30-40 hours (most complex step)

---

## Step 6: Port Entry Point (Program.cs)

**Goal:** Convert main() and file handling

### Tasks
1. Port `Main()` function:
   - Command-line argument parsing
   - Error handling and exit codes
   - Console output formatting
2. Port file I/O operations:
   - `File.ReadAllText()` → read entire file to string
   - `File.WriteAllText()` → write string to file
   - Atomic file replacement via temporary file
3. Port `OverwriteByMove()` utility:
   - Create temporary file (`.tmp` suffix)
   - Write content
   - Atomically replace target file
   - Set file attributes (read-only on success)
4. Generate `.uid` file output:
   - Write actor identifier mappings
   - Format: `uid_high|uid_low|actor_name`
5. Port `ErrorMessagePolicy` handling:
   - `--disable-diagnostics` flag
   - `--generate-probes` flag

### Key Challenges

**Challenge 1: File I/O**
```csharp
// C#: Simple file operations
var inputData = File.ReadAllText(input);
File.WriteAllText(output, result);

// C++: Use fstream
std::string readFile(const std::string& path) {
    std::ifstream file(path);
    if (!file) throw std::runtime_error("Cannot open file");
    return std::string(std::istreambuf_iterator<char>(file),
                      std::istreambuf_iterator<char>());
}

void writeFile(const std::string& path, const std::string& content) {
    std::ofstream file(path);
    if (!file) throw std::runtime_error("Cannot write file");
    file << content;
}
```

**Challenge 2: Atomic File Replacement**
```csharp
// C#: File attributes and atomic move
if (File.Exists(target)) {
    File.SetAttributes(target, FileAttributes.Normal);
    File.Delete(target);
}
File.Move(temporaryFile, target);
File.SetAttributes(target, FileAttributes.ReadOnly);

// C++: Use std::filesystem
namespace fs = std::filesystem;
if (fs::exists(target)) {
    fs::permissions(target, fs::perms::owner_write, fs::perm_options::add);
    fs::remove(target);
}
fs::rename(temporaryFile, target);
fs::permissions(target, fs::perms::owner_read | fs::perms::group_read | 
                        fs::perms::others_read);
```

**Challenge 3: Command-Line Parsing**
```csharp
// C#: Array operations
if (args.Length < 2) { /* error */ }
foreach (var arg in args) {
    if (arg.StartsWith("--")) { /* process flag */ }
}

// C++: Simple loop
if (argc < 3) { /* error */ }
for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.starts_with("--")) { /* process flag */ }
}
```

### Deliverable
- [ ] `main.cpp` with entry point
- [ ] File I/O utilities
- [ ] Command-line parsing
- [ ] Error handling and exit codes
- [ ] `.uid` file generation

**Estimated Time:** 6-10 hours

---

## Step 7: Build System Integration

**Goal:** Integrate C++ actorcompiler into CMake

### Tasks
1. Create `cmake/CompileActorCompilerCpp.cmake`:
   - Add C++ executable target
   - Link required libraries (OpenSSL for SHA256, or use header-only)
   - Set C++17 standard
   - Handle platform differences (Windows, Linux, macOS)
2. Update `cmake/CompileActorCompiler.cmake`:
   - Add option to choose C# or C++ implementation
   - Default to C++ once validated
3. Keep both implementations temporarily:
   - `actorcompiler` (C# via Mono/dotnet)
   - `actorcompiler_cpp` (new C++ version)
4. Update `cmake/FlowCommands.cmake`:
   - Make actor compilation use new actorcompiler_cpp
   - Ensure all `.actor.cpp` files are processed correctly
5. Test build on all platforms:
   - Linux (GCC, Clang)
   - macOS (Clang)
   - Windows (MSVC)

### CMakeLists.txt Structure
```cmake
# flow/actorcompiler_cpp/CMakeLists.txt
cmake_minimum_required(VERSION 3.13)

add_executable(actorcompiler_cpp
    main.cpp
    Tokenizer.cpp
    ActorParser.cpp
    ActorCompiler.cpp
)

target_include_directories(actorcompiler_cpp PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_compile_features(actorcompiler_cpp PRIVATE cxx_std_17)

# Link OpenSSL for SHA256 (or use header-only alternative)
find_package(OpenSSL REQUIRED)
target_link_libraries(actorcompiler_cpp PRIVATE OpenSSL::Crypto)

# Platform-specific settings
if(WIN32)
    # Windows-specific flags
elseif(APPLE)
    # macOS-specific flags
else()
    # Linux-specific flags
endif()
```

### Deliverable
- [ ] `flow/actorcompiler_cpp/CMakeLists.txt` created
- [ ] `cmake/CompileActorCompilerCpp.cmake` created
- [ ] Build succeeds on all platforms
- [ ] Both C# and C++ versions available for comparison

**Estimated Time:** 4-8 hours

---

## Step 8: Testing & Validation

**Goal:** Ensure output equivalence between C# and C++ implementations

### Testing Strategy

**Phase 1: Unit Tests (Optional but Recommended)**
- Test tokenization on simple inputs
- Test parsing of individual statements
- Test code generation for minimal actors

**Phase 2: Integration Tests**
1. Run C++ actorcompiler on test inputs:
   - Create `tests/simple_actor.actor.cpp` with basic actor
   - Create `tests/complex_actor.actor.cpp` with choose/when, try/catch
   - Run both C# and C++ versions
2. Compare outputs line-by-line:
   - Ignore `#line` directive line numbers (may differ)
   - Ignore whitespace differences
   - Verify functional equivalence
3. Test on subset of real FDB actors:
   - `flow/` directory actors
   - `fdbclient/` directory actors
   - `fdbserver/` directory actors

**Phase 3: Full Build Test**
1. Configure CMake to use `actorcompiler_cpp`
2. Clean build FoundationDB from scratch
3. Verify all `.actor.cpp` files compile
4. Check for any new compiler warnings/errors

**Phase 4: Functional Testing**
1. Run FoundationDB test suite:
   - `ctest -L fast` - Quick smoke tests
   - Full test suite if possible
2. Verify simulation tests pass
3. Performance comparison:
   - Measure compilation time (C++ should be faster)
   - Measure memory usage during compilation

### Validation Commands
```bash
# Compare outputs
./actorcompiler input.actor.cpp output.cpp
./actorcompiler_cpp input.actor.cpp output_cpp.cpp
diff -u output.cpp output_cpp.cpp

# Batch test all actors
find . -name "*.actor.cpp" | while read f; do
    ./actorcompiler "$f" "/tmp/cs_output.cpp"
    ./actorcompiler_cpp "$f" "/tmp/cpp_output.cpp"
    diff -q "/tmp/cs_output.cpp" "/tmp/cpp_output.cpp" || echo "DIFF: $f"
done

# Build with new compiler
mkdir build_cpp
cd build_cpp
cmake -DUSE_CPP_ACTORCOMPILER=ON ..
make -j$(nproc)
```

### Deliverable
- [ ] All test actors compile identically
- [ ] Full FDB build succeeds with C++ actorcompiler
- [ ] Test suite passes (no regressions)
- [ ] Performance metrics collected (optional)
- [ ] Documentation of any intentional differences

**Estimated Time:** 16-24 hours

---

## Step 9: Cleanup & Documentation

**Goal:** Finalize the conversion and prepare for production

### Tasks
1. Remove C# code once C++ version is validated:
   - Delete `flow/actorcompiler/*.cs`
   - Remove `cmake/CompileActorCompiler.cmake` (old C# version)
   - Remove Mono/.NET dependencies from documentation
2. Update documentation:
   - `CONTRIBUTING.md` - Update build instructions
   - `flow/README.md` - Document actor compiler (if not already present)
   - Add comments to complex C++ code
   - Create `flow/actorcompiler_cpp/README.md` with architecture overview
3. Code cleanup:
   - Run clang-format on all C++ files
   - Fix any compiler warnings
   - Add Doxygen comments for public APIs
4. Consider performance optimizations:
   - Memory pools for AST nodes (if allocation is hot)
   - Faster string handling (string_view where possible)
   - Parallel file processing (if compiling multiple files)
   - Profile with perf/Instruments to find bottlenecks
5. Add error message improvements:
   - Better diagnostics for common errors
   - Suggest fixes (e.g., "did you mean 'state'?")
   - Color output for terminals (optional)

### Documentation Updates

**CONTRIBUTING.md**
```markdown
## Building from Source

### Prerequisites
- C++17 compiler (GCC 7+, Clang 5+, MSVC 2017+)
- CMake 3.13+
- OpenSSL (for actor compiler)

The actor compiler (flow/actorcompiler_cpp) is built automatically as part
of the FoundationDB build process. It translates `.actor.cpp` files into
standard C++ before compilation.
```

**flow/actorcompiler_cpp/README.md** (new file)
```markdown
# Actor Compiler

The actor compiler is a source-to-source transpiler that converts Flow actor
syntax into standard C++ state machines.

## Architecture

- **Tokenizer**: Lexical analysis, splits input into tokens
- **Parser**: Builds abstract syntax tree (AST) from tokens
- **Compiler**: Generates C++ code from AST

## Usage

    actorcompiler_cpp input.actor.cpp output.cpp [--generate-probes]

## Development

See CPP_CONVERSION_PLAN.md for implementation details.
```

### Deliverable
- [ ] C# code removed from repository
- [ ] Documentation updated
- [ ] Code formatted and commented
- [ ] No compiler warnings
- [ ] Performance baseline established
- [ ] Production-ready C++ actorcompiler

**Estimated Time:** 8-12 hours

---

## Technical Reference

### C# to C++ Common Patterns

#### LINQ to STL
```csharp
// C# LINQ                          // C++ STL equivalent
items.Where(x => pred(x))          // std::copy_if or std::filter_view
items.Select(x => transform(x))    // std::transform
items.First()                      // items.front() or *items.begin()
items.FirstOrDefault(pred)         // std::find_if → optional
items.Skip(n)                      // std::next(items.begin(), n)
items.Take(n)                      // std::span or custom range
items.Any(pred)                    // std::any_of
items.All(pred)                    // std::all_of
items.Count(pred)                  // std::count_if
items.Aggregate(f)                 // std::accumulate
```

#### String Operations
```csharp
// C# string methods               // C++ string methods
str.StartsWith("x")                // str.starts_with("x") [C++20]
str.EndsWith("x")                  // str.ends_with("x") [C++20]
str.Contains("x")                  // str.find("x") != npos
str.Replace("x", "y")              // custom or boost::replace_all
str.Substring(pos, len)            // str.substr(pos, len)
str.Trim()                         // custom trim function
String.Join(",", items)            // custom join or fmt::join
str.Split(',')                     // custom split function
```

#### Collections
```csharp
// C# collections                   // C++ collections
List<T>                            // std::vector<T>
HashSet<T>                         // std::unordered_set<T>
Dictionary<K,V>                    // std::unordered_map<K,V>
Queue<T>                           // std::queue<T>
Stack<T>                           // std::stack<T>
IEnumerable<T>                     // std::vector<T> or range
T[]                                // std::array<T,N> or std::vector<T>
```

#### Control Flow
```csharp
// C# features                      // C++ equivalent
foreach (var x in items)           // for (const auto& x : items)
using (var x = ...) { }            // { auto x = ...; } (RAII)
throw new Exception("msg")         // throw std::runtime_error("msg")
try/catch/finally                  // try/catch + RAII for finally
yield return value                 // return std::vector or use generator
switch (type) with pattern match   // std::visit or if-else chain
```

### Memory Management Strategy

1. **AST Nodes:** `std::unique_ptr` for ownership, raw pointers for references
2. **Strings:** `std::string` for owned, `std::string_view` for borrowed
3. **Collections:** `std::vector` with move semantics, reserve capacity when known
4. **Temporary Objects:** Use stack allocation, avoid heap when possible
5. **RAII:** File handles, locks, resources automatically cleaned up

### Error Handling

```cpp
// Custom Error class for parsing/compilation errors
class Error : public std::runtime_error {
    int sourceLine;
public:
    Error(int line, const std::string& msg) 
        : std::runtime_error(msg), sourceLine(line) {}
    int getSourceLine() const { return sourceLine; }
};

// Usage in parser
if (condition) {
    throw Error(token.sourceLine, "Expected ';' after statement");
}
```

---

## Risk Mitigation

### High-Risk Areas

1. **Regex Differences:** C++ `std::regex` may behave differently from C# `Regex`
   - **Mitigation:** Extensive testing with edge cases, consider PCRE2 if needed

2. **Unicode Handling:** C++ string handling is byte-oriented, not Unicode-aware
   - **Mitigation:** FDB code is primarily ASCII, but test with Unicode identifiers

3. **Platform Differences:** File paths, line endings, case sensitivity
   - **Mitigation:** Use `std::filesystem`, normalize line endings, test on all platforms

4. **Performance Regression:** C++ might be slower if poorly implemented
   - **Mitigation:** Profile after implementation, optimize hot paths

5. **Output Differences:** Generated code might differ subtly
   - **Mitigation:** Extensive diff testing, manual review of differences

### Rollback Plan

If C++ implementation has critical issues:
1. Keep C# version available via CMake option
2. Document known issues
3. Incrementally fix C++ version
4. Switch back to C# temporarily if needed

---

## Success Criteria

- [ ] C++ actorcompiler builds on Linux, macOS, Windows
- [ ] Output is byte-identical to C# version (or documented differences)
- [ ] Full FoundationDB builds with C++ actorcompiler
- [ ] All tests pass (simulation, unit, integration)
- [ ] Compilation time same or faster than C# version
- [ ] Memory usage reasonable (< 500MB for large files)
- [ ] No .NET/Mono dependency required
- [ ] Code is maintainable (commented, formatted, documented)

---

## Timeline Estimate

| Step | Description | Time Estimate |
|------|-------------|---------------|
| 1 | Project Setup & Dependencies | 4-8 hours |
| 2 | Port ParseTree.cs | 8-12 hours |
| 3 | Port Tokenization | 12-16 hours |
| 4 | Port Parsing Logic | 20-30 hours |
| 5 | Port Code Generation | 30-40 hours |
| 6 | Port Entry Point | 6-10 hours |
| 7 | Build System Integration | 4-8 hours |
| 8 | Testing & Validation | 16-24 hours |
| 9 | Cleanup & Documentation | 8-12 hours |
| **Total** | | **108-160 hours** |

**Estimated Calendar Time:** 3-4 weeks of focused full-time work, or 6-8 weeks part-time.

---

## Next Steps

1. Review this plan and adjust as needed
2. Set up development environment
3. Start with Step 1: Project Setup
4. Work through each step incrementally
5. Validate frequently with small test cases
6. Request code review before final integration

---

## References

- **C++17 Standard:** https://en.cppreference.com/w/cpp/17
- **std::filesystem:** https://en.cppreference.com/w/cpp/filesystem
- **std::regex:** https://en.cppreference.com/w/cpp/regex
- **OpenSSL SHA256:** https://www.openssl.org/docs/man1.1.1/man3/SHA256.html
- **PicoSHA2:** https://github.com/okdshin/PicoSHA2 (header-only alternative)
- **FoundationDB Flow Guide:** flow/README.md
- **FoundationDB Build Instructions:** CONTRIBUTING.md
