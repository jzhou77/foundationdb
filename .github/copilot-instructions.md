# FoundationDB AI Coding Agent Instructions

## Big Picture Architecture

FoundationDB is a distributed ACID database built around **Flow**, a custom asynchronous programming framework that extends C++ with actor-based coroutines. The system consists of several major components:

- **fdbserver**: Core database server implementing storage, transaction, and coordination layers
- **fdbclient**: Client library providing database access APIs  
- **fdbrpc**: RPC framework built on Flow for inter-process communication
- **flow**: Custom coroutine framework - the foundation of everything (see below)
- **fdbmonitor**: Process manager that supervises and restarts server processes

Key architectural concepts:
- **Separation of storage and transaction layers**: Transaction processing (proxies, resolvers) is independent from storage servers
- **Deterministic simulation**: The entire system can run in a single-process simulator with injected failures
- **Knobs system**: Tunable parameters at runtime via `ClientKnobs`, `ServerKnobs`, `FlowKnobs`

## Flow: The Critical Difference

**Flow is not just async C++** - it's a custom language transpiled by the actor compiler. Understanding this is essential:

### Actor Compiler
- Files ending in `.actor.cpp` or `.actor.h` are preprocessed by `actorcompiler` (C# tool in `flow/actorcompiler/`)
- The `ACTOR` keyword defines coroutine functions that can use `wait()` statements
- `state` variables persist across `wait()` points; non-state locals are destroyed
- Actor functions return `Future<T>` but you write `return T;`
- The compiler generates complex state machines with callbacks

### Flow Syntax Essentials
```cpp
// ACTOR function - can only exist in .actor.cpp files
ACTOR Future<int> myActor(Future<int> input) {
    state int value = wait(input);  // 'state' persists across waits
    int result = value + 1;         // non-state is destroyed at next wait
    return result;
}

// Actors can call other actors
ACTOR Future<Void> caller() {
    int x = wait(myActor(someInput()));
    return Void();
}
```

### Flow Gotchas
- **Switch statements**: Cannot contain `wait()` - the compiler can't handle them
- **Variable scoping**: Use `state` for anything needed after a `wait()`
- **`choose/when`**: Flow's way to wait on multiple futures (not C++ `std::variant`)
- **Cancellation**: Dropping the last reference to a `Future` cancels the actor
- **No threading in actors**: Flow is single-threaded per network; actors cooperatively yield

### Memory Management in Flow
- **Reference counting**: Use `Reference<T>` smart pointers, inherit from `ReferenceCounted<T>`
- **Arenas**: Pool allocators for buffers; `Standalone<StringRef>` manages its own arena
- **`*Ref` types**: Don't own memory (e.g., `StringRef`, `KeyRef`, `ValueRef`)
- **`Standalone<T>`**: Owns memory via an arena (e.g., `Standalone<StringRef>` = `Key`)

## Development Workflows

### Building
```bash
ssh jzhou-dev.okteto ccmk
```

**Critical build flags**:
- `-DOPEN_FOR_IDE=ON`: Generate IDE-friendly project (won't compile, but allows code navigation)
- `-DCMAKE_EXPORT_COMPILE_COMMANDS=ON`: Generate `compile_commands.json` for LSP tools
- `-DUSE_WERROR=ON`: Enable `-Werror` (CI uses this, so you should too)

### Testing

FoundationDB has a sophisticated testing infrastructure built around **deterministic simulation**:

#### Simulation Tests
- Run entire distributed clusters in a single process with injected failures
- Tests are defined in `.toml` files in `tests/` directory
- Each test specifies workloads (C++ classes in `fdbserver/workloads/`)
- Simulation uses `deterministicRandom()` with seeds - failures are reproducible

```bash
# Run simulation tests via ctest
cd build
ctest -L fast  # Quick tests
ctest -R TestName  # Specific test

# Run a single simulation with seed (for reproducing failures)
./bin/fdbserver -r simulation -f /path/to/test.toml -s 123456789
```

#### Workload System
Workloads are the test units in FoundationDB. Create them by:
1. Inheriting from `TestWorkload` in `fdbserver/include/fdbserver/workloads/workloads.actor.h`
2. Implementing `setup()`, `start()`, `check()`, `getMetrics()`
3. Using `WorkloadFactory<YourWorkload>` to register
4. Declaring `static constexpr auto NAME = "WorkloadName";`

Example:
```cpp
struct MyWorkload : TestWorkload {
    static constexpr auto NAME = "MyWorkload";
    
    MyWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
        // Read test parameters
        testDuration = getOption(options, "testDuration"_sr, 10.0);
    }
    
    Future<Void> setup(Database const& cx) override { return Void(); }
    Future<Void> start(Database const& cx) override { return _start(cx, this); }
    Future<bool> check(Database const& cx) override { return true; }
    
    ACTOR static Future<Void> _start(Database cx, MyWorkload* self) {
        // Actual test logic using Flow actors
    }
};

WorkloadFactory<MyWorkload> MyWorkloadFactory;
```

#### Unit Tests
- Use `TEST_CASE("/path/to/test")` macro for unit tests
- Tests run via the `UnitTests` workload or directly
- Located throughout codebase but collected at link time

### The Knobs System

FoundationDB's behavior is controlled by thousands of tunable parameters called "knobs":

- **FlowKnobs**: Network and core Flow behavior
- **ClientKnobs**: Client library behavior (e.g., `CLIENT_KNOBS->TRANSACTION_SIZE_LIMIT`)
- **ServerKnobs**: Server-side behavior (e.g., `SERVER_KNOBS->MIN_SHARD_BYTES`)

Knobs can be:
- Randomized with `if (randomize && BUGGIFY)` to find edge cases
- Set at runtime via `--knob_NAME=value` command line or config database
- Accessed globally via `CLIENT_KNOBS->NAME` or `SERVER_KNOBS->NAME`

When adding a new knob:
1. Add to appropriate `*Knobs.h` header
2. Initialize in `*Knobs.cpp` with `init(KNOB_NAME, default_value);`
3. Optionally add buggification: `if (randomize && BUGGIFY) KNOB_NAME = randomValue;`

## Project-Specific Conventions

### File Organization
- `.actor.cpp/.actor.h`: Flow code with actors (preprocessed)
- `.cpp/.h`: Standard C++ (no actors)
- `include/fdbclient/`, `include/fdbserver/`: Public headers
- `workloads/`: Test workloads for simulation

### Naming Conventions
- `*Ref`: Types that don't own memory (`StringRef`, `KeyRef`, `ValueRef`)
- `Standalone<T>`: Arena-backed, memory-owning version of `*Ref` types
- `ACTOR`: Functions that can use `wait()` - must return `Future<T>`
- `state`: Variables that survive across `wait()` statements in actors

### Common Patterns
- **RPC Interfaces**: Structs with `PromiseStream<RequestType>` members for each RPC endpoint
- **Error Handling**: Use `Error` class; errors propagate through `Future<T>` and throw at `wait()`
- **Serialization**: Implement `template<class Ar> void serialize(Ar& ar)` for network types
- **TraceEvent**: Structured logging - use liberally, especially for simulation debugging

### Adding New Features
1. **Plan first**: Discuss on forums for non-trivial changes
2. **Actor or not**: If you need to wait on async operations, use actors (`.actor.cpp`)
3. **Simulation-friendly**: Use `deterministicRandom()`, not `std::random` or `rand()`
4. **Add tests**: Create a workload in `fdbserver/workloads/` and a `.toml` test file
5. **Knobs for tunables**: Don't hardcode thresholds; use knobs with buggification
6. **TraceEvents**: Add events for observability, especially state transitions

### Debugging Simulation Failures
When simulation tests fail with a seed:
1. Re-run with the same seed to reproduce: `fdbserver -r simulation -s <seed> -f test.toml`
2. Look for `TraceEvent` logs in `trace.*.xml` files
3. Common issues: non-determinism (threads, real time, OS randomness), race conditions
4. Use `ASSERT` and `TraceEvent` liberally - they're free in simulation

### Integration Points
- **Storage engines**: Pluggable via `IKeyValueStore` interface (SQLite, RocksDB, Redwood)
- **Client bindings**: C API in `bindings/c/`, language bindings wrap it
- **Backup/restore**: `fdbbackup` tool and backup agent in `fdbclient/`
- **Monitoring**: Status JSON from `fdbcli`, prometheus metrics

## Critical Files for Understanding
- `flow/flow.h`: Core Flow primitives (`Future`, `Promise`, `Actor`)
- `flow/README.md`: Comprehensive Flow tutorial
- `fdbclient/NativeAPI.actor.cpp`: Client transaction implementation
- `fdbserver/masterserver.actor.cpp`: Transaction system coordinator
- `fdbserver/storageserver.actor.cpp`: Storage layer implementation
- `CMakeLists.txt`: Build configuration
- `tests/`: Simulation test definitions

## Common Pitfalls
1. **Forgetting `.actor.cpp` extension**: Can't use `ACTOR` or `wait()` in regular `.cpp` files
2. **Missing `state` keyword**: Variables not marked `state` are destroyed at `wait()` boundaries
3. **Breaking determinism**: Using `std::random`, threads, or real time in simulation tests
4. **Ignoring buggification**: New code should work even when knobs are randomized
5. **Not checking errors**: Always handle errors from `wait()` calls

## Resources
- **Forums**: https://forums.foundationdb.org - for design discussions
- **Documentation**: https://apple.github.io/foundationdb/
- **Build Docker**: `foundationdb/build` on Docker Hub
- **This repo**: See `CONTRIBUTING.md`, `README.md`, `flow/README.md`
