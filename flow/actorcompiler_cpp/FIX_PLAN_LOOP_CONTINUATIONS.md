# Fix Plan: Loop Continuation Methods

## Problem

The C++ actor compiler generates goto/label pattern for loops containing wait statements, while the C# reference implementation generates continuation methods. This causes `a_body1cont1()` to be structurally different.

## Comparison

### C# Pattern (CORRECT)
```cpp
int a_body1cont1(int loopDepth) {
    sum = 0;
    i = 0;
    ;
    loopDepth = a_body1cont1loopHead1(loopDepth);
    return loopDepth;
}

int a_body1cont1loopHead1(int loopDepth) {
    int oldLoopDepth = ++loopDepth;
    while (loopDepth == oldLoopDepth) loopDepth = a_body1cont1loopBody1(loopDepth);
    return loopDepth;
}

int a_body1cont1loopBody1(int loopDepth) {
    if (i >= n) {
        return a_body1cont1break1(loopDepth==0?0:loopDepth-1); // break
    }
    StrictFuture<int> __when_expr_1 = Future<int>(i);
    // ... wait setup
    return loopDepth;
}

int a_body1cont1loopBody1cont1(int loopDepth) {
    sum += value;
    i++;
    if (loopDepth == 0) return a_body1cont1loopHead1(0);
    return loopDepth;
}

int a_body1cont1break1(int loopDepth) {
    try {
        return a_body1cont2(loopDepth);
    } catch (Error& error) {
        loopDepth = a_body1Catch1(error, loopDepth);
    } catch (...) {
        loopDepth = a_body1Catch1(unknown_error(), loopDepth);
    }
    return loopDepth;
}
```

### C++ Current (WRONG)
```cpp
int a_body1cont1(int loopDepth) {
    sum = 0;
    i = 0;

    cont1:  // GOTO LABEL
    {
        if (i >= n) { goto cont2; }
        // ... wait setup
    }
    cont3:
    goto cont1;
    
    cont2:
    // return handling inline
}
```

## Implementation Strategy

Need to detect loops containing waits and generate continuation methods instead of goto/labels.

### Key Detection
1. Check if loop body contains WaitStatement
2. If yes, generate continuation methods
3. If no, can use simple inline loop

### Method Generation Pattern
For a loop in function `a_body1cont1`:
- `a_body1cont1loopHead1` - Loop control (manages loopDepth)
- `a_body1cont1loopBody1` - Loop body (condition + statements before wait)
- `a_body1cont1loopBody1cont1` - Continuation after wait
- `a_body1cont1break1` - Break handler (wraps next continuation in try-catch)

## TODO

This requires significant refactoring of `compileStatement(ForStatement*)` and `compileStatement(LoopStatement*)`.
