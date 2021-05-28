## Flow Tutorial: How Things Do Not Work

* Single threaded
* Calling ACTOR function creates an object and does NOT block.
  * Hold the returned Future object, otherwise the actor is cancelled.
  Good pattern: Add a future variable to an actor collection and wait on it, otherwise if an exception is thrown from the actor, the exception is silently ignored (not raised to the caller function!). E.g., masterServer().

* All parameters are const &
  * Shadowing argument, e.g., std::vector<Future<Void>>, iterator
  * state X x(params) results in no matching function for call to 'X::X()' error.

* Reduce state variables
* broken promise. RPC server side, the ReplyPromise is destroyed before replying back.

* Very rare: uncancellable ACTOR: `grep ACTOR */*.actor.cpp | grep void`.
* static ACTOR member function
* Flow priorities
* Flow locks

## Generics

* `waitForAll()`: https://github.com/jzhou77/foundationdb/blob/a84b1dd7ba6534a61aeda4a34e2a7cc8d48a29f4/fdbserver/CommitProxyServer.actor.cpp#L55-L76
* `quorum()`: https://github.com/jzhou77/foundationdb/blob/a84b1dd7ba6534a61aeda4a34e2a7cc8d48a29f4/fdbserver/TagPartitionedLogSystem.actor.cpp#L585
* `store()`
* `success()`
* `&&` , `||`: https://github.com/jzhou77/foundationdb/blob/a84b1dd7ba6534a61aeda4a34e2a7cc8d48a29f4/fdbserver/CommitProxyServer.actor.cpp#L1632
