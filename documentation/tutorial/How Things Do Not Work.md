# Flow Tutorial: How Things Do Not Work

## Topics

* Single threaded
* ACTOR model and actor life cycle

* Calling ACTOR function creates an object and does NOT block.
  * Hold the returned Future object.

* ACTOR chaining and exceptions
  * wait() and exceptions

* All parameters are const &
  * Shadowing argument
  * state X x(params) results in no matching function for call to 'X::X()' error.

* Reduce state variables
* broken promise

## Probably not covered

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
