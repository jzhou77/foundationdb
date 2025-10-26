// simple_wait.actor.cpp - Test simple wait statement
// This actor waits on a single future and returns its value

#include "flow/flow.h"

ACTOR Future<int> simpleWait(Future<int> f) {
	state int x = wait(f);
	return x;
}
