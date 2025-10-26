// multiple_waits.actor.cpp - Test multiple sequential wait statements
// This actor waits on two futures and returns their sum

#include "flow/flow.h"

ACTOR Future<int> multipleWaits(Future<int> f1, Future<int> f2) {
	state int x = wait(f1);
	state int y = wait(f2);
	return x + y;
}
