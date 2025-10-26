// choose_when.actor.cpp - Test choose/when statement
// This actor races two futures and returns the first one that completes

#include "flow/flow.h"

ACTOR Future<int> chooseWhen(Future<int> a, Future<int> b) {
	choose {
		when(int x = wait(a)) {
			return x;
		}
		when(int y = wait(b)) {
			return y;
		}
	}
}
