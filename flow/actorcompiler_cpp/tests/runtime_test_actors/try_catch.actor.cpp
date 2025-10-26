// try_catch.actor.cpp - Test try/catch error handling
// This actor catches errors from a future and returns a default value

#include "flow/flow.h"

ACTOR Future<int> tryCatch(Future<int> f) {
	try {
		state int x = wait(f);
		return x;
	} catch (Error& e) {
		return -1;
	}
}
