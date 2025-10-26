// loop_with_wait.actor.cpp - Test loop with wait statement
// This actor demonstrates waiting inside a loop

#include "flow/flow.h"

ACTOR Future<int> loopWithWait(Future<int> count) {
	state int n = wait(count);
	state int sum = 0;
	state int i = 0;

	loop {
		if (i >= n) {
			break;
		}
		state int value = wait(Future<int>(i));
		sum += value;
		i++;
	}

	return sum;
}
