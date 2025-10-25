#include "Context.h"

namespace actorcompiler {

Context Context::withTarget(Function* newTarget) const {
	Context c = *this;
	c.target = newTarget;
	return c;
}

Context Context::loopContext(Function* newTarget,
                             Function* breakFunc,
                             Function* continueFunc,
                             int deltaLoopDepth) const {
	Context c = *this;
	c.target = newTarget;
	c.breakF = breakFunc;
	c.continueF = continueFunc;
	c.tryLoopDepth += deltaLoopDepth;
	return c;
}

Context Context::withCatch(Function* newCatchFErr) const {
	Context c = *this;
	c.catchFErr = newCatchFErr;
	return c;
}

Context Context::clone() const {
	return *this;
}

} // namespace actorcompiler
