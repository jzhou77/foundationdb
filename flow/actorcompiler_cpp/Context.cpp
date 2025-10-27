#include "Context.h"

namespace actorcompiler {

Context Context::withTarget(const std::string& newTarget) const {
	Context c = *this;
	c.targetLabel = newTarget;
	return c;
}

Context Context::loopContext(const std::string& breakLbl, const std::string& continueLbl) const {
	Context c = *this;
	c.breakLabel = breakLbl;
	c.continueLabel = continueLbl;
	return c;
}

Context Context::loopBodyContext(int depth, const std::string& bodyPrefix, const std::string& breakLbl, const std::string& continueLbl) const {
	Context c = *this;
	c.loopDepth = depth;
	c.loopBodyPrefix = bodyPrefix;
	c.breakLabel = breakLbl;
	c.continueLabel = continueLbl;
	return c;
}

Context Context::withCatch(const std::string& errVar, const std::string& errCode, const std::string& handler) const {
	Context c = *this;
	c.errorVarName = errVar;
	c.errorCodeVarName = errCode;
	c.catchHandler = handler;
	return c;
}

Context Context::clone() const {
	return *this;
}

} // namespace actorcompiler
