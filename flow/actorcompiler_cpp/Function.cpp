#include "Function.h"
#include <sstream>

namespace actorcompiler {

void Function::indent(int change) {
	if (change > 0) {
		indentation.append(static_cast<size_t>(change), '\t');
	} else if (change < 0) {
		size_t dec = static_cast<size_t>(-change);
		if (dec >= indentation.size())
			indentation.clear();
		else
			indentation.erase(indentation.size() - dec);
	}
}

void Function::writeLine(const std::string& line) {
	body << indentation << line << '\n';
}

void Function::writeLineUnindented(const std::string& line) {
	body << line << '\n';
}

std::string Function::call(const std::vector<std::string>& parameters) {
	std::ostringstream oss;
	oss << name << '(';
	bool first = true;
	for (const auto& p : parameters) {
		if (!first)
			oss << ", ";
		first = false;
		oss << p;
	}
	oss << ')';
	called = true;
	return oss.str();
}

} // namespace actorcompiler
