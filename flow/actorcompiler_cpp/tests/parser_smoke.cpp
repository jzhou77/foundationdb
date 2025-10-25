#include "ActorParser.h"
#include <sstream>
#include <iostream>

using namespace actorcompiler;

int main() {
	try {
		const std::string src = "// simple actor with a wait\n"
		                        "ACTOR Future<Void> Foo() {\n"
		                        "    wait(delay(0.0));\n"
		                        "}\n"
		                        "\n"
		                        "DESCR struct Bar {\n"
		                        "    int x; // comment\n"
		                        "};\n";

		ErrorMessagePolicy policy; // default
		ActorParser parser(src, "parser_smoke.actor.cpp", policy, /*generateProbes*/ false);
		std::ostringstream out;
		parser.write(out, "parser_smoke.out.cpp");

		// Basic sanity: output should start with our post-processor define
		auto text = out.str();
		if (text.find("#define POST_ACTOR_COMPILER") == std::string::npos) {
			std::cerr << "Missing POST_ACTOR_COMPILER define in output" << std::endl;
			return 2;
		}

		// Success
		return 0;
	} catch (const std::exception& ex) {
		std::cerr << "Exception: " << ex.what() << std::endl;
		return 1;
	}
}
