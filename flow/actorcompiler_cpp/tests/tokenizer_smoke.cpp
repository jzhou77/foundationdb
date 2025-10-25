/*
 * tokenizer_smoke.cpp
 */

#include "Tokenizer.h"
#include <iostream>
#include <string>

using namespace actorcompiler;

int main() {
	const char* input = R"ACTOR(
ACTOR Future<Void> Foo() {
    state int x = 0;
    // line comment
    /* block
       comment */
    auto s = "hello";
    auto ch = 'a';
    wait(someFuture());
    when (wait(someOtherFuture())) {
        x = 1;
    }
    choose {
        when (waitNext(stream)) {
            x = 2;
        }
    }
}
)ACTOR";

	try {
		auto tokens = Tokenizer::tokenize(input);
		Tokenizer::countParens(tokens);

		int waitCount = 0;
		int chooseCount = 0;
		int whenCount = 0;
		int stringCount = 0; // double-quoted strings
		int charCount = 0; // single-quoted chars
		int commentCount = 0; // // and /* */

		for (const auto& t : tokens) {
			if (t.value == "wait" || t.value == "waitNext")
				++waitCount;
			else if (t.value == "choose")
				++chooseCount;
			else if (t.value == "when")
				++whenCount;
			else if (!t.value.empty() && t.value.front() == '"')
				++stringCount;
			else if (!t.value.empty() && t.value.front() == '\'')
				++charCount;
			else if (t.value.rfind("//", 0) == 0 || t.value.rfind("/*", 0) == 0)
				++commentCount;
		}

		bool ok = true;
		if (waitCount != 3) {
			std::cerr << "Expected 3 wait/waitNext tokens, got " << waitCount << "\n";
			ok = false;
		}
		if (chooseCount != 1) {
			std::cerr << "Expected 1 choose token, got " << chooseCount << "\n";
			ok = false;
		}
		if (whenCount != 2) {
			std::cerr << "Expected 2 when tokens, got " << whenCount << "\n";
			ok = false;
		}
		if (stringCount != 1) {
			std::cerr << "Expected 1 string literal, got " << stringCount << "\n";
			ok = false;
		}
		if (charCount != 1) {
			std::cerr << "Expected 1 char literal, got " << charCount << "\n";
			ok = false;
		}
		if (commentCount != 2) {
			std::cerr << "Expected 2 comments, got " << commentCount << "\n";
			ok = false;
		}

		if (!ok)
			return 1;
		std::cout << "Tokenizer smoke test passed\n";
		return 0;
	} catch (const std::exception& e) {
		std::cerr << "Tokenizer smoke test failed: " << e.what() << "\n";
		return 1;
	}
}
