/*
 * Program.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ActorCompiler.h"
#include "ActorParser.h"

#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>

namespace fs = std::filesystem;

namespace actorcompiler {

void OverwriteByMove(const std::string& target, const std::string& temporaryFile) {
	if (fs::exists(target)) {
		fs::permissions(target, fs::perms::all);
		fs::remove(target);
	}
	fs::rename(temporaryFile, target);
	fs::permissions(target, fs::perms::owner_read | fs::perms::group_read | fs::perms::others_read);
}

} // namespace actorcompiler

int main(int argc, char* argv[]) {
	if (argc < 3) {
		std::cout << "Usage:" << std::endl;
		std::cout << " actorcompiler <input> <output> [--disable-diagnostics] [--generate-probes]" << std::endl;
		return 100;
	}

	std::cout << "actorcompiler ";
	for (int i = 1; i < argc; i++) {
		std::cout << argv[i] << " ";
	}
	std::cout << std::endl;

	bool generateProbes = false;
	std::string input = argv[1];
	std::string output = argv[2];
	std::string outputtmp = output + ".tmp";
	std::string outputUid = output + ".uid";

	actorcompiler::ErrorMessagePolicy errorMessagePolicy;

	for (int i = 3; i < argc; i++) {
		std::string arg = argv[i];
		if (arg.substr(0, 2) == "--") {
			if (arg == "--disable-diagnostics") {
				errorMessagePolicy.DisableDiagnostics = true;
			} else if (arg == "--generate-probes") {
				generateProbes = true;
			}
		}
	}

	try {
		std::ifstream inputFile(input);
		if (!inputFile) {
			std::cerr << "Could not open input file: " << input << std::endl;
			throw std::runtime_error("Could not open input file: " + input);
		}

		std::string inputData((std::istreambuf_iterator<char>(inputFile)), std::istreambuf_iterator<char>());

		std::string normalizedInput = input;
		std::replace(normalizedInput.begin(), normalizedInput.end(), '\\', '/');

		actorcompiler::ActorParser parser(inputData, normalizedInput, errorMessagePolicy, generateProbes);

		{
			std::ofstream outputStream(outputtmp);
			if (!outputStream) {
				std::cerr << "Could not open output file: " << outputtmp << std::endl;
				throw std::runtime_error("Could not open output file: " + outputtmp);
			}

			std::string normalizedOutput = output;
			std::replace(normalizedOutput.begin(), normalizedOutput.end(), '\\', '/');
			parser.write(outputStream, normalizedOutput);
		}

		actorcompiler::OverwriteByMove(output, outputtmp);

		{
			std::ofstream outputStream(outputtmp);
			if (!outputStream) {
				std::cerr << "Could not open output file: " << outputtmp << std::endl;
				throw std::runtime_error("Could not open output file: " + outputtmp);
			}

			for (const auto& entry : parser.uidObjects) {
				outputStream << entry.first.first << "|" << entry.first.second << "|" << entry.second << std::endl;
			}
		}

		actorcompiler::OverwriteByMove(outputUid, outputtmp);

		return 0;
	} catch (const actorcompiler::Error& e) {
		std::cerr << input << "(" << e.sourceLine << "): error FAC1000: " << e.message << std::endl;
		if (fs::exists(outputtmp)) {
			fs::remove(outputtmp);
		}
		if (fs::exists(output)) {
			fs::permissions(output, fs::perms::all);
			fs::remove(output);
		}
		return 1;
	} catch (const std::exception& e) {
		std::cerr << input << "(1): error FAC2000: Internal " << e.what() << std::endl;
		if (fs::exists(outputtmp)) {
			fs::remove(outputtmp);
		}
		if (fs::exists(output)) {
			fs::permissions(output, fs::perms::all);
			fs::remove(output);
		}
		return 3;
	}
}