/*
 * main.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ActorParser.h"
#include "Error.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <filesystem>
#include <cstdlib>

using namespace actorcompiler;
namespace fs = std::filesystem;

// Read entire file into string
std::string readFile(const std::string& path) {
	std::ifstream file(path);
	if (!file) {
		throw std::runtime_error("Cannot open input file: " + path);
	}
	std::ostringstream ss;
	ss << file.rdbuf();
	return ss.str();
}

// Write string to file
void writeFile(const std::string& path, const std::string& content) {
	std::ofstream file(path);
	if (!file) {
		throw std::runtime_error("Cannot write output file: " + path);
	}
	file << content;
}

// Atomically replace target file with temporary file
void overwriteByMove(const std::string& target, const std::string& temporaryFile) {
	if (fs::exists(target)) {
		// Make writable before deleting
		fs::permissions(target, fs::perms::owner_write, fs::perm_options::add);
		fs::remove(target);
	}
	fs::rename(temporaryFile, target);
	// Make read-only after writing
	fs::permissions(
	    target, fs::perms::owner_read | fs::perms::group_read | fs::perms::others_read, fs::perm_options::replace);
}

int main(int argc, char* argv[]) {
	// Parse command line arguments
	if (argc < 3) {
		std::cerr << "Usage:\n";
		std::cerr << "  actorcompiler_cpp <input> <output> [--disable-diagnostics] [--generate-probes]\n";
		return 100;
	}

	std::string input = argv[1];
	std::string output = argv[2];
	std::string outputTmp = output + ".tmp";
	std::string outputUid = output + ".uid";

	// Parse flags
	ErrorMessagePolicy errorMessagePolicy;
	bool generateProbes = false;

	for (int i = 3; i < argc; ++i) {
		std::string arg = argv[i];
		if (arg == "--disable-diagnostics") {
			errorMessagePolicy.disableDiagnostics = true;
		} else if (arg == "--generate-probes") {
			generateProbes = true;
		}
	}

	// Log command
	std::cout << "actorcompiler_cpp";
	for (int i = 1; i < argc; ++i) {
		std::cout << " " << argv[i];
	}
	std::cout << "\n";

	try {
		// Read input file
		std::string inputData = readFile(input);

		// Normalize path separators
		std::string normalizedInput = input;
		std::replace(normalizedInput.begin(), normalizedInput.end(), '\\', '/');
		std::string normalizedOutput = output;
		std::replace(normalizedOutput.begin(), normalizedOutput.end(), '\\', '/');

		// Parse and compile
		ActorParser parser(inputData, normalizedInput, errorMessagePolicy, generateProbes);

		// Write output
		std::ostringstream outputStream;
		parser.write(outputStream, normalizedOutput);
		writeFile(outputTmp, outputStream.str());
		overwriteByMove(output, outputTmp);

		// Write UID file
		std::ostringstream uidStream;
		for (const auto& entry : parser.getUidObjects()) {
			uidStream << entry.first.first << "|" << entry.first.second << "|" << entry.second << "\n";
		}
		writeFile(outputTmp, uidStream.str());
		overwriteByMove(outputUid, outputTmp);

		return 0;

	} catch (const Error& e) {
		std::cerr << input << "(" << e.getSourceLine() << "): error FAC1000: " << e.what() << "\n";
		if (fs::exists(outputTmp)) {
			fs::remove(outputTmp);
		}
		if (fs::exists(output)) {
			fs::permissions(output, fs::perms::owner_write, fs::perm_options::add);
			fs::remove(output);
		}
		return 1;

	} catch (const std::exception& e) {
		std::cerr << input << "(1): error FAC2000: Internal " << e.what() << "\n";
		if (fs::exists(outputTmp)) {
			fs::remove(outputTmp);
		}
		if (fs::exists(output)) {
			fs::permissions(output, fs::perms::owner_write, fs::perm_options::add);
			fs::remove(output);
		}
		return 3;
	}
}
