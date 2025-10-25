/*
 * ParseTree.cpp
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

#include "ParseTree.h"

namespace actorcompiler {

// WhileStatement
bool WhileStatement::containsWait() const {
	return body && body->containsWait();
}

// ForStatement
bool ForStatement::containsWait() const {
	return body && body->containsWait();
}

// RangeForStatement
bool RangeForStatement::containsWait() const {
	return body && body->containsWait();
}

// LoopStatement
bool LoopStatement::containsWait() const {
	return body && body->containsWait();
}

// IfStatement
bool IfStatement::containsWait() const {
	if (ifBody && ifBody->containsWait()) {
		return true;
	}
	if (elseBody && elseBody->containsWait()) {
		return true;
	}
	return false;
}

// ChooseStatement
bool ChooseStatement::containsWait() const {
	return body && body->containsWait();
}

// TryStatement
bool TryStatement::containsWait() const {
	if (tryBody && tryBody->containsWait()) {
		return true;
	}
	for (const auto& c : catches) {
		if (c.body && c.body->containsWait()) {
			return true;
		}
	}
	return false;
}

// CodeBlock
bool CodeBlock::containsWait() const {
	for (const auto& stmt : statements) {
		if (stmt && stmt->containsWait()) {
			return true;
		}
	}
	return false;
}

} // namespace actorcompiler
