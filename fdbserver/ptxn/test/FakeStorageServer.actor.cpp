/*
 * FakeStorageServer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ptxn/test/FakeStorageServer.actor.h"

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogPeekCursor.actor.h"
#include "flow/String.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn::test {

void IKeyValueStoreCloser(IKeyValueStore* pStore) {
	pStore->close();
}

FakeStorageServerContext::FakeStorageServerContext()
  : id(randomUID()), pCursor(std::make_shared<MergedStorageTeamPeekCursor>()),
    pStorageEngine(
        openKVStore(KeyValueStoreType::MEMORY, concatToString("fakeStorageServer_", id.toString()), id, (1 << 30)),
        &IKeyValueStoreCloser) {

	pStorageEngine->init();
}

void FakeStorageServerContext::initializePeekCursor() {
	for (auto& [storageTeamID, tLogInterface] : pTestDriverContext->storageTeamIDTLogInterfaceMapper) {
		if (pTestDriverContext->getStorageServerInterface(storageTeamID) == pStorageServerInterface) {
			// FIXME For test purpose the start version is 0
			pCursor->addCursor(std::make_unique<StorageTeamPeekCursor>(0, storageTeamID, tLogInterface.get()));
		}
	}
}

namespace {

// Sets the flag that the storage server has retrieved a mutation for a given CommitValidationRecord
void markStorageServerValidated(CommitValidationRecord& record) {
	record.storageServerValidated = true;
}

ACTOR Future<Void> writeKeyValuesToFile(std::shared_ptr<FakeStorageServerContext> pContext, Version version) {
	std::string filePath = concatToString("fakeStorageServer/", pContext->id.toString(), "/");
	platform::createDirectory(filePath);
	std::string fileName = concatToString(filePath, version);

	std::cout << "Write FakeStorageServer " << pContext->id.toString() << " content to " << fileName << std::endl
	          << std::endl;

	state std::ofstream ofs(fileName, std::ios::out);
	ofs << "FakeStorageServer " << pContext->id.toString() << " at version " << version << std::endl;

	state std::set<std::string>::const_iterator iter = pContext->keys.cbegin();
	while (iter != pContext->keys.cend()) {
		Optional<Value> value = wait(pContext->pStorageEngine->readValue(*iter));
		if (value.present()) {
			ofs << std::setw(30) << "Key: " << *iter << std::endl;
			ofs << std::setw(30) << "Value: " << value.get().toString() << std::endl;
			ofs << std::endl;
		} else {
			ofs << std::setw(30) << "Key: " << *iter << std::endl;
			ofs << std::setw(30) << "Value: " << "deleted" << std::endl;
			ofs << std::endl;
		}
		iter = std::next(iter);
	}
	ofs.close();

	return Void();
}

void persistMutations(std::shared_ptr<FakeStorageServerContext> pContext, const VectorRef<MutationRef>& mutations) {
	auto& pStorageEngine = pContext->pStorageEngine;

	for (const auto& mutation : mutations) {
		if (mutation.type == MutationRef::SetValue) {
			Key key(mutation.param1);
			Value value(mutation.param2);
			KeyValueRef kvPair(key, value);
			pStorageEngine->set(kvPair);
			pContext->keys.insert(key.toString());
		} else if (mutation.type == MutationRef::ClearRange) {
			Key begin(mutation.param1);
			Key end(mutation.param2);
			KeyRangeRef range(begin, end);
			pStorageEngine->clear(range);
		}
	}

	pStorageEngine->commit();
}

// Continuously peek from TLog for new
ACTOR Future<Void> pullDataFromTLog(std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeStorageServerContext->pTestDriverContext;
	state std::shared_ptr<StorageServerInterfaceBase> pStorageServerInterface =
	    pFakeStorageServerContext->pStorageServerInterface;
	state std::shared_ptr<MergedStorageTeamPeekCursor> pCursor = pFakeStorageServerContext->pCursor;
	state Version lastVersion = 0;
	state VectorRef<MutationRef> currentCommitMutations;

	ASSERT(pTestDriverContext != nullptr);
	ASSERT(pStorageServerInterface != nullptr);
	ASSERT(pCursor != nullptr);

	loop {
		bool remoteAvailable = wait(pCursor->remoteMoreAvailable());

		if (!remoteAvailable) {
			wait(delay(0.000001));
			continue;
		}

		while (pCursor->hasRemaining()) {
			if (pCursor->get().version != lastVersion) {
				persistMutations(pFakeStorageServerContext, currentCommitMutations);
				wait(writeKeyValuesToFile(pFakeStorageServerContext, lastVersion));
				currentCommitMutations.resize(pFakeStorageServerContext->persistenceArena, 0);
				lastVersion = pCursor->get().version;
			}
			currentCommitMutations.push_back(pFakeStorageServerContext->persistenceArena, pCursor->get().mutation);
			pCursor->next();
		}
	}
}

} // anonymous namespace

ACTOR Future<Void> fakeStorageServer_PassivelyReceive(
    std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeStorageServerContext->pTestDriverContext;
	state std::shared_ptr<StorageServerInterface_PassivelyReceive> pStorageServerInterface =
	    std::dynamic_pointer_cast<StorageServerInterface_PassivelyReceive>(
	        pFakeStorageServerContext->pStorageServerInterface);

	ASSERT(pStorageServerInterface);

	loop choose {
		when(StorageServerPushRequest request = waitNext(pStorageServerInterface->pushRequests.getFuture())) {
			std::vector<MutationRef> mutations(request.mutations.begin(), request.mutations.end());
			verifyMutationsInRecord(pTestDriverContext->commitRecord,
			                        request.version,
			                        request.storageTeamID,
			                        mutations,
			                        markStorageServerValidated);
			request.reply.send(StorageServerPushReply());
		}
	}
}

ACTOR Future<Void> fakeStorageServer_ActivelyPull(std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	state std::shared_ptr<TestDriverContext> pTestDriverContext = pFakeStorageServerContext->pTestDriverContext;
	state std::shared_ptr<StorageServerInterface_ActivelyPull> pStorageServerInterface =
	    std::dynamic_pointer_cast<StorageServerInterface_ActivelyPull>(
	        pFakeStorageServerContext->pStorageServerInterface);
	state std::vector<Future<Void>> actors;

	ASSERT(pStorageServerInterface);

	pFakeStorageServerContext->initializePeekCursor();
	actors.push_back(pullDataFromTLog(pFakeStorageServerContext));

	loop {
		loop choose {
			when(wait(delay(1.0))) { break; }
		}
	}
}

Future<Void> getFakeStorageServerActor(const MessageTransferModel model,
                                       std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext) {
	switch (model) {
	case MessageTransferModel::TLogActivelyPush:
		return fakeStorageServer_PassivelyReceive(pFakeStorageServerContext);
	case MessageTransferModel::StorageServerActivelyPull:
		return fakeStorageServer_ActivelyPull(pFakeStorageServerContext);
	default:
		throw internal_error_msg("Unsupported message transfer model");
	}
}

} // namespace ptxn::test
