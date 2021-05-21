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

extern AsyncTrigger cycleCompleted;

namespace ptxn::test {

void IKeyValueStoreCloser(IKeyValueStore* pStore) {
	pStore->close();
}

FakeStorageServerContext::FakeStorageServerContext()
  : id(randomUID()),
    pStorageEngine(
        openKVStore(KeyValueStoreType::MEMORY, concatToString("fakeStorageServer_", id.toString()), id, (1 << 30)),
        &IKeyValueStoreCloser) {

	pStorageEngine->init();
}

void FakeStorageServerContext::initializePeekCursor() {
	for (auto& [storageTeamID, tLogInterface] : pTestDriverContext->storageTeamIDTLogInterfaceMapper) {
		cursorPtrs.emplace_back(std::make_unique<StorageTeamPeekCursor>(0, storageTeamID, tLogInterface.get()));
	}
}

namespace {

// Sets the flag that the storage server has retrieved a mutation for a given CommitValidationRecord
void markStorageServerValidated(CommitValidationRecord& record) {
	record.storageServerValidated = true;
}

ACTOR Future<Void> writeKeyValuesToFile(std::shared_ptr<FakeStorageServerContext> pContext) {
	std::string filePath = concatToString("fakeStorageServer/", pContext->id.toString(), "/");
	platform::createDirectory(filePath);
	std::string fileName = concatToString(filePath, pContext->lastVersion);

	std::cout << "Write FakeStorageServer " << pContext->id.toString() << " content to " << fileName << std::endl
	          << std::endl;

	state std::ofstream ofs(fileName, std::ios::out);
	ofs << "FakeStorageServer " << pContext->id.toString() << " at version " << pContext->lastVersion << std::endl;

	state KeyRef startKey = "A"_sr;
	state KeyRef endKey = "z"_sr;
	state std::map<StringRef, StringRef> kvPairs;

	KeyRangeRef range(startKey, endKey);
	RangeResult rangeResult = wait(pContext->pStorageEngine->readRange(range));
	std::cout << "Number of key-value pairs: " << rangeResult.size() << std::endl;
	for (decltype(rangeResult.begin()) iter = rangeResult.begin(); iter != rangeResult.end(); ++iter) {
		StringRef key(pContext->persistenceArena, iter->key);
		StringRef value(pContext->persistenceArena, iter->value);
		std::cout << key.toString() << '\t' << value.toString() << std::endl;
		kvPairs[key] = value;
	}

	for (const auto& [key, value] : kvPairs) {
		ofs << std::setw(15) << "Key: " << key.toHexString() << std::endl;
		ofs << std::setw(15) << "Value: " << value.toHexString() << std::endl;
	}

	ofs << "End of dumping" << std::endl;
	ofs.close();

	return Void();
}

// Continuously peek from TLog for new
ACTOR Future<Void> pullDataFromTLog(std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext, int index) {
	state StorageTeamPeekCursor* pCursor(pFakeStorageServerContext->cursorPtrs[index].get());
	state IKeyValueStore* pStorageEngine(pFakeStorageServerContext->pStorageEngine.get());
	ASSERT(pCursor != nullptr);

	loop {
		bool remoteAvailable = wait(pCursor->remoteMoreAvailable());

		if (!remoteAvailable) {
			wait(delay(0.000001));
		} else {
			while (pCursor->hasRemaining()) {
				const VersionSubsequenceMutation& item = pCursor->get();
				const MutationRef& mutation = item.mutation;
				pFakeStorageServerContext->lastVersion = std::max(pFakeStorageServerContext->lastVersion, item.version);
				if (mutation.type == MutationRef::SetValue) {
					Key key(mutation.param1);
					Value value(mutation.param2);
					KeyValueRef kvPair(key, value);
					pStorageEngine->set(kvPair);
					std::cout << "Set " << key.toString() << " -> " << value.toString() << std::endl;
				} else if (mutation.type == MutationRef::ClearRange) {
					Key begin(mutation.param1);
					Key end(mutation.param2);
					KeyRangeRef range(begin, end);
					pStorageEngine->clear(range);
				}
				// We commit *every* mutation for demo purpose
				pStorageEngine->commit();
				pCursor->next();
			}
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
	for (int i = 0; i < pFakeStorageServerContext->cursorPtrs.size(); ++i) {
		actors.push_back(pullDataFromTLog(pFakeStorageServerContext, i));
	}

	loop {
		choose {
			when(wait(cycleCompleted.onTrigger())) {
				std::cout << " Cycle completed triggered " << std::endl;
				wait(writeKeyValuesToFile(pFakeStorageServerContext));
				break;
			}
		}
	}
	return Void();
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
