/*
 * Driver.h
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

#ifndef FDBSERVER_PTXN_TEST_DRIVER_H
#define FDBSERVER_PTXN_TEST_DRIVER_H

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/StorageServerInterface.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/UnitTest.h"

namespace ptxn::test {

struct CommitValidationRecord {
	bool tLogValidated = false;
	bool storageServerValidated = false;

	bool validated() const;
};

struct CommitRecord {
	Version version;
	StorageTeamID storageTeamID;
	std::vector<MutationRef> mutations;

	CommitValidationRecord validation;

	CommitRecord(const Version& version, const StorageTeamID& storageTeamID, std::vector<MutationRef>&& mutationRef);
};

// Driver options for starting mock environment.
struct TestDriverOptions {
	static const int DEFAULT_NUM_COMMITS = 3;
	static const int DEFAULT_NUM_TEAMS = 10;
	static const int DEFAULT_NUM_PROXIES = 1;
	static const int DEFAULT_NUM_TLOGS = 3;
	static const int DEFAULT_NUM_TLOG_GROUPS = 4;
	static const int DEFAULT_NUM_STORAGE_SERVERS = 3;
	static const int DEFAULT_NUM_RESOLVERS = 2;
	static const MessageTransferModel DEFAULT_MESSAGE_TRANSFER_MODEL = MessageTransferModel::TLogActivelyPush;

	int numCommits;
	int numStorageTeams;
	int numProxies;
	int numTLogs;
	int numTLogGroups;
	int numStorageServers;
	int numResolvers;
	MessageTransferModel transferModel;

	explicit TestDriverOptions(const UnitTestParameters&);
};

struct TestDriverContext {
	// Num commits to be created
	int numCommits;

	// Teams
	int numStorageTeamIDs;
	std::vector<StorageTeamID> storageTeamIDs;

	MessageTransferModel messageTransferModel;

	// Proxies
	bool useFakeProxy;
	int numProxies;

	// Resolvers
	int numResolvers;
	std::vector<std::shared_ptr<ResolverInterface>> resolverInterfaces;

	// TLog
	bool useFakeTLog;
	int numTLogs;
	int numTLogGroups;
	std::vector<TLogGroup> tLogGroups;
	std::unordered_map<TLogGroupID, std::shared_ptr<TLogInterfaceBase>> tLogGroupLeaders;
	std::vector<std::shared_ptr<TLogInterfaceBase>> tLogInterfaces;
	std::unordered_map<StorageTeamID, std::shared_ptr<TLogInterfaceBase>> storageTeamIDTLogInterfaceMapper;
	std::shared_ptr<TLogInterfaceBase> getTLogInterface(const StorageTeamID&);

	// Storage Server
	bool useFakeStorageServer;
	int numStorageServers;
	std::vector<std::shared_ptr<StorageServerInterfaceBase>> storageServerInterfaces;
	std::unordered_map<StorageTeamID, std::shared_ptr<StorageServerInterfaceBase>> storageTeamIDStorageServerInterfaceMapper;
	std::shared_ptr<StorageServerInterfaceBase> getStorageServerInterface(const StorageTeamID&);

	// Stores the generated commits
	Arena mutationsArena;
	std::vector<CommitRecord> commitRecord;
};

// Returns an initialized TestDriverContext with default values specified in "options".
std::shared_ptr<TestDriverContext> initTestDriverContext(const TestDriverOptions& options);

// Check if all records are validated
bool isAllRecordsValidated(const std::vector<CommitRecord>& records);

// Check if a set of mutations is coming from a previous know commit
void verifyMutationsInRecord(std::vector<CommitRecord>& record,
                             const Version&,
                             const StorageTeamID&,
                             const std::vector<MutationRef>& mutations,
                             std::function<void(CommitValidationRecord&)> validateUpdater);


void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

// Starts all fake resolvers specified in the pTestDriverContext.
void startFakeResolver(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

void startFakeTLog(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

void startFakeStorageServer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_DRIVER_H
