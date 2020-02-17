/*
 * RestoreMaster.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

// This file implements the functions for RestoreMaster role

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/MutationList.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/RestoreUtil.h"
#include "fdbserver/RestoreCommon.actor.h"
#include "fdbserver/RestoreRoleCommon.actor.h"
#include "fdbserver/RestoreMaster.actor.h"
#include "fdbserver/RestoreApplier.actor.h"
#include "fdbserver/RestoreLoader.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

ACTOR static Future<Void> clearDB(Database cx);
ACTOR static Future<Void> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* rangeFiles,
                                             std::vector<RestoreFileFR>* logFiles, Database cx, RestoreRequest request);

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx, RestoreRequest request);
ACTOR static Future<Void> startProcessRestoreRequests(Reference<RestoreMasterData> self, Database cx);
ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx,
                                                            RestoreRequest request, VersionBatch versionBatch);

ACTOR static Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                              Reference<RestoreMasterData> masterData);
ACTOR static Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                                   Reference<RestoreMasterData> masterData);

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx);
ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self);
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx);

void splitKeyRangeForAppliers(Reference<RestoreMasterData> self);

ACTOR Future<Void> startRestoreMaster(Reference<RestoreWorkerData> masterWorker, Database cx) {
	state Reference<RestoreMasterData> self = Reference<RestoreMasterData>(new RestoreMasterData());

	try {
		// recruitRestoreRoles must come after masterWorker has finished collectWorkerInterface
		wait(recruitRestoreRoles(masterWorker, self));

		wait(distributeRestoreSysInfo(masterWorker, self));

		wait(startProcessRestoreRequests(self, cx));
	} catch (Error& e) {
		TraceEvent(SevError, "FastRestore")
		    .detail("StartRestoreMaster", "Unexpectedly unhandled error")
		    .detail("Error", e.what())
		    .detail("ErrorCode", e.code());
	}

	return Void();
}

// RestoreWorker that has restore master role: Recruite a role for each worker
ACTOR Future<Void> recruitRestoreRoles(Reference<RestoreWorkerData> masterWorker,
                                       Reference<RestoreMasterData> masterData) {
	state int nodeIndex = 0;
	state RestoreRole role = RestoreRole::Invalid;

	TraceEvent("FastRestore")
	    .detail("RecruitRestoreRoles", masterWorker->workerInterfaces.size())
	    .detail("NumLoaders", opConfig.num_loaders)
	    .detail("NumAppliers", opConfig.num_appliers);
	ASSERT(masterData->loadersInterf.empty() && masterData->appliersInterf.empty());

	ASSERT(masterData.isValid());
	ASSERT(opConfig.num_loaders > 0 && opConfig.num_appliers > 0);
	// We assign 1 role per worker for now
	ASSERT(opConfig.num_loaders + opConfig.num_appliers <= masterWorker->workerInterfaces.size());

	// Assign a role to each worker
	std::vector<std::pair<UID, RestoreRecruitRoleRequest>> requests;
	for (auto& workerInterf : masterWorker->workerInterfaces) {
		if (nodeIndex >= 0 && nodeIndex < opConfig.num_appliers) {
			// [0, numApplier) are appliers
			role = RestoreRole::Applier;
		} else if (nodeIndex >= opConfig.num_appliers && nodeIndex < opConfig.num_loaders + opConfig.num_appliers) {
			// [numApplier, numApplier + numLoader) are loaders
			role = RestoreRole::Loader;
		} else {
			break;
		}

		TraceEvent("FastRestore")
		    .detail("Role", getRoleStr(role))
		    .detail("NodeIndex", nodeIndex)
		    .detail("WorkerNode", workerInterf.first);
		requests.emplace_back(workerInterf.first, RestoreRecruitRoleRequest(role, nodeIndex));
		nodeIndex++;
	}

	state std::vector<RestoreRecruitRoleReply> replies;
	wait(getBatchReplies(&RestoreWorkerInterface::recruitRole, masterWorker->workerInterfaces, requests, &replies));
	for (auto& reply : replies) {
		if (reply.role == RestoreRole::Applier) {
			ASSERT_WE_THINK(reply.applier.present());
			masterData->appliersInterf[reply.applier.get().id()] = reply.applier.get();
		} else if (reply.role == RestoreRole::Loader) {
			ASSERT_WE_THINK(reply.loader.present());
			masterData->loadersInterf[reply.loader.get().id()] = reply.loader.get();
		} else {
			TraceEvent(SevError, "FastRestore").detail("RecruitRestoreRoles_InvalidRole", reply.role);
		}
	}
	TraceEvent("FastRestore").detail("RecruitRestoreRolesDone", masterWorker->workerInterfaces.size());

	return Void();
}

ACTOR Future<Void> distributeRestoreSysInfo(Reference<RestoreWorkerData> masterWorker,
                                            Reference<RestoreMasterData> masterData) {
	ASSERT(masterData.isValid());
	ASSERT(!masterData->loadersInterf.empty());
	RestoreSysInfo sysInfo(masterData->appliersInterf);
	std::vector<std::pair<UID, RestoreSysInfoRequest>> requests;
	for (auto& loader : masterData->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreSysInfoRequest(sysInfo)));
	}

	TraceEvent("FastRestore").detail("DistributeRestoreSysInfoToLoaders", masterData->loadersInterf.size());
	wait(sendBatchRequests(&RestoreLoaderInterface::updateRestoreSysInfo, masterData->loadersInterf, requests));

	return Void();
}

// The server of the restore master. It drives the restore progress with the following steps:
// 1) Lock database and clear the normal keyspace
// 2) Wait on each RestoreRequest, which is sent by RestoreAgent operated by DBA
// 3) Process each restore request in actor processRestoreRequest;
// 3.1) Sample workload to decide the key range for each applier, which is implemented as a dummy sampling;
// 3.2) Send each loader the map of key-range to applier interface;
// 3.3) Construct requests of which file should be loaded by which loader, and send requests to loaders;
// 4) After process all restore requests, finish restore by cleaning up the restore related system key
//    and ask all restore roles to quit.
ACTOR Future<Void> startProcessRestoreRequests(Reference<RestoreMasterData> self, Database cx) {
	state UID randomUID = deterministicRandom()->randomUniqueID();
	state Standalone<VectorRef<RestoreRequest>> restoreRequests = wait(collectRestoreRequests(cx));
	state int numTries = 0;
	state int restoreIndex = 0;

	TraceEvent("FastRestore").detail("RestoreMaster", "WaitOnRestoreRequests");

	// lock DB for restore
	numTries = 0;
	loop {
		try {
			wait(lockDatabase(cx, randomUID));
			state Reference<ReadYourWritesTransaction> tr =
			    Reference<ReadYourWritesTransaction>(new ReadYourWritesTransaction(cx));
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			wait(checkDatabaseLock(tr, randomUID));
			TraceEvent("FastRestore").detail("DBIsLocked", randomUID);
			break;
		} catch (Error& e) {
			TraceEvent("FastRestore").detail("CheckLockError", e.what());
			TraceEvent(numTries > 50 ? SevError : SevWarnAlways, "FastRestoreMayFail")
			    .detail("Reason", "DB is not properly locked")
			    .detail("ExpectedLockID", randomUID);
			numTries++;
			wait(delay(5.0));
		}
	}

	wait(clearDB(cx));

	// Step: Perform the restore requests
	try {
		for (restoreIndex = 0; restoreIndex < restoreRequests.size(); restoreIndex++) {
			RestoreRequest& request = restoreRequests[restoreIndex];
			TraceEvent("FastRestore").detail("RestoreRequestInfo", request.toString());
			wait(success(processRestoreRequest(self, cx, request)));
		}
	} catch (Error& e) {
		if (restoreIndex < restoreRequests.size()) {
			TraceEvent(SevError, "FastRestoreFailed")
			    .detail("RestoreRequest", restoreRequests[restoreIndex].toString());
		} else {
			TraceEvent(SevError, "FastRestoreFailed")
			    .detail("RestoreRequests", restoreRequests.size())
			    .detail("RestoreIndex", restoreIndex);
		}
	}

	// Step: Notify all restore requests have been handled by cleaning up the restore keys
	wait(notifyRestoreCompleted(self, cx));

	try {
		wait(unlockDatabase(cx, randomUID));
	} catch (Error& e) {
		TraceEvent(SevError, "UnlockDBFailed").detail("UID", randomUID.toString());
		ASSERT_WE_THINK(false); // This unlockDatabase should always succeed, we think.
	}


	TraceEvent("FastRestore").detail("RestoreMasterComplete", self->id());

	return Void();
}

ACTOR static Future<Version> processRestoreRequest(Reference<RestoreMasterData> self, Database cx,
                                                   RestoreRequest request) {
	state std::vector<RestoreFileFR> rangeFiles;
	state std::vector<RestoreFileFR> logFiles;
	state std::vector<RestoreFileFR> allFiles;
	state std::map<Version, VersionBatch>::iterator versionBatch = self->versionBatches.begin();

	self->initBackupContainer(request.url);

	// Get all backup files' description and save them to files
	wait(collectBackupFiles(self->bc, &rangeFiles, &logFiles, cx, request));

	std::sort(rangeFiles.begin(), rangeFiles.end());
	std::sort(logFiles.begin(), logFiles.end(), [](RestoreFileFR const& f1, RestoreFileFR const& f2) -> bool {
		return std::tie(f1.endVersion, f1.beginVersion, f1.fileIndex) <
		       std::tie(f2.endVersion, f2.beginVersion, f2.fileIndex);
	});

	// TODO for Jingyu: Verify new backup files are grouped into correct version batches.
	self->buildVersionBatches(rangeFiles, logFiles, &self->versionBatches); // Divide files into version batches
	self->dumpVersionBatches(self->versionBatches);

	ASSERT(self->batchIndex == 1); // versionBatchIndex starts at 1 because NotifiedVersion starts at 0
	for (versionBatch = self->versionBatches.begin(); versionBatch != self->versionBatches.end(); versionBatch++) {
		wait(initializeVersionBatch(self));
		wait(distributeWorkloadPerVersionBatch(self, cx, request, versionBatch->second));
		self->batchIndex++;
	}

	TraceEvent("FastRestore").detail("RestoreToVersion", request.targetVersion);
	return request.targetVersion;
}

ACTOR static Future<Void> loadFilesOnLoaders(Reference<RestoreMasterData> self, Database cx, RestoreRequest request,
                                             VersionBatch versionBatch, bool isRangeFile) {
	TraceEvent("FastRestore")
	    .detail("FileTypeLoadedInVersionBatch", isRangeFile)
	    .detail("BeginVersion", versionBatch.beginVersion)
	    .detail("EndVersion", versionBatch.endVersion);

	std::vector<RestoreFileFR>* files = isRangeFile ? &versionBatch.rangeFiles : &versionBatch.logFiles;

	// sort files in increasing order of beginVersion
	std::sort(files->begin(), files->end());

	std::vector<std::pair<UID, RestoreLoadFileRequest>> requests;
	std::map<UID, RestoreLoaderInterface>::iterator loader = self->loadersInterf.begin();

	for (auto& file : *files) {
		// NOTE: Cannot skip empty files because empty files, e.g., log file, still need to generate dummy mutation to
		// drive applier's NotifiedVersion.
		if (loader == self->loadersInterf.end()) {
			loader = self->loadersInterf.begin();
		}
		// Prepare loading
		LoadingParam param;
		param.url = request.url;
		param.isRangeFile = file.isRange;
		param.rangeVersion = file.isRange ? file.version : -1;
		param.blockSize = file.blockSize;

		param.asset.uid = deterministicRandom()->randomUniqueID();
		param.asset.filename = file.fileName;
		param.asset.fileIndex = file.fileIndex;
		param.asset.partitionId = file.partitionId;
		param.asset.offset = 0;
		param.asset.len = file.fileSize;
		param.asset.range = request.range;
		param.asset.beginVersion = versionBatch.beginVersion;
		param.asset.endVersion = versionBatch.endVersion;

		TraceEvent("FastRestore").detail("LoadParam", param.toString()).detail("LoaderID", loader->first.toString());
		ASSERT_WE_THINK(param.asset.len > 0);
		ASSERT_WE_THINK(param.asset.offset >= 0);
		ASSERT_WE_THINK(param.asset.offset <= file.fileSize);
		ASSERT_WE_THINK(param.asset.beginVersion <= param.asset.endVersion);

		requests.emplace_back(loader->first, RestoreLoadFileRequest(param));
		loader++;
	}

	state std::vector<RestoreLoadFileReply> replies;
	// Wait on the batch of load files or log files
	wait(getBatchReplies(&RestoreLoaderInterface::loadFile, self->loadersInterf, requests, &replies));
	TraceEvent("FastRestore").detail("VersionBatch", self->batchIndex).detail("SamplingReplies", replies.size());
	for (auto& reply : replies) {
		TraceEvent("FastRestore").detail("VersionBatch", self->batchIndex).detail("SamplingReplies", reply.toString());
		for (int i = 0; i < reply.samples.size(); ++i) {
			MutationRef mutation = reply.samples[i];
			self->samples.addMetric(mutation.param1, mutation.totalSize());
			self->samplesSize += mutation.totalSize();
		}
	}

	return Void();
}

// Ask loaders to send its buffered mutations to appliers
ACTOR static Future<Void> sendMutationsFromLoaders(Reference<RestoreMasterData> self, bool useRangeFile) {
	TraceEvent("FastRestore")
	    .detail("SendMutationsFromLoaders", self->batchIndex)
	    .detail("UseRangeFiles", useRangeFile);

	std::vector<std::pair<UID, RestoreSendMutationsToAppliersRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.emplace_back(loader.first, RestoreSendMutationsToAppliersRequest(self->rangeToApplier, useRangeFile));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::sendMutations, self->loadersInterf, requests));

	return Void();
}

ACTOR static Future<Void> distributeWorkloadPerVersionBatch(Reference<RestoreMasterData> self, Database cx,
                                                            RestoreRequest request, VersionBatch versionBatch) {
	ASSERT(!versionBatch.isEmpty());
	ASSERT(self->loadersInterf.size() > 0);
	ASSERT(self->appliersInterf.size() > 0);

	// Parse log files and send mutations to appliers before we parse range files
	// TODO: Allow loading both range and log files in parallel
	wait(loadFilesOnLoaders(self, cx, request, versionBatch, false));
	wait(loadFilesOnLoaders(self, cx, request, versionBatch, true));

	splitKeyRangeForAppliers(self);

	// Loaders should ensure log files' mutations sent to appliers before range files' mutations
	// TODO: Let applier buffer mutations from log and range files differently so that loaders can send mutations in
	// parallel
	wait(sendMutationsFromLoaders(self, false));
	wait(sendMutationsFromLoaders(self, true));

	wait(notifyApplierToApplyMutations(self));

	return Void();
}

// Decide which key range should be taken by which applier
void splitKeyRangeForAppliers(Reference<RestoreMasterData> self) {
	ASSERT(self->samplesSize >= 0);
	int numAppliers = self->appliersInterf.size();
	double slotSize = std::max(self->samplesSize / numAppliers, 1.0);
	std::vector<Key> keyrangeSplitter;
	keyrangeSplitter.push_back(normalKeys.begin); // First slot
	double cumulativeSize = slotSize;
	TraceEvent("FastRestore").detail("VersionBatch", self->batchIndex).detail("SamplingSize", self->samplesSize);
	while (cumulativeSize < self->samplesSize) {
		IndexedSet<Key, int64_t>::iterator lowerBound = self->samples.index(cumulativeSize);
		if (lowerBound == self->samples.end()) {
			break;
		}
		keyrangeSplitter.push_back(*lowerBound);
		TraceEvent("FastRestore")
		    .detail("VersionBatch", self->batchIndex)
		    .detail("CumulativeSize", cumulativeSize)
		    .detail("SlotSize", slotSize);
		cumulativeSize += slotSize;
	}
	if (keyrangeSplitter.size() < numAppliers) {
		TraceEvent(SevWarnAlways, "FastRestore")
		    .detail("NotAllAppliersAreUsed", keyrangeSplitter.size())
		    .detail("NumAppliers", numAppliers);
	} else if (keyrangeSplitter.size() > numAppliers) {
		TraceEvent(SevError, "FastRestore")
		    .detail("TooManySlotsThanAppliers", keyrangeSplitter.size())
		    .detail("NumAppliers", numAppliers);
	}
	// std::sort(keyrangeSplitter.begin(), keyrangeSplitter.end());
	int i = 0;
	self->rangeToApplier.clear();
	for (auto& applier : self->appliersInterf) {
		if (i >= keyrangeSplitter.size()) {
			break; // Not all appliers will be used
		}
		self->rangeToApplier[keyrangeSplitter[i]] = applier.first;
		i++;
	}
	ASSERT(self->rangeToApplier.size() > 0);
	ASSERT(self->sanityCheckApplierKeyRange());
	self->logApplierKeyRange();
}

ACTOR static Future<Standalone<VectorRef<RestoreRequest>>> collectRestoreRequests(Database cx) {
	state Standalone<VectorRef<RestoreRequest>> restoreRequests;
	state Future<Void> watch4RestoreRequest;
	state ReadYourWritesTransaction tr(cx);

	// wait for the restoreRequestTriggerKey to be set by the client/test workload
	loop {
		try {
			tr.reset();
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> numRequests = wait(tr.get(restoreRequestTriggerKey));
			if (!numRequests.present()) {
				watch4RestoreRequest = tr.watch(restoreRequestTriggerKey);
				wait(tr.commit());
				wait(watch4RestoreRequest);
			} else {
				Standalone<RangeResultRef> restoreRequestValues =
				    wait(tr.getRange(restoreRequestKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!restoreRequestValues.more);
				if (restoreRequestValues.size()) {
					for (auto& it : restoreRequestValues) {
						restoreRequests.push_back(restoreRequests.arena(), decodeRestoreRequestValue(it.value));
						TraceEvent("FastRestore").detail("RestoreRequest", restoreRequests.back().toString());
					}
				}
				break;
			}
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	return restoreRequests;
}

// Collect the backup files' description into output_files by reading the backupContainer bc.
ACTOR static Future<Void> collectBackupFiles(Reference<IBackupContainer> bc, std::vector<RestoreFileFR>* rangeFiles,
                                             std::vector<RestoreFileFR>* logFiles, Database cx,
                                             RestoreRequest request) {
	state BackupDescription desc = wait(bc->describeBackup());

	// Convert version to real time for operators to read the BackupDescription desc.
	wait(desc.resolveVersionTimes(cx));
	TraceEvent("FastRestore").detail("BackupDesc", desc.toString());

	if (request.targetVersion == invalidVersion && desc.maxRestorableVersion.present()) {
		request.targetVersion = desc.maxRestorableVersion.get();
	}

	Optional<RestorableFileSet> restorable = wait(bc->getPartitionedRestoreSet(request.targetVersion));

	if (!restorable.present()) {
		TraceEvent(SevWarn, "FastRestore").detail("NotRestorable", request.targetVersion);
		throw restore_missing_data();
	}

	ASSERT(rangeFiles->empty());
	ASSERT(logFiles->empty());

	for (const RangeFile& f : restorable.get().ranges) {
		TraceEvent("FastRestore").detail("RangeFile", f.toString());
		if (f.fileSize <= 0) {
			continue;
		}
		RestoreFileFR file(f);
		TraceEvent("FastRestore").detail("RangeFileFR", file.toString());
		rangeFiles->push_back(file);
	}
	for (const LogFile& f : restorable.get().logs) {
		TraceEvent("FastRestore").detail("LogFile", f.toString());
		if (f.fileSize <= 0) {
			continue;
		}
		RestoreFileFR file(f);
		TraceEvent("FastRestore").detail("LogFileFR", file.toString());
		logFiles->push_back(file);
	}

	return Void();
}

ACTOR static Future<Void> clearDB(Database cx) {
	wait(runRYWTransaction(cx, [](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
		tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr->setOption(FDBTransactionOptions::LOCK_AWARE);
		tr->clear(normalKeys);
		return Void();
	}));

	return Void();
}

ACTOR static Future<Void> initializeVersionBatch(Reference<RestoreMasterData> self) {

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToAppliers;
	for (auto& applier : self->appliersInterf) {
		requestsToAppliers.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::initVersionBatch, self->appliersInterf, requestsToAppliers));

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requestsToLoaders;
	for (auto& loader : self->loadersInterf) {
		requestsToLoaders.push_back(std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreLoaderInterface::initVersionBatch, self->loadersInterf, requestsToLoaders));

	self->resetPerVersionBatch();

	return Void();
}

// Ask each applier to apply its received mutations to DB
ACTOR static Future<Void> notifyApplierToApplyMutations(Reference<RestoreMasterData> self) {
	// Prepare the applyToDB requests
	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto& applier : self->appliersInterf) {
		requests.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	wait(sendBatchRequests(&RestoreApplierInterface::applyToDB, self->appliersInterf, requests));

	TraceEvent("FastRestore").detail("Master", self->id()).detail("ApplyToDB", "Completed");
	return Void();
}

// Ask all loaders and appliers to perform housecleaning at the end of restore and
// Register the restoreRequestDoneKey to signal the end of restore
ACTOR static Future<Void> notifyRestoreCompleted(Reference<RestoreMasterData> self, Database cx) {
	state Reference<ReadYourWritesTransaction> tr(new ReadYourWritesTransaction(cx));

	std::vector<std::pair<UID, RestoreVersionBatchRequest>> requests;
	for (auto& loader : self->loadersInterf) {
		requests.push_back(std::make_pair(loader.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	// A loader exits immediately after it receives the request. Master may not receive acks.
	Future<Void> endLoaders = sendBatchRequests(&RestoreLoaderInterface::finishRestore, self->loadersInterf, requests);

	requests.clear();
	for (auto& applier : self->appliersInterf) {
		requests.push_back(std::make_pair(applier.first, RestoreVersionBatchRequest(self->batchIndex)));
	}
	Future<Void> endApplier =
	    sendBatchRequests(&RestoreApplierInterface::finishRestore, self->appliersInterf, requests);

	wait(delay(5.0)); // Give some time for loaders and appliers to exit

	// Notify tester that the restore has finished
	loop {
		try {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->clear(restoreRequestTriggerKey);
			tr->clear(restoreRequestKeys);
			Version readVersion = wait(tr->getReadVersion());
			tr->set(restoreRequestDoneKey, restoreRequestDoneVersionValue(readVersion));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	TraceEvent("FastRestore").detail("RestoreMaster", "RestoreCompleted");

	return Void();
}
