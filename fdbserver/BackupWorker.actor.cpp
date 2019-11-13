/*
 * BackupWorker.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BackupContainer.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/BackupInterface.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

struct VersionedMessage {
	LogMessageVersion version;
	StringRef message;
	VectorRef<Tag> tags;
	Arena arena; // Keep a reference to the memory containing the message

	VersionedMessage(LogMessageVersion v, StringRef m, const VectorRef<Tag>& t, const Arena& a)
	  : version(v), message(m), tags(t), arena(a) {}
	const Version getVersion() const { return version.version; }
	const uint32_t getSubVersion() const { return version.sub; }
};

struct BackupData {
	const UID myId;
	const Tag tag; // LogRouter tag for this worker, i.e., (-2, i)
	const Version startVersion;
	const Optional<Version> endVersion; // old epoch's end version (inclusive), or empty for current epoch
	const LogEpoch recruitedEpoch;
	const LogEpoch backupEpoch;
	Version minKnownCommittedVersion;
	Version savedVersion, lastSeenVersion;
	AsyncVar<Reference<ILogSystem>> logSystem;
	Database cx;
	std::vector<VersionedMessage> messages;
	Reference<IBackupContainer> container;
	AsyncVar<bool> pullFinished;

	CounterCollection cc;
	Future<Void> logger;

	explicit BackupData(Reference<AsyncVar<ServerDBInfo>> db, const InitializeBackupRequest& req)
	  : myId(req.reqId), tag(req.routerTag), startVersion(req.startVersion), endVersion(req.endVersion),
	    recruitedEpoch(req.recruitedEpoch), backupEpoch(req.backupEpoch), minKnownCommittedVersion(invalidVersion),
	    savedVersion(invalidVersion), lastSeenVersion(invalidVersion), cc("BackupWorker", myId.toString()) {
		cx = openDBOnServer(db, TaskPriority::DefaultEndpoint, true, true);
		pullFinished.set(false);

		specialCounter(cc, "SavedVersion", [this]() { return this->savedVersion; });
		specialCounter(cc, "MinKnownCommittedVersion", [this]() { return this->minKnownCommittedVersion; });
		specialCounter(cc, "MsgQ", [this]() { return this->messages.size(); });
		logger = traceCounters("BackupWorkerMetrics", myId, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc,
		                       "BackupWorkerMetrics");
		if (g_network->isSimulated()) {
			container = IBackupContainer::openContainer("file://simfdb/mutation_backups/");
		} else {
			// TODO: use blobstore URL passed from somewhere.
			ASSERT(false);
			container = IBackupContainer::openContainer("blobstore://");
		}
	}

	void pop() {
		const LogEpoch oldest = logSystem.get()->getOldestBackupEpoch();
		if (backupEpoch > oldest) {
			// Defer pop if old epoch hasn't finished popping yet.
			TraceEvent("BackupWorkerPopDeferred", myId)
			    .suppressFor(1.0)
			    .detail("BackupEpoch", backupEpoch)
			    .detail("OldestEpoch", oldest)
			    .detail("Version", savedVersion);
			return;
		}
		const Tag popTag = logSystem.get()->getPseudoPopTag(tag, ProcessClass::BackupClass);
		logSystem.get()->pop(savedVersion, popTag);
	}

	void eraseMessagesAfterEndVersion() {
		ASSERT(endVersion.present());
		const Version ver = endVersion.get();
		while (!messages.empty()) {
			if (messages.back().getVersion() > ver) {
				messages.pop_back();
			} else {
				return;
			}
		}
	}
};

ACTOR Future<Void> saveProgress(BackupData* self, Version backupVersion) {
	state Transaction tr(self->cx);
	state Key key = backupProgressKeyFor(self->myId);

	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);

			WorkerBackupStatus status(self->backupEpoch, backupVersion, self->tag);
			tr.set(key, backupProgressValue(status));
			tr.addReadConflictRange(singleKeyRange(key));
			wait(tr.commit());
			return Void();
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Returns true if the message is a mutation that should be backuped, i.e.,
// either key is not in system key space or is not a metadataVersionKey.
bool isBackupMessage(const VersionedMessage& msg) {
	// std::cout << msg.message.printable() << std::endl;
	// std::cout << "Tags: " << describe(msg.tags) << ", Version: " << msg.version.toString() << std::endl;

	for (Tag tag : msg.tags) {
		if (tag.locality == tagLocalitySpecial || tag.locality == tagLocalityTxs) {
			return false; // skip Txs mutations
		}
	}

	BinaryReader reader(msg.message.begin(), msg.message.size(), AssumeVersion(currentProtocolVersion));

	// Return false for LogProtocolMessage.
	if (LogProtocolMessage::isNextIn(reader)) return false;

	MutationRef m;
	reader >> m;

	// check for metadataVersionKey and special metadata mutations
	if (!normalKeys.contains(m.param1) && m.param1 != metadataVersionKey) {
		return false;
	}

	// std::cout << "BK: " << msg.version.version << " " << m.toString() << std::endl;
	return true;
}

// Saves messages in the range of [0, numMsg) to a file and then remove these
// messages. The file format is a sequence of (Version, sub#, msgSize, message),
ACTOR Future<Void> saveMutationsToFile(BackupData* self, Version popVersion, int numMsg) {
	const int blockSize = 1 << 20;
	state Reference<IBackupFile> logFile =
	    wait(self->container->writeTaggedLogFile(self->messages[0].getVersion(), popVersion, blockSize, self->tag.id));
	state int idx = 0;
	for (; idx < numMsg; idx++) {
		// TODO: Endianness for version.version, version.sub, and msgSize
		if (!isBackupMessage(self->messages[idx])) continue;
		wait(logFile->append((void*)&self->messages[idx].version.version, sizeof(Version)));
		wait(logFile->append((void*)&self->messages[idx].version.sub, sizeof(uint32_t)));
		state int msgSize = self->messages[idx].message.size();
		wait(logFile->append((void*)&msgSize, sizeof(msgSize)));
		wait(logFile->append(self->messages[idx].message.begin(), msgSize));
	}

	self->messages.erase(self->messages.begin(), self->messages.begin() + numMsg);
	wait(logFile->finish());
	return Void();
}

// Uploads self->messages to cloud storage and updates savedVersion.
ACTOR Future<Void> uploadData(BackupData* self) {
	state Version popVersion = invalidVersion;

	loop {
		if (self->endVersion.present() && self->savedVersion >= self->endVersion.get()) {
			self->messages.clear();
			return Void();
		}

		// FIXME: knobify the delay of 10s. This delay is sensitive, as it is the
		// lag TLog might have. Changing to 20s may fail consistency check.
		state Future<Void> uploadDelay = delay(10);

		if (self->messages.empty()) {
			// Even though messages is empty, we still want to advance popVersion.
			popVersion = std::max(popVersion, self->lastSeenVersion);
		} else {
			const Version maxPopVersion =
			    self->endVersion.present() ? self->endVersion.get() : self->minKnownCommittedVersion;
			int numMsg = 0;
			for (const auto& message : self->messages) {
				if (message.getVersion() > maxPopVersion) break;
				popVersion = std::max(popVersion, message.getVersion());
				numMsg++;
			}
			if (numMsg > 0) {
				wait(saveMutationsToFile(self, popVersion, numMsg));
			}
		}
		if (self->pullFinished.get() && self->messages.empty()) {
			// Advance popVersion to the endVersion to avoid gap between last
			// message version and the endVersion.
			popVersion = self->endVersion.get();
		}

		if (popVersion > self->savedVersion) {
			wait(saveProgress(self, popVersion));
			TraceEvent("BackupWorkerSavedProgress", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("Version", popVersion)
			    .detail("MsgQ", self->messages.size());
			self->savedVersion = std::max(popVersion, self->savedVersion);
			self->pop();
		}

		if (!self->pullFinished.get()) {
			wait(uploadDelay || self->pullFinished.onChange());
		}
	}
}

// Pulls data from TLog servers using LogRouter tag.
ACTOR Future<Void> pullAsyncData(BackupData* self) {
	state Future<Void> logSystemChange = Void();
	state Reference<ILogSystem::IPeekCursor> r;
	state Version tagAt = self->startVersion;
	state Version lastVersion = 0;

	loop {
		loop choose {
			when (wait(r ? r->getMore(TaskPriority::TLogCommit) : Never())) {
				break;
			}
			when (wait(logSystemChange)) {
				if (self->logSystem.get()) {
					r = self->logSystem.get()->peekLogRouter(self->myId, tagAt, self->tag);
				} else {
					r = Reference<ILogSystem::IPeekCursor>();
				}
				logSystemChange = self->logSystem.onChange();
			}
		}
		self->minKnownCommittedVersion = std::max(self->minKnownCommittedVersion, r->getMinKnownCommittedVersion());

		// Note we aggressively peek (uncommitted) messages, but only committed
		// messages/mutations will be flushed to disk/blob in uploadData().
		while (r->hasMessage()) {
			self->messages.emplace_back(r->version(), r->getMessage(), r->getTags(), r->arena());
			r->nextMessage();
		}

		tagAt = std::max(r->version().version, lastVersion);
		self->lastSeenVersion = std::max(tagAt, self->lastSeenVersion);
		TraceEvent("BackupWorkerGot", self->myId).suppressFor(1.0).detail("V", tagAt);
		if (self->endVersion.present() && tagAt > self->endVersion.get()) {
			self->eraseMessagesAfterEndVersion();
			TraceEvent("BackupWorkerFinishPull", self->myId)
			    .detail("Tag", self->tag.toString())
			    .detail("VersionGot", tagAt)
			    .detail("EndVersion", self->endVersion.get())
			    .detail("MsgQ", self->messages.size());
			self->pullFinished.set(true);
			return Void();
		}
		wait(yield());
	}
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo>> db, LogEpoch recoveryCount,
                                BackupData* self) {
	loop {
		bool isDisplaced =
		    db->get().recoveryCount > recoveryCount && db->get().recoveryState != RecoveryState::UNINITIALIZED;
		if (isDisplaced) {
			TraceEvent("BackupWorkerDisplaced", self->myId)
			    .detail("RecoveryCount", recoveryCount)
			    .detail("SavedVersion", self->savedVersion)
			    .detail("BackupWorkers", describe(db->get().logSystemConfig.tLogs))
			    .detail("DBRecoveryCount", db->get().recoveryCount)
			    .detail("RecoveryState", (int)db->get().recoveryState);
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> backupWorker(BackupInterface interf, InitializeBackupRequest req,
                                Reference<AsyncVar<ServerDBInfo>> db) {
	state BackupData self(db, req);
	state PromiseStream<Future<Void>> addActor;
	state Future<Void> error = actorCollection(addActor.getFuture());
	state Future<Void> dbInfoChange = Void();

	TraceEvent("BackupWorkerStart", self.myId)
	    .detail("Tag", req.routerTag.toString())
	    .detail("StartVersion", req.startVersion)
	    .detail("EndVersion", req.endVersion.present() ? req.endVersion.get() : -1)
	    .detail("LogEpoch", req.recruitedEpoch)
	    .detail("BackupEpoch", req.backupEpoch);
	try {
		addActor.send(pullAsyncData(&self));
		addActor.send(checkRemoved(db, req.recruitedEpoch, &self));
		addActor.send(waitFailureServer(interf.waitFailure.getFuture()));

		state Future<Void> done = uploadData(&self);

		loop choose {
			when(wait(dbInfoChange)) {
				dbInfoChange = db->onChange();
				Reference<ILogSystem> ls = ILogSystem::fromServerDBInfo(self.myId, db->get(), true);
				bool hasPseudoLocality = ls.isValid() && ls->hasPseudoLocality(tagLocalityBackup);
				if (hasPseudoLocality) {
					self.logSystem.set(ls);
					self.pop();
				}
				TraceEvent("BackupWorkerLogSystem", self.myId)
				    .detail("HasBackupLocality", hasPseudoLocality)
				    .detail("Tag", self.tag.toString());
			}
			when(wait(done)) {
				TraceEvent("BackupWorkerDone", self.myId).detail("BackupEpoch", self.backupEpoch);
				// Notify master so that this worker can be removed from log system, then this
				// worker (for an old epoch's unfinished work) can safely exit.
				wait(brokenPromiseToNever(db->get().master.notifyBackupWorkerDone.getReply(
				    BackupWorkerDoneRequest(self.myId, self.backupEpoch))));
				break;
			}
			when(wait(error)) {}
		}
	} catch (Error& e) {
		TraceEvent("BackupWorkerTerminated", self.myId).error(e, true);
		if (e.code() != error_code_actor_cancelled && e.code() != error_code_worker_removed) {
			throw;
		}
	}
	return Void();
}