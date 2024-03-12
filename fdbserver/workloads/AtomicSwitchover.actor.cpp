/*
 * AtomicSwitchover.actor.cpp
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

#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// A workload which test the correctness of backup and restore process
struct AtomicSwitchoverWorkload : TestWorkload {
	double switch1delay, switch2delay, stopDelay;
	Standalone<VectorRef<KeyRangeRef>> backupRanges;
	Database extraDB;

	AtomicSwitchoverWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {

		switch1delay = getOption(options, LiteralStringRef("switch1delay"), 50.0);
		switch2delay = getOption(options, LiteralStringRef("switch2delay"), 50.0);
		stopDelay = getOption(options, LiteralStringRef("stopDelay"), 50.0);

		backupRanges.push_back_deep(backupRanges.arena(), normalKeys);

		Reference<ClusterConnectionFile> extraFile(new ClusterConnectionFile(*g_simulator.extraDB));
		extraDB = Database::createDatabase(extraFile, -1);
	}

	virtual std::string description() { return "AtomicSwitchover"; }

	virtual Future<Void> setup(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _setup(cx, this);
	}

	ACTOR static Future<Void> _setup(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		try {
			TraceEvent("AS_Submit1");
			wait(backupAgent.submitBackup(self->extraDB,
			                              BackupAgentBase::getDefaultTag(),
			                              self->backupRanges,
			                              false,
			                              StringRef(),
			                              StringRef(),
			                              true));
			TraceEvent("AS_Submit2");
		} catch (Error& e) {
			if (e.code() != error_code_backup_duplicate)
				throw;
		}
		return Void();
	}

	virtual Future<Void> start(Database const& cx) {
		if (clientId != 0)
			return Void();
		return _start(cx, this);
	}

	virtual Future<bool> check(Database const& cx) { return true; }

	virtual void getMetrics(vector<PerfMetric>& m) {}

	ACTOR static Future<Void> _start(Database cx, AtomicSwitchoverWorkload* self) {
		state DatabaseBackupAgent backupAgent(cx);
		state DatabaseBackupAgent restoreTool(self->extraDB);

		TraceEvent("AS_Wait1");
		wait(success(backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), false)));
		TraceEvent("AS_Ready1");
		wait(delay(deterministicRandom()->random01() * self->switch1delay));
		TraceEvent("AS_Switch1");
		wait(backupAgent.atomicSwitchover(
		    self->extraDB, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()));
		TraceEvent("AS_Wait2");
		wait(success(restoreTool.waitBackup(cx, BackupAgentBase::getDefaultTag(), false)));
		TraceEvent("AS_Ready2");
		wait(delay(deterministicRandom()->random01() * self->switch2delay));
		TraceEvent("AS_Switch2");
		wait(restoreTool.atomicSwitchover(
		    cx, BackupAgentBase::getDefaultTag(), self->backupRanges, StringRef(), StringRef()));
		TraceEvent("AS_Wait3");
		wait(success(backupAgent.waitBackup(self->extraDB, BackupAgentBase::getDefaultTag(), false)));
		TraceEvent("AS_Ready3");
		wait(delay(deterministicRandom()->random01() * self->stopDelay));
		TraceEvent("AS_Abort");
		wait(backupAgent.abortBackup(self->extraDB, BackupAgentBase::getDefaultTag()));
		TraceEvent("AS_Done");

		// SOMEDAY: Remove after backup agents can exist quiescently
		if (g_simulator.drAgents == ISimulator::BackupToDB) {
			g_simulator.drAgents = ISimulator::NoBackupAgents;
		}

		return Void();
	}
};

WorkloadFactory<AtomicSwitchoverWorkload> AtomicSwitchoverWorkloadFactory("AtomicSwitchover");
