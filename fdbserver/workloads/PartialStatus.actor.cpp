/*
 * PartialStatus.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/StatusClient.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// This workload tests a gray failure scenario: primary storage servers have network issue
// communicating with the data distributor. As a result, the status json is incomplete and
// reporting unhealthy servers.
struct PartialStatusWorkload : TestWorkload {
	static constexpr auto NAME = "PartialStatus";
	bool enabled;
	double testDuration;
	double startDelay;
	std::vector<std::pair<IPAddress, IPAddress>> cloggedPairs;

	PartialStatusWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, "testDuration"_sr, 1000.0);
		startDelay = getOption(options, "startDelay"_sr, 10.0);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (g_network->isSimulated() && enabled)
			return timeout(reportErrors(clogClient(this, cx), "PartialStatusError"), testDuration, Void());
		else
			return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Returns a set of storage servers that are not protected, i.e., coordinators.
	ACTOR static Future<std::set<IPAddress>> getStorageServers(Database cx) {
		state std::set<IPAddress> servers;
		state Transaction tr(cx);
		loop {
			try {
				std::vector<std::pair<StorageServerInterface, ProcessClass>> results =
				    wait(NativeAPI::getServerListAndProcessClasses(&tr));
				for (auto& [ssi, p] : results) {
					// if (g_simulator->protectedAddresses.count(ssi.address()) == 0) {
					servers.insert(ssi.address().ip);
					//}
				}
				return servers;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Clog remote SS to DD connections.
	bool clogDDandSS(std::set<IPAddress> sses, double seconds) {
		ASSERT(dbInfo->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION);

		std::vector<IPAddress> ips; // all remote process IPs
		for (const auto& process : g_simulator->getAllProcesses()) {
			const auto& ip = process->address.ip;
			if (process->locality.dcId().present() && process->locality.dcId() == g_simulator->primaryDcId &&
			    sses.count(ip) > 0) {
				ips.push_back(ip);
			}
		}
		ASSERT(ips.size() > 0);

		// Find DD
		ASSERT(dbInfo->get().distributor.present());
		auto dd = dbInfo->get().distributor.get().address().ip;

		// clog DD and remote SSes
		for (const auto& ip : ips) {
			if (dd != ip) {
				g_simulator->clogPair(ip, dd, seconds);
				g_simulator->clogPair(dd, ip, seconds);
				cloggedPairs.emplace_back(ip, dd);
				cloggedPairs.emplace_back(dd, ip);
			}
		}
		return true;
	}

	void unclogAll() {
		// unclog previously clogged connections
		for (const auto& pair : cloggedPairs) {
			g_simulator->unclogPair(pair.first, pair.second);
		}
		cloggedPairs.clear();
	}

	ACTOR static Future<Optional<double>> fetchUnhealthySS(PartialStatusWorkload* self, Database cx) {
		StatusObject result = wait(StatusClient::statusFetcher(cx));
		StatusObjectReader statusObj(result);
		StatusObjectReader statusObjCluster;
		if (!statusObj.get("cluster", statusObjCluster)) {
			TraceEvent("PartialStatusNoCluster");
			return Optional<double>();
		}

		StatusObjectReader dataObj;
		if (!statusObjCluster.get("data", dataObj)) {
			TraceEvent("PartialStatusNoData");
			return Optional<double>();
		}

		if (!dataObj.has("team_trackers")) {
			TraceEvent("PartialStatusNoTracker");
			return Optional<double>();
		}
		StatusArray trackers = dataObj["team_trackers"].get_array();

		double unhealthySS = 0;
		for (StatusObjectReader trackerObj : trackers) {
			bool isPrimary = false;
			if (!trackerObj.get("primary", isPrimary)) {
				TraceEvent("PartialStatusNoPrimary");
			}
			double count;
			if (!trackerObj.get("unhealthy_servers", count)) {
				TraceEvent("PartialStatusNoUnhealthy").detail("Primary", isPrimary);
			}
			TraceEvent("PartialStatusUnhealthy").detail("Unhealthy", count).detail("Primary", isPrimary);
			unhealthySS += count;
		}

		TraceEvent("PartialStatusPrimaryUnhealthy").detail("Unhealthy", unhealthySS);
		return unhealthySS;
	}

	ACTOR Future<Void> clogClient(PartialStatusWorkload* self, Database cx) {
		wait(delay(self->startDelay));

		while (self->dbInfo->get().recoveryState < RecoveryState::FULLY_RECOVERED) {
			wait(self->dbInfo->onChange());
		}

		double startTime = now();
		double workloadEnd = now() + self->testDuration;
		TraceEvent("PartialStatus").detail("StartTime", startTime).detail("EndTime", workloadEnd);

		std::set<IPAddress> SSes = wait(self->getStorageServers(cx));
		TraceEvent("PartialStatus").detail("SSes", SSes.size());

		// Clog and wait for recovery to happen
		if (!self->clogDDandSS(SSes, self->testDuration)) {
			return Void(); // skip the test if no satellite found
		}
		TraceEvent("PartialStatusClogged");

		state Future<Optional<double>> status = Never();
		loop choose {
			when(wait(delay(5.0))) {
				// Fetch unhealthy SSes from status every 5s
				status = fetchUnhealthySS(self, cx);
			}
			when(Optional<double> unhealthySS = wait(status)) {
				TraceEvent("PartialStatusUnhealthySS")
				    .detail("UnhealthySS", unhealthySS.present() ? unhealthySS.get() : -1);
				if (unhealthySS.present() && unhealthySS.get() >= 5) {
					// Detected unhealthy SSes
					return Void();
				}
				status = Never();
			}
		}
	}
};

WorkloadFactory<PartialStatusWorkload> PartialStatusWorkloadFactory;
