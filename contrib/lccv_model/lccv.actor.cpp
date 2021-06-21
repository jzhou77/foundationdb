/*
 * Model the latest committed version vector (LCVV) read performance.
 */

#include <iostream>
#include <memory>
#include <random>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Notified.h"
#include "fdbrpc/simulator.h"
#include "flow/ActorCollection.h"
#include "flow/Histogram.h"
#include "flow/IRandom.h"
#include "flow/Platform.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/TLSConfig.actor.h"
#include "flow/Trace.h"


struct WriteRequest {
	int id;
	double duration;
	double startTime;
	Version readVersion;
	Version prevVersion;
	Version commitVersion;

	static int count;
	WriteRequest(double duration_, Version rv, Version pcv, Version cv)
	  : id(count), duration(duration_), startTime(g_network->now()), readVersion(rv), prevVersion(pcv),
	    commitVersion(cv) {
		count++;
	}

	std::string toString() {
		return format(
		    "%d duration=%f RV=%lld PCV=%lld CV=%lld", id, duration, readVersion, prevVersion, commitVersion);
	}
};
int WriteRequest::count = 0;

struct SharedState {
	// Sequencer's most recent commit version that has been given out.
	Version writeVersion = 0;

	// Most recent committed version
	NotifiedVersion currentVersion;

	int64_t reads = 0;
	int64_t writes = 0;
	double startTime;
	double stopAfter = 1000;
	PromiseStream<Future<Void>> addActor;
	Reference<Histogram> wh;
	Reference<Histogram> rh;

	// Exponential distribution's lambda
	double writeArrival;
	double writeDuration;
	double readArrival;

	std::default_random_engine generator;
	std::exponential_distribution<double> dWriteDuration;
	std::exponential_distribution<double> dWriteArrival;
	std::exponential_distribution<double> dReadArrival;

	SharedState(double wArrival, double wDuration, double rArrival)
	  : startTime(g_network->now()), wh(Histogram::getHistogram("lccv"_sr, "writes"_sr, Histogram::Unit::microseconds)),
	    rh(Histogram::getHistogram("lccv"_sr, "reads"_sr, Histogram::Unit::microseconds)), writeArrival(wArrival),
	    writeDuration(wDuration), readArrival(rArrival), generator(141), dWriteDuration(wDuration),
	    dWriteArrival(wArrival), dReadArrival(rArrival) {
		std::cout << format("Start at: %.2f, write arrival %f, write duration %f, read arrival %f",
		                    startTime,
		                    writeArrival,
		                    writeDuration,
		                    readArrival)
		          << "\n";
	}

	ACTOR static Future<Void> updateVersion_impl(SharedState* self, WriteRequest req) {
		wait(delay(req.duration)); // processing delay
		wait(self->currentVersion.whenAtLeast(req.prevVersion));
		if (self->currentVersion.get() == req.prevVersion) {
			double duration = g_network->now() - req.startTime;
			// adds to stats
			self->wh->sampleSeconds(duration);
			std::cout << "Sample " << req.toString() << ", duration " << duration << "\n";

			// std::cout << "Current " << self->currentVersion.get() << ", to " << req.commitVersion << ", req " << req.id << "\n";
			self->currentVersion.set(req.commitVersion);
		} else {
			ASSERT(false); // versions must be updated in order
		}
		return Void();
	}

	Future<Void> updateVersion(const WriteRequest& req) {
		return updateVersion_impl(this, req);
	}

	Version getReadVersion() const {
		return currentVersion.get();
	}

	void addWriteRequest(const WriteRequest& req) {
		writes++;
		addActor.send(updateVersion(req));
	}

	ACTOR static Future<Void> read_impl(SharedState* self) {
		state Version rv = self->currentVersion.get();
		state Version recentCV = self->writeVersion;
		state double startTime = g_network->now();

		if (rv == recentCV) {
			// adds to stats
			self->rh->sampleSeconds(0.02); // assume 2ms read latency
		} else {
			// Delay to read most recent commit, which is not exactly
			// what I want to measure.
			wait(self->currentVersion.whenAtLeast(recentCV));
			double duration = g_network->now() - startTime;
			self->rh->sampleSeconds(duration);
		}
		return Void();
	}

	Future<Void> read() {
		return read_impl(this);
	}

	void addReadRequest() {
		addActor.send(read());
	}

	// Returns (previous commit version, commit version) pair
	std::pair<Version, Version> getCommitVersion() {
		std::pair<Version, Version> p;
		p.first = writeVersion;
		writeVersion += deterministicRandom()->randomInt64(10, 101);
		p.second = writeVersion;
		return p;
	}

	void dump() {
		std::cout << "Total reads: " << reads << ", total writes: " << writes << ", Version: " << writeVersion << "\n";
		GetHistogramRegistry().logReport();
	}
};

// Generates write requests
ACTOR Future<Void> writes(std::shared_ptr<SharedState> stats) {
	loop {
		state Version readVersion = stats->getReadVersion();

		// model delay before commits, i.e., client processing delay
		double arrivalDelay = stats->dWriteDuration(stats->generator);
		wait(delay(arrivalDelay));

		// get PCV, CV pair
		std::pair<Version, Version> p = stats->getCommitVersion();

		double processingTime = stats->dWriteDuration(stats->generator);
		// at least 20ms
		processingTime = processingTime < 0.02 ? 0.02 : processingTime;
		// at most 4s
		processingTime = processingTime > 4 ? 4 : processingTime;

		WriteRequest req(processingTime, readVersion, p.first, p.second);
		// std::cout << req.toString() << "\n";
		stats->addWriteRequest(req);

		//std::cout << format("Now: %.2f", g_network->now()) << "\n";
		if (g_network->now() - stats->startTime >= stats->stopAfter) {
			break;
		}
	}
	std::cout << "Writers done.\n";
	return Void();
}

ACTOR Future<Void> reads(std::shared_ptr<SharedState> stats) {
	loop {
		double arrivalDelay = stats->dReadArrival(stats->generator);
		wait(delay(arrivalDelay));

		stats->addReadRequest();

		if (g_network->now() - stats->startTime >= stats->stopAfter) {
			break;
		}
	}
	std::cout << "Reads done.\n";
	return Void();
}

ACTOR Future<Void> actors(std::shared_ptr<SharedState> stats) {
	state Future<Void> error = actorCollection(stats->addActor.getFuture());
	state Future<Void> done = delay(stats->stopAfter);

	loop choose {
		when(wait(error)) { ASSERT(false); }
		when(wait(done)) { return Void(); }
	}
}

int main(int argc, char** argv) {
	double writeArrival = 100, writeDuration = 100;
	double readArrival = 1000;
	for (int i = 1; i < argc; i++) {
		std::string arg(argv[i]);
		if (arg == "--write-arrival") {
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after --write-arrival\n";
				return 1;
			}
			writeArrival = atof(argv[++i]);
		} else if (arg == "--write-duration") {
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after --write-duration\n";
				return 1;
			}
			writeDuration = atof(argv[++i]);
		} else if (arg == "--read-arrival") {
			if (i + 1 >= argc) {
				std::cout << "Expecting an argument after --read-arrival\n";
				return 1;
			}
			readArrival = atof(argv[++i]);
		} else {
			std::cerr << "Unknown argument: " << arg << "\n";
			return 1;
		}
	}

	platformInit();
	//g_network = newNet2(TLSConfig(), false, true);
	startNewSimulator();
	openTraceFile(NetworkAddress(), 1e7, 1e9, ".", "trace", "lccv");

	std::shared_ptr<SharedState> stats = std::make_shared<SharedState>(writeArrival, writeDuration, readArrival);

	// now we start the actors
	std::vector<Future<Void>> all;
	all.push_back(writes(stats));
	all.push_back(reads(stats));
	all.push_back(actors(stats));

	auto f = stopAfter(waitForAll(all));
	g_network->run();

	stats->dump();
	return 0;
}