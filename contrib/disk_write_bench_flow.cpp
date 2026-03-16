/*
 * disk_write_bench_flow.cpp
 *
 * Disk write throughput benchmark using the Flow async I/O library.
 *
 * Each writer is a Flow coroutine that sequentially writes a randomly-sized
 * buffer and syncs (F_FULLFSYNC on macOS, fdatasync on Linux) to a dedicated
 * file. Multiple coroutines run concurrently, controlled by --iodepth.
 * Sync calls are dispatched to a thread pool, so all iodepth syncs are
 * genuinely in-flight simultaneously.
 *
 * Build (from the FDB build directory):
 *   cmake target: disk_write_bench_flow
 *   link libraries: flow fdbrpc fdbclient
 *
 * Usage:
 *   disk_write_bench_flow [--iodepth N] [--dir PATH] [--duration SECS]
 */

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "fdbrpc/Net2FileSystem.h"
#include "flow/IAsyncFile.h"
#include "flow/Platform.h"
#include "flow/TLSConfig.actor.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "flow/actorcompiler.h" // must be last include

// ---------------------------------------------------------------------------
// Benchmark parameters
// ---------------------------------------------------------------------------

// On Linux with --direct, writes go through AsyncFileKAIO (kernel AIO via
// io_submit/eventfd), bypassing the EIO thread pool entirely.  O_DIRECT
// requires all sizes and offsets to be multiples of DIRECT_IO_ALIGN.
static constexpr int DIRECT_IO_ALIGN = 4096;
static constexpr int MIN_WRITE_BYTES = 100 * 1024;  // 100 KB
static constexpr int MAX_WRITE_BYTES = 1024 * 1024; // 1 MB

// Round n up to the nearest multiple of align (align must be a power of 2).
static inline int alignUp(int n, int align) {
	return (n + align - 1) & ~(align - 1);
}

// ---------------------------------------------------------------------------
// Per-worker stats (single-threaded Flow, no locking needed)
// ---------------------------------------------------------------------------

struct WorkerStats {
	int64_t totalBytes = 0;
	int64_t totalWrites = 0;
	double totalSyncSecs = 0.0;
	double maxSyncSecs = 0.0;
	bool hadError = false;
};

// ---------------------------------------------------------------------------
// Writer coroutine
// ---------------------------------------------------------------------------

Future<Void> writerActor(int id, std::string dir, double deadline, bool directIO, WorkerStats* stats) {
	std::string path = dir + "/disk_write_bench_" + std::to_string(id) + ".bin";

	try {
		// Remove any leftover file from a previous run.
		try {
			co_await IAsyncFileSystem::filesystem()->deleteFile(path, /*mustBeDurable=*/false);
		} catch (Error&) {
			// Ignore – file may not exist yet.
		}

		// OPEN_UNCACHED | OPEN_UNBUFFERED on Linux → AsyncFileKAIO (kernel AIO,
		// no EIO thread pool for writes).  Requires 4 KB-aligned buf/offset/size.
		// On macOS these flags are accepted but still route through EIO.
		int64_t openFlags = IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE;
		if (directIO)
			openFlags |= IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_UNBUFFERED;

		Reference<IAsyncFile> file = co_await IAsyncFileSystem::filesystem()->open(path, openFlags, 0644);

		// Allocate a page-aligned buffer (required for O_DIRECT).
		const int bufSize = directIO ? alignUp(MAX_WRITE_BYTES, DIRECT_IO_ALIGN) : MAX_WRITE_BYTES;
		void* rawBuf = nullptr;
		if (posix_memalign(&rawBuf, DIRECT_IO_ALIGN, bufSize) != 0)
			throw std::bad_alloc();
		std::unique_ptr<void, decltype(&free)> bufGuard(rawBuf, free);
		uint8_t* buf = static_cast<uint8_t*>(rawBuf);
		deterministicRandom()->randomBytes(buf, bufSize);

		int64_t offset = 0;

		while (now() < deadline) {
			int writeSize = deterministicRandom()->randomInt(MIN_WRITE_BYTES, MAX_WRITE_BYTES + 1);
			// O_DIRECT requires size and offset to be multiples of the block size.
			if (directIO)
				writeSize = alignUp(writeSize, DIRECT_IO_ALIGN);

			co_await file->write(buf, writeSize, offset);
			offset += writeSize;

			double syncStart = now();
			// sync() on Linux/kaio still goes through EIO (kernel AIO's IOCB_CMD_FDSYNC
			// is not reliably supported), so this remains the bottleneck at high iodepth.
			// On macOS: F_FULLFSYNC via EIO thread pool regardless of directIO.
			co_await file->sync();
			double syncElapsed = now() - syncStart;

			stats->totalSyncSecs += syncElapsed;
			stats->maxSyncSecs = std::max(stats->maxSyncSecs, syncElapsed);
			stats->totalBytes += writeSize;
			stats->totalWrites++;
		}

		file.clear();
		co_await IAsyncFileSystem::filesystem()->deleteFile(path, /*mustBeDurable=*/false);

	} catch (Error& e) {
		fprintf(stderr, "worker %d error: %s\n", id, e.what());
		stats->hadError = true;
	}

	co_return;
}

// ---------------------------------------------------------------------------
// Top-level benchmark coroutine
// ---------------------------------------------------------------------------

Future<Void> runBench(std::string dir, int iodepth, double durationSecs, bool directIO) {
	printf("iodepth: %d  dir: %s  duration: %.0f s  direct-io: %s\n\n",
	       iodepth, dir.c_str(), durationSecs, directIO ? "yes (kaio on Linux)" : "no (EIO)");

	double startTime = now();
	double deadline = startTime + durationSecs;

	std::vector<WorkerStats> stats(iodepth);
	std::vector<Future<Void>> workers;
	workers.reserve(iodepth);

	for (int i = 0; i < iodepth; i++) {
		workers.push_back(writerActor(i, dir, deadline, directIO, &stats[i]));
	}

	co_await waitForAll(workers);

	double elapsed = now() - startTime;

	// Aggregate across workers.
	int64_t totalBytes = 0;
	int64_t totalWrites = 0;
	double totalSyncSecs = 0.0;
	double maxSyncSecs = 0.0;
	bool hadError = false;

	for (const auto& s : stats) {
		totalBytes += s.totalBytes;
		totalWrites += s.totalWrites;
		totalSyncSecs += s.totalSyncSecs;
		maxSyncSecs = std::max(maxSyncSecs, s.maxSyncSecs);
		hadError |= s.hadError;
	}

	if (hadError) {
		fprintf(stderr, "warning: one or more workers encountered errors\n\n");
	}

	double totalMB = totalBytes / (1024.0 * 1024.0);
	double avgWriteKB = totalWrites ? totalBytes / (1024.0 * totalWrites) : 0.0;
	// Per-writer average: each writer's syncs are serialised from its own view.
	double avgSyncMs = totalWrites ? totalSyncSecs * 1000.0 / totalWrites : 0.0;
	double maxSyncMs = maxSyncSecs * 1000.0;

	printf("Duration:             %.1f s\n", elapsed);
	printf("Workers (iodepth):    %d\n", iodepth);
	printf("Total writes:         %lld\n", (long long)totalWrites);
	printf("Total data:           %.2f MB\n", totalMB);
	printf("Throughput:           %.2f MB/s\n", totalMB / elapsed);
	printf("Avg write size:       %.2f KB\n", avgWriteKB);
	printf("Avg sync latency:     %.3f ms\n", avgSyncMs);
	printf("Max sync latency:     %.3f ms\n", maxSyncMs);
	printf("Write IOPS:           %.1f\n", (double)totalWrites / elapsed);

	co_return;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

static void usage(const char* prog) {
	fprintf(stderr, "Usage: %s [--iodepth N] [--dir PATH] [--duration SECS] [--direct]\n", prog);
	fprintf(stderr, "  --iodepth  N     concurrent writer coroutines (default: 1)\n");
	fprintf(stderr, "  --dir      PATH  directory for temp files     (default: /tmp)\n");
	fprintf(stderr, "  --duration SECS  benchmark duration in seconds (default: 60)\n");
	fprintf(stderr, "  --direct         use O_DIRECT + kernel AIO on Linux (kaio path,\n");
	fprintf(stderr, "                   no EIO thread pool for writes; sizes rounded up\n");
	fprintf(stderr, "                   to 4 KB alignment; no effect on macOS)\n");
}

extern "C" void eio_set_max_parallel(unsigned int nthreads); // from libeio

int main(int argc, char* argv[]) {
	int iodepth = 1;
	std::string dir = "/tmp";
	double durationSecs = 60.0;
	bool directIO = false;

	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--iodepth") == 0 && i + 1 < argc) {
			iodepth = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
			dir = argv[++i];
		} else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
			durationSecs = atof(argv[++i]);
		} else if (strcmp(argv[i], "--direct") == 0) {
			directIO = true;
		} else {
			usage(argv[0]);
			return 1;
		}
	}

	if (iodepth < 1) {
		fprintf(stderr, "iodepth must be >= 1\n");
		return 1;
	}
	if (durationSecs <= 0) {
		fprintf(stderr, "duration must be > 0\n");
		return 1;
	}

	platformInit();
	g_network = newNet2(TLSConfig(), /*useThreadPool=*/false, /*useMetrics=*/true);
	Net2FileSystem::newFileSystem(/*cacheSize=*/-1, /*folder=*/"");
	// EIO_MAX_PARALLELISM defaults to 4. With iodepth > 4, syncs queue up behind
	// the thread pool and serialise. Raise it to match iodepth so all workers
	// can have a sync in-flight at the same time.
	// eio_set_max_parallel() is safe to call after init and takes effect immediately.
	eio_set_max_parallel(static_cast<unsigned int>(std::max(iodepth, 4)));

	auto f = stopAfter(runBench(dir, iodepth, durationSecs, directIO));
	g_network->run();

	return 0;
}
