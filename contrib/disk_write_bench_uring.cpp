/*
 * disk_write_bench_uring.cpp
 *
 * Disk write throughput benchmark using Linux io_uring.
 *
 * A single io_uring ring drives all iodepth workers concurrently.  Each
 * worker cycles through: submit write SQE → sync SQE → next write SQE …
 * No threads.  All parallelism comes from having multiple SQEs in flight
 * simultaneously inside the ring.
 *
 * Sync uses IORING_OP_FSYNC with IORING_FSYNC_DATASYNC (fdatasync semantics).
 *
 * Build (standalone):
 *   g++ -O2 -std=c++17 -o disk_write_bench_uring disk_write_bench_uring.cpp -luring
 *
 * Usage:
 *   disk_write_bench_uring [--iodepth N] [--dir PATH] [--duration SECS] [--direct]
 */

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <liburing.h>
#include <random>
#include <string>
#include <unistd.h>
#include <vector>

static constexpr int    DIRECT_IO_ALIGN = 4096;
static constexpr size_t MIN_WRITE       = 100 * 1024;   // 100 KB
static constexpr size_t MAX_WRITE       = 1024 * 1024;  // 1 MB

static inline size_t alignUp(size_t n, size_t align) {
    return (n + align - 1) & ~(align - 1);
}

using Clock     = std::chrono::steady_clock;
using TimePoint = Clock::time_point;

// ---------------------------------------------------------------------------
// Per-worker state
// ---------------------------------------------------------------------------

struct WorkerStats {
    uint64_t total_bytes  = 0;
    uint64_t total_writes = 0;
    uint64_t total_sync_us = 0;
    uint64_t max_sync_us   = 0;
    bool     error         = false;
};

enum Phase : uint8_t { WRITING = 0, SYNCING = 1 };

struct Worker {
    int         id     = 0;
    int         fd     = -1;
    std::string path;
    int64_t     offset = 0;
    Phase       phase  = WRITING;
    TimePoint   sync_start;
    uint8_t*    buf      = nullptr;
    size_t      buf_size = 0;
    size_t      write_size = 0; // size of the current in-flight write
    WorkerStats stats;
    std::mt19937                             rng;
    std::uniform_int_distribution<size_t>   size_dist{ MIN_WRITE, MAX_WRITE };
};

// Pack worker id + phase into the 64-bit user_data field of an SQE/CQE.
static uint64_t make_ud(int id, Phase p) {
    return ((uint64_t)(uint32_t)id << 1) | (uint64_t)p;
}
static void split_ud(uint64_t ud, int* id, Phase* p) {
    *p  = (Phase)(ud & 1);
    *id = (int)(uint32_t)(ud >> 1);
}

// ---------------------------------------------------------------------------
// SQE helpers
// ---------------------------------------------------------------------------

// Returns false if the submission queue is unexpectedly full.
static bool submit_write(struct io_uring* ring, Worker& w, bool direct_io) {
    size_t sz = w.size_dist(w.rng);
    if (direct_io)
        sz = alignUp(sz, DIRECT_IO_ALIGN);
    w.write_size = sz;
    w.phase      = WRITING;

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "worker %d: SQ full on write\n", w.id);
        return false;
    }
    io_uring_prep_write(sqe, w.fd, w.buf, sz, w.offset);
    io_uring_sqe_set_data(sqe, (void*)make_ud(w.id, WRITING));
    return true;
}

static bool submit_sync(struct io_uring* ring, Worker& w) {
    w.sync_start = Clock::now();
    w.phase      = SYNCING;

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        fprintf(stderr, "worker %d: SQ full on sync\n", w.id);
        return false;
    }
    // IORING_FSYNC_DATASYNC → fdatasync semantics (no metadata update).
    io_uring_prep_fsync(sqe, w.fd, IORING_FSYNC_DATASYNC);
    io_uring_sqe_set_data(sqe, (void*)make_ud(w.id, SYNCING));
    return true;
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s [--iodepth N] [--dir PATH] [--duration SECS] [--direct]\n", prog);
    fprintf(stderr, "  --iodepth  N     concurrent files / ring depth (default: 1)\n");
    fprintf(stderr, "  --dir      PATH  directory for temp files (default: /tmp)\n");
    fprintf(stderr, "  --duration SECS  benchmark duration in seconds (default: 60)\n");
    fprintf(stderr, "  --direct         open files with O_DIRECT (4 KB-aligned writes)\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    int         iodepth      = 1;
    std::string dir          = "/tmp";
    int         duration_secs = 60;
    bool        direct_io    = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--iodepth") == 0 && i + 1 < argc) {
            iodepth = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
            dir = argv[++i];
        } else if (strcmp(argv[i], "--duration") == 0 && i + 1 < argc) {
            duration_secs = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--direct") == 0) {
            direct_io = true;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (iodepth < 1)       { fprintf(stderr, "iodepth must be >= 1\n");  return 1; }
    if (duration_secs <= 0){ fprintf(stderr, "duration must be > 0\n");  return 1; }

    printf("io_uring  iodepth: %d  dir: %s  duration: %ds  direct-io: %s\n\n",
           iodepth, dir.c_str(), duration_secs, direct_io ? "yes" : "no");

    // -----------------------------------------------------------------------
    // Initialize io_uring ring.
    // Each worker has at most one op in flight at a time, so a ring of
    // next_pow2(iodepth) entries is always large enough.
    // -----------------------------------------------------------------------
    unsigned ring_depth = 1;
    while (ring_depth < (unsigned)iodepth)
        ring_depth <<= 1;

    struct io_uring ring;
    if (data(ring_depth, &ring, 0) < 0) {
        perror("io_uring_queue_init");
        return 1;
    }

    // -----------------------------------------------------------------------
    // Initialize workers.
    // -----------------------------------------------------------------------
    const size_t buf_size    = direct_io ? alignUp(MAX_WRITE, DIRECT_IO_ALIGN) : MAX_WRITE;
    const int    open_flags  = O_WRONLY | O_CREAT | O_TRUNC | (direct_io ? O_DIRECT : 0);

    std::vector<Worker> workers(iodepth);
    bool init_ok = true;

    for (int i = 0; i < iodepth && init_ok; i++) {
        Worker& w = workers[i];
        w.id       = i;
        w.buf_size = buf_size;
        w.path     = dir + "/disk_write_bench_uring_" + std::to_string(i) + ".bin";
        w.rng.seed(std::random_device{}() ^ (uint32_t)i);

        if (posix_memalign((void**)&w.buf, DIRECT_IO_ALIGN, buf_size) != 0) {
            fprintf(stderr, "worker %d: posix_memalign failed\n", i);
            init_ok = false;
            break;
        }
        // Fill buffer with random bytes.
        std::uniform_int_distribution<int> bd(0, 255);
        for (size_t j = 0; j < buf_size; j++)
            w.buf[j] = (uint8_t)bd(w.rng);

        unlink(w.path.c_str()); // remove stale file from previous run
        w.fd = open(w.path.c_str(), open_flags, 0644);
        if (w.fd < 0) {
            fprintf(stderr, "worker %d: open %s: %s\n", i, w.path.c_str(), strerror(errno));
            init_ok = false;
        }
    }

    if (!init_ok) {
        for (auto& w : workers) {
            if (w.fd  >= 0) close(w.fd);
            if (w.buf)      free(w.buf);
        }
        io_uring_queue_exit(&ring);
        return 1;
    }

    // -----------------------------------------------------------------------
    // Benchmark loop.
    // -----------------------------------------------------------------------
    auto start_time = Clock::now();
    auto deadline   = start_time + std::chrono::seconds(duration_secs);

    // Submit the first write for each worker and flush the SQ in one shot.
    for (auto& w : workers)
        submit_write(&ring, w, direct_io);
    io_uring_submit(&ring);

    int active = iodepth;

    while (active > 0) {
        struct io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) {
            fprintf(stderr, "io_uring_wait_cqe: %s\n", strerror(-ret));
            break;
        }

        int   wid;
        Phase phase;
        split_ud((uint64_t)(uintptr_t)io_uring_cqe_get_data(cqe), &wid, &phase);
        Worker& w = workers[wid];

        if (cqe->res < 0) {
            fprintf(stderr, "worker %d %s: %s\n", wid,
                    phase == WRITING ? "write" : "sync", strerror(-cqe->res));
            w.stats.error = true;
            active--;
        } else if (phase == WRITING) {
            w.stats.total_bytes += (uint64_t)cqe->res;
            w.offset += cqe->res;
            if (!submit_sync(&ring, w)) {
                w.stats.error = true;
                active--;
            } else {
                io_uring_submit(&ring);
            }
        } else { // SYNCING
            uint64_t sync_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                   Clock::now() - w.sync_start).count();
            w.stats.total_sync_us += sync_us;
            w.stats.max_sync_us    = std::max(w.stats.max_sync_us, sync_us);
            w.stats.total_writes++;

            if (Clock::now() < deadline) {
                if (!submit_write(&ring, w, direct_io)) {
                    w.stats.error = true;
                    active--;
                } else {
                    io_uring_submit(&ring);
                }
            } else {
                active--;
            }
        }

        io_uring_cqe_seen(&ring, cqe);
    }

    double elapsed = std::chrono::duration<double>(Clock::now() - start_time).count();

    // -----------------------------------------------------------------------
    // Cleanup.
    // -----------------------------------------------------------------------
    for (auto& w : workers) {
        if (w.fd >= 0) close(w.fd);
        unlink(w.path.c_str());
        free(w.buf);
    }
    io_uring_queue_exit(&ring);

    // -----------------------------------------------------------------------
    // Aggregate and print stats.
    // -----------------------------------------------------------------------
    uint64_t total_bytes = 0, total_writes = 0, total_sync_us = 0, max_sync_us = 0;
    bool had_error = false;
    for (const auto& w : workers) {
        total_bytes   += w.stats.total_bytes;
        total_writes  += w.stats.total_writes;
        total_sync_us += w.stats.total_sync_us;
        max_sync_us    = std::max(max_sync_us, w.stats.max_sync_us);
        had_error     |= w.stats.error;
    }

    if (had_error)
        fprintf(stderr, "warning: one or more workers encountered errors\n\n");

    double total_mb      = total_bytes / (1024.0 * 1024.0);
    double avg_write_kb  = total_writes ? total_bytes   / (1024.0 * total_writes) : 0.0;
    double avg_sync_ms   = total_writes ? total_sync_us / (1000.0 * total_writes) : 0.0;
    double max_sync_ms   = max_sync_us  / 1000.0;

    printf("Duration:             %.1f s\n",  elapsed);
    printf("Workers (iodepth):    %d\n",      iodepth);
    printf("Total writes:         %llu\n",    (unsigned long long)total_writes);
    printf("Total data:           %.2f MB\n", total_mb);
    printf("Throughput:           %.2f MB/s\n", total_mb / elapsed);
    printf("Avg write size:       %.2f KB\n", avg_write_kb);
    printf("Avg sync latency:     %.3f ms\n", avg_sync_ms);
    printf("Max sync latency:     %.3f ms\n", max_sync_ms);
    printf("Write IOPS:           %.1f\n",    total_writes / elapsed);

    return 0;
}
