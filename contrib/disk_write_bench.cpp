#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

static constexpr int DIRECT_IO_ALIGN = 4096;

static inline size_t alignUp(size_t n, size_t align) {
    return (n + align - 1) & ~(align - 1);
}

static constexpr size_t MIN_WRITE = 100 * 1024;   // 100 KB
static constexpr size_t MAX_WRITE = 1024 * 1024;  // 1 MB

using Clock = std::chrono::steady_clock;

struct WorkerStats {
    uint64_t total_bytes = 0;
    uint64_t total_writes = 0;
    uint64_t total_sync_us = 0;
    uint64_t max_sync_us = 0;
    bool error = false;
};

static void worker(int id, const std::string& dir, Clock::time_point deadline, bool direct_io, WorkerStats& stats) {
    std::string path = dir + "/disk_write_bench_" + std::to_string(id) + ".bin";

    int open_flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifndef __APPLE__
    if (direct_io)
        open_flags |= O_DIRECT;
#endif

    int fd = open(path.c_str(), open_flags, 0644);
    if (fd < 0) {
        perror("open");
        stats.error = true;
        return;
    }

#ifdef __APPLE__
    if (direct_io && fcntl(fd, F_NOCACHE, 1) < 0) {
        perror("F_NOCACHE");
        stats.error = true;
        close(fd);
        return;
    }
#endif

    const size_t buf_size = direct_io ? alignUp(MAX_WRITE, DIRECT_IO_ALIGN) : MAX_WRITE;
    void* raw_buf = nullptr;
    if (direct_io) {
        if (posix_memalign(&raw_buf, DIRECT_IO_ALIGN, buf_size) != 0) {
            fprintf(stderr, "posix_memalign failed\n");
            stats.error = true;
            close(fd);
            return;
        }
    } else {
        raw_buf = malloc(buf_size);
        if (!raw_buf) {
            fprintf(stderr, "malloc failed\n");
            stats.error = true;
            close(fd);
            return;
        }
    }

    std::mt19937 rng(std::random_device{}() ^ static_cast<uint32_t>(id));
    std::uniform_int_distribution<size_t> size_dist(MIN_WRITE, MAX_WRITE);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    char* buf = static_cast<char*>(raw_buf);
    for (size_t i = 0; i < buf_size; i++) buf[i] = static_cast<char>(byte_dist(rng));

    while (Clock::now() < deadline) {
        size_t write_size = size_dist(rng);
        if (direct_io)
            write_size = alignUp(write_size, DIRECT_IO_ALIGN);

        ssize_t written = write(fd, buf, write_size);
        if (written < 0) {
            perror("write");
            stats.error = true;
            break;
        }

        auto sync_start = Clock::now();
#ifdef __APPLE__
        if (fcntl(fd, F_FULLFSYNC) < 0) {
            perror("F_FULLFSYNC");
#else
        if (fdatasync(fd) < 0) {
            perror("fdatasync");
#endif
            stats.error = true;
            break;
        }
        uint64_t sync_us =
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sync_start).count();
        stats.total_sync_us += sync_us;
        stats.max_sync_us = std::max(stats.max_sync_us, sync_us);

        stats.total_bytes += static_cast<size_t>(written);
        stats.total_writes++;
    }

    free(raw_buf);
    close(fd);
    unlink(path.c_str());
}

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s [--iodepth N] [--dir PATH] [--duration SECS] [--direct]\n", prog);
    fprintf(stderr, "  --iodepth  N     number of concurrent writers (default: 1)\n");
    fprintf(stderr, "  --dir      PATH  directory for temp files (default: /tmp)\n");
    fprintf(stderr, "  --duration SECS  benchmark duration in seconds (default: 60)\n");
    fprintf(stderr, "  --direct         use O_DIRECT on Linux / F_NOCACHE on macOS;\n");
    fprintf(stderr, "                   write sizes rounded up to %d-byte alignment\n", DIRECT_IO_ALIGN);
}

int main(int argc, char* argv[]) {
    int iodepth = 1;
    std::string dir = "/tmp";
    int duration_secs = 60;
    bool direct_io = false;

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

    if (iodepth < 1) {
        fprintf(stderr, "iodepth must be >= 1\n");
        return 1;
    }
    if (duration_secs <= 0) {
        fprintf(stderr, "duration must be > 0\n");
        return 1;
    }

    printf("iodepth: %d  dir: %s  duration: %ds  direct-io: %s\n\n",
           iodepth, dir.c_str(), duration_secs, direct_io ? "yes" : "no");

    auto deadline = Clock::now() + std::chrono::seconds(duration_secs);

    std::vector<WorkerStats> stats(iodepth);
    std::vector<std::thread> threads;
    threads.reserve(iodepth);

    for (int i = 0; i < iodepth; i++) {
        threads.emplace_back(worker, i, std::ref(dir), deadline, direct_io, std::ref(stats[i]));
    }
    for (auto& t : threads) t.join();

    // Aggregate across workers.
    WorkerStats agg;
    for (const auto& s : stats) {
        agg.total_bytes += s.total_bytes;
        agg.total_writes += s.total_writes;
        agg.total_sync_us += s.total_sync_us;
        agg.max_sync_us = std::max(agg.max_sync_us, s.max_sync_us);
        agg.error |= s.error;
    }

    if (agg.error) {
        fprintf(stderr, "One or more workers encountered errors.\n");
    }

    double total_mb = agg.total_bytes / (1024.0 * 1024.0);
    double avg_write_kb = agg.total_writes ? agg.total_bytes / (1024.0 * agg.total_writes) : 0;
    // Per-writer avg sync latency (each writer's syncs are serial from its own perspective).
    double avg_sync_ms = agg.total_writes ? agg.total_sync_us / (1000.0 * agg.total_writes) : 0;
    double max_sync_ms = agg.max_sync_us / 1000.0;

    printf("Duration:             %d s\n", duration_secs);
    printf("Workers (iodepth):    %d\n", iodepth);
    printf("Total writes:         %llu\n", (unsigned long long)agg.total_writes);
    printf("Total data:           %.2f MB\n", total_mb);
    printf("Throughput:           %.2f MB/s\n", total_mb / duration_secs);
    printf("Avg write size:       %.2f KB\n", avg_write_kb);
    printf("Avg sync latency:     %.3f ms\n", avg_sync_ms);
    printf("Max sync latency:     %.3f ms\n", max_sync_ms);
    printf("Write IOPS:           %.1f\n", static_cast<double>(agg.total_writes) / duration_secs);

    return 0;
}
