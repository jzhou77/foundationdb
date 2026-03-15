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

static constexpr size_t MIN_WRITE = 100 * 1024;   // 100 KB
static constexpr size_t MAX_WRITE = 1024 * 1024;  // 1 MB
static constexpr int DURATION_SECS = 60;

using Clock = std::chrono::steady_clock;

struct WorkerStats {
    uint64_t total_bytes = 0;
    uint64_t total_writes = 0;
    uint64_t total_sync_us = 0;
    bool error = false;
};

static void worker(int id, const std::string& dir, Clock::time_point deadline, WorkerStats& stats) {
    std::string path = dir + "/disk_write_bench_" + std::to_string(id) + ".bin";

    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        perror("open");
        stats.error = true;
        return;
    }

    std::vector<char> buf(MAX_WRITE);
    std::mt19937 rng(std::random_device{}() ^ static_cast<uint32_t>(id));
    std::uniform_int_distribution<size_t> size_dist(MIN_WRITE, MAX_WRITE);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    for (auto& b : buf) b = static_cast<char>(byte_dist(rng));

    while (Clock::now() < deadline) {
        size_t write_size = size_dist(rng);

        ssize_t written = write(fd, buf.data(), write_size);
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
        stats.total_sync_us +=
            std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - sync_start).count();

        stats.total_bytes += static_cast<size_t>(written);
        stats.total_writes++;
    }

    close(fd);
    unlink(path.c_str());
}

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s [--iodepth N] [--dir PATH]\n", prog);
    fprintf(stderr, "  --iodepth N   number of concurrent writers (default: 1)\n");
    fprintf(stderr, "  --dir PATH    directory for temp files (default: /tmp)\n");
}

int main(int argc, char* argv[]) {
    int iodepth = 1;
    std::string dir = "/tmp";

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--iodepth") == 0 && i + 1 < argc) {
            iodepth = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--dir") == 0 && i + 1 < argc) {
            dir = argv[++i];
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (iodepth < 1) {
        fprintf(stderr, "iodepth must be >= 1\n");
        return 1;
    }

    printf("iodepth: %d  dir: %s  duration: %ds\n\n", iodepth, dir.c_str(), DURATION_SECS);

    auto deadline = Clock::now() + std::chrono::seconds(DURATION_SECS);

    std::vector<WorkerStats> stats(iodepth);
    std::vector<std::thread> threads;
    threads.reserve(iodepth);

    for (int i = 0; i < iodepth; i++) {
        threads.emplace_back(worker, i, std::ref(dir), deadline, std::ref(stats[i]));
    }
    for (auto& t : threads) t.join();

    // Aggregate across workers.
    WorkerStats agg;
    for (const auto& s : stats) {
        agg.total_bytes += s.total_bytes;
        agg.total_writes += s.total_writes;
        agg.total_sync_us += s.total_sync_us;
        agg.error |= s.error;
    }

    if (agg.error) {
        fprintf(stderr, "One or more workers encountered errors.\n");
    }

    double total_mb = agg.total_bytes / (1024.0 * 1024.0);
    double avg_write_kb = agg.total_writes ? agg.total_bytes / (1024.0 * agg.total_writes) : 0;
    // Per-writer avg sync latency (each writer's syncs are serial from its own perspective).
    double avg_sync_ms = agg.total_writes ? agg.total_sync_us / (1000.0 * agg.total_writes) : 0;

    printf("Duration:             %d s\n", DURATION_SECS);
    printf("Workers (iodepth):    %d\n", iodepth);
    printf("Total writes:         %llu\n", agg.total_writes);
    printf("Total data:           %.2f MB\n", total_mb);
    printf("Throughput:           %.2f MB/s\n", total_mb / DURATION_SECS);
    printf("Avg write size:       %.2f KB\n", avg_write_kb);
    printf("Avg sync latency:     %.3f ms\n", avg_sync_ms);
    printf("Write IOPS:           %.1f\n", static_cast<double>(agg.total_writes) / DURATION_SECS);

    return 0;
}
