# ObjectFS Torture Test

The ObjectFS Torture Test (`TestObjectFSTortureE2E`) is a multi-node concurrency and chaos stress test designed to uncover failure modes and race conditions arising from interactions under disruption.

While presubmit gates validate components in isolation (e.g. running single-instance `fsx` or killing a single component once), the torture test exercises the full system under concurrent load and recurring disruptions, enforcing a hard "nothing may hang" bound.

---

## 1. Workload Architecture

The test spins up a 2-worker Kind cluster with a single ObjectFS volume mounted on both nodes via the CSI driver. The controller is deployed with `--debug-mutation-check` and node daemons with `--debug` to enable FUSE request tracing.

Three concurrent workloads run for a configurable duration (default: 10 minutes for nightly runs; 2 minutes for local smoke testing):

1. **Concurrent `fsx × K` per node (default K=4):**
   - Each `fsx` instance runs against its own dedicated file (e.g. `/data/fsx_n1_0.bin`), ensuring single-writer validity per file while stressing concurrent controller commits.
   - Operations use the full POSIX gate profile with mapped writes enabled (`-l 16777216 -o 65536 -p 200 -C -I -0 -B -J -E`).
   - Seeds for all instances are derived from a master run seed and printed in the test log for deterministic replay.

2. **Metadata Churn Generator (Node 1):**
   - Continuously performs `mkdir`, create, `rename`, `unlink`, and `symlink` operations across a nested tree in `/data/churn/`.
   - Synchronizes changes via directory `sync` and records expected tree state strictly after `sync` returns.

3. **Cross-Node Readers (Node 2):**
   - A writer on Node 1 writes checkpointed files with `dd conv=fsync` and records expected SHA256 hashes to a durability manifest.
   - Node 2 reads each checkpointed file back using `dd` and `sha256sum` (via plain `read(2)`, avoiding `splice`/`sendfile` stale EOF per #278) and compares against the manifest.
   - Concurrently verifies directory attributes and file listings across nodes for the metadata churn tree.

---

## 2. Disruption Schedule

During the run, the test executes timed disruptions:
- **Controller Kill:** Force-kills `objectfs-controller-0` every ~90 seconds.
- **WAL Buffer Kill:** Force-kills `wal-buffer-0` every ~120 seconds (offset from controller kills).
- **Controller Outage:** Scales `objectfs-controller` to 0 replicas once per run for 35 seconds (exceeding the 30-second metadata timeout), then scales back up to test the bounded-EIO recovery contract.
- **Mid-Run Snapshot:** Creates an online snapshot (`objectfsctl snapshots create`) midway through the workload.
- **Node Daemon Restart (optional):** Disabled by default pending #132.
- **Watch Stream Kill:** Evaluated; watch streams share the primary gRPC port (50051) with controller RPCs, so stream-only disruption is skipped.

---

## 3. The Five Invariants

A violation of any invariant results in an immediate test failure and triggers a complete diagnostic log dump (controller, WAL buffer, both node daemons, and `fsx` operations):

1. **Invariant 1: Nothing Hangs:**
   Every step (workload iteration, reader verification, disruption recovery) executes under a hard deadline (default: 60 seconds). A timeout fails immediately naming the specific step rather than stalling until the global Go test timeout.
2. **Invariant 2: `fsx` Integrity:**
   Every `fsx` process verifies its own data integrity continuously. Any detected corruption fails the test immediately.
3. **Invariant 3: Durability:**
   Every file written and acknowledged after `fsync` must match its expected SHA256 hash when read on the other node, and must persist byte-for-byte in a fresh mount of a restored snapshot taken at the end of the run.
4. **Invariant 4: Bounded Outage Failure:**
   FUSE operations may fail with `EIO` only while the controller is scaled to 0 (plus the recovery bound from #299). Any `EIO` observed outside this outage window is a hard failure.
5. **Invariant 5: No Silent Divergence:**
   `--debug-mutation-check` on the controller must remain clean. At the conclusion of the test, recursive tree checksums across Node 1, Node 2, and the restored snapshot mount must be byte-identical.

---

## 4. Running the Test

### Local Smoke Run (2 minutes)
```bash
OBJECTFS_TORTURE=1 OBJECTFS_TORTURE_DURATION=2m go test -v ./tests/e2e -run TestObjectFSTortureE2E -timeout 40m
```

### Nightly Periodic Run (10 minutes)
Run via the periodic CI runner:
```bash
./dev/ci/periodics/ap-torture
```

### Bisect / Hang-Rate Measurement Mode
Run a loop with multiple iterations to measure hang frequency:
```bash
OBJECTFS_TORTURE=1 OBJECTFS_TORTURE_DURATION=2m go test -v -count=10 ./tests/e2e -run TestObjectFSTortureE2E -timeout 120m
```

---

## 5. Replaying a Seed

Every run prints the master seed and per-instance seeds:
```text
fsx Node 1 instance 2: seed=428938192847291 file=/data/fsx_n1_2.bin
```

To replay a specific failure locally:
1. Build the standalone `fsx` binary:
   ```bash
   ./dev/tasks/build-fsx
   ```
2. Start a local FUSE mount via `fuse-start`:
   ```bash
   ./dev/tasks/fuse-start -- .build/bin/fsx -N 1000 -S 428938192847291 -l 16777216 -o 65536 -p 200 -C -I -0 -B -J -E replay.bin
   ```

---

## 6. Known Issues

- **#308 / #291 (WAL buffer restart hang):**
  When `wal-buffer-0` is killed, the controller's replication stream may stall during reconnection, causing subsequent `fsync` calls to wait indefinitely. The test maintains active verification of this path and annotates occurrences with `[KNOWN ISSUE #308 / #291]` rather than disabling the check.
