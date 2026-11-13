# 🚌 Transit Pulse

**Batch + streaming analytics platform for Malaysian public transit.**

Transit Pulse ingests live vehicle positions (GTFS-Realtime) and published
timetables (GTFS static) for Rapid KL, MRT feeder buses and KTMB from
[data.gov.my](https://developer.data.gov.my/realtime-api/gtfs-realtime), and
turns them into answers like:

- Which routes are consistently late, and at what time of day?
- How reliable is each stop?
- When is the network most delayed?

---

## Architecture

```
 GTFS-Realtime API (every 30s)              GTFS Static (daily zip)
          │                                          │
  transit.producer (Python)                 Airflow: gtfs_static_ingest
          │                                          │
   Redpanda / Kafka  ── topic: gtfs.vehicle_positions │
          │                                          │
          ├─► transit.bronze_sink ───────►  Data lake (SeaweedFS / S3, Parquet)
          │                                  bronze/      raw, as received
          └─► Spark Structured Streaming ─►  silver/      cleaned, deduplicated
              parse → validate → dedupe      quarantine/  rows that failed a check
                                             gold/        analytics marts
                                                     │
                                    [phase 3] BigQuery + dbt
                                             │
                                    [phase 5] Dashboard
```

### Data lake layout

| Layer  | Path | Written by |
|--------|------|------------|
| bronze | `bronze/vehicle_positions/feed=*/dt=*/hour=*/part-*.parquet` | `bronze_sink` (micro-batches) |
| bronze | `bronze/gtfs_static/feed=*/dt=*/gtfs.zip` | Airflow |
| silver | `silver/gtfs_static/<table>/feed=*/dt=*/data.parquet` | Airflow |
| silver | `silver/vehicle_positions/feed=*/event_date=*/` | Spark Structured Streaming |
| quarantine | `quarantine/vehicle_positions/processed_date=*/dq_reason=*/` | Spark Structured Streaming |

### Data quality: what silver guarantees

Every vehicle position goes through these checks (`spark/jobs/transforms.py`).
Rows that fail are **not dropped silently**. They go to `quarantine/` with the
reason and the original payload, so they can be counted, alerted on and replayed.

| Rule | Quarantine reason |
|------|-------------------|
| JSON parses and has an `event_id` | `unparseable_or_missing_event_id` |
| Vehicle id, timestamp and GPS position present | `missing_vehicle_id`, `missing_event_ts`, `missing_position` |
| Position inside Malaysia (catches `0,0` "null island" fixes) | `position_outside_malaysia` |
| Speed between 0 and 160 km/h | `negative_speed`, `implausible_speed` |
| Timestamp not more than 5 min in the future | `event_in_future` |
| No duplicates: one row per `event_id` (15-minute watermark) | dropped as duplicate |

**Finding: the speed unit is wrong at the source.** The GTFS-Realtime spec says
`speed` is in metres/second. Comparing reported speed with the speed implied by
consecutive GPS fixes (2,148 pairs of moving buses) gave a median ratio of
**3.30 (Rapid KL)** and **3.45 (MRT feeder)**. That's close to 3.6, the km/h↔m/s
factor, so these feeds send km/h. Silver stores `speed_kmh`, a corrected `speed_mps`,
and the untouched `speed_reported_raw`.

**Finding: KTMB speed is a placeholder.** Every KTMB position reports exactly `90.0`
(160 of 160 rows), and each train stayed at one fixed position for two hours. Read as
m/s, that's 324 km/h and the rows were all quarantined as `implausible_speed`. Instead,
KTMB speed is now stored as null (raw value kept), so positions are judged on their
own. The frozen positions are tracked as a freshness check for Phase 4.

**What the quarantine caught in its first live run** (13,570 events):

| Rows | Reason | Root cause |
|------|--------|------------|
| 67 | `position_outside_malaysia` | GPS "null island" fixes at (0, 0) from KTMB and MRT feeder buses |
| 2 | `event_in_future` | Bus `VAL4552` reported a timestamp in **2029**: a broken device clock |

**Replayable by design.** After the KTMB rule changed, silver and quarantine were rebuilt
from scratch by deleting the output and checkpoints and letting Spark re-read the Kafka topic.

**Finding: broken column at the source.** In the Rapid Rail GTFS `stops.txt`, the
`geometry` column contains the literal text `"[object Object]"` (a JavaScript
serialisation bug upstream). It is kept in silver as-is and excluded in modeling.

### Key design decisions

- **Natural event key.** `event_id = feed:vehicle_id:timestamp`. The API repeats a
  vehicle's last position on every poll until it reports again, so this key is
  what makes deduplication possible.
- **At-least-once delivery.** The Kafka producer is idempotent (`acks=all`), and
  the sink commits offsets only *after* a Parquet file is safely written. A crash
  can cause duplicates but never data loss, and duplicates are removed downstream.
- **Exactly-once silver.** Spark keeps Kafka offsets and dedupe state in a checkpoint,
  and the Parquet sink only exposes committed files (`_spark_metadata`). A restart
  resumes exactly where it stopped, with no gaps or double counting. Locally the
  checkpoint lives on a Docker volume (it needs real directory renames). In the cloud
  it moves to GCS or HDFS via `SPARK_CHECKPOINT_DIR`. Readers must use Spark, or
  respect `_spark_metadata`, so they never see files from a failed micro-batch.
- **Silver is partitioned by the Malaysian local day** (`event_date`), because that's how
  riders and planners think about service. Timestamps themselves stay in UTC.
- **Bronze is partitioned by ingestion time.** It is append-only, so any hour can
  be reprocessed. Event-time partitioning happens in silver.
- **Idempotent batch jobs.** Each Airflow run writes to paths keyed by its logical
  date, so re-runs and backfills overwrite instead of duplicating.
- **Strings in, types later.** Silver GTFS tables keep source values as strings.
  Casting happens in dbt, where it is tested and documented.
- **S3 API everywhere.** Locally the lake is [SeaweedFS](https://github.com/seaweedfs/seaweedfs),
  an open-source S3-compatible store. MinIO stopped publishing free Docker images in 2025.
  Version 4.x is required: 3.x reported folders as files to Spark's S3A connector.
  The code only speaks the S3 API, so switching to AWS S3 just means unsetting `S3_ENDPOINT_URL`.
- **Fail per feed.** Dynamic task mapping runs one task per feed, so a broken KTMB
  feed doesn't block Rapid KL.

---

## Quick start

**Requirements:** Docker Desktop (4 GB+ RAM allocated) and Python 3.11+ for tests.

```bash
cp .env.example .env
docker compose up -d --build
```

| Service | URL | Login |
|---------|-----|-------|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Redpanda Console (Kafka UI) | http://localhost:8085 | none |
| SeaweedFS file browser (data lake) | http://localhost:8888/buckets/transit-lake/ | none |
| Spark UI (Structured Streaming tab) | http://localhost:4040 | none |

Then:

1. Open **Redpanda Console** → Topics → `gtfs.vehicle_positions` to watch live buses arriving.
2. In **Airflow**, unpause `gtfs_static_ingest` and trigger it.
3. In the **SeaweedFS file browser**, open the `transit-lake` bucket. Bronze Parquet files from the
   stream appear every `SINK_FLUSH_SECONDS` (5 min by default).

### Run pieces without Docker

```bash
python -m venv .venv
.venv/Scripts/activate        # Windows  (macOS/Linux: source .venv/bin/activate)
pip install -r requirements-dev.txt

# Peek at live data without Kafka:
cd ingestion && python -m transit.producer --once --dry-run
```

### Tests and linting

```bash
pytest -v
ruff check .

# Spark transform tests run inside the Spark image (no local Java needed)
docker compose run --rm --no-deps spark-streaming python3 -m pytest -v tests
```

CI runs both on every push and PR (`.github/workflows/ci.yml`).

---

## Roadmap

- [x] **Phase 1, Ingestion:** GTFS-RT producer → Kafka → bronze; Airflow GTFS static → bronze/silver
- [x] **Phase 2, Stream processing:** Spark Structured Streaming: validation + quarantine, dedupe on `event_id` with event-time watermark, speed-unit fix, silver vehicle positions
- [ ] **Phase 3, Warehouse and modeling:** BigQuery external tables + dbt (`dim_route` SCD2, `fct_vehicle_positions`, on-time performance marts)
- [ ] **Phase 4, Production hardening:** Terraform (GCS/BigQuery), data quality tests, alerting, pipeline health metrics, freshness checks (e.g. frozen KTMB positions), small-file compaction
- [ ] **Phase 5, Serving:** dashboard (delay heatmap, worst routes, stop reliability map) and hosted dbt docs

## Data source and licence

Data from the Malaysian Government Open Data portal (data.gov.my), provided
under the [Creative Commons Attribution 4.0](https://creativecommons.org/licenses/by/4.0/) licence.
