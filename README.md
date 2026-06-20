# (Riska Amalia) Data Engineer Take-Home Bvarta

## Pipeline Design

The pipeline follows the **Medallion Architecture** (Bronze → Silver → Gold) using PySpark batch processing.

```
data/raw/events/*.jsonl          data/reference/users.csv
        │                                  │
        ▼                                  │
┌──────────────────┐                       │
│   INGESTION      │  Explicit schemas     │
│   (Step 1)       │                       │
└──────────────────┘                       │
        │                                  │
        ▼                                  │
┌──────────────────┐                       │
│   BRONZE         │  Clean events         │
│   (Step 2)       │  Quarantine bad rows  │
└──────────────────┘                       │
        │                                  │
        ▼                                  ▼
┌──────────────────────────────────────────┐
│   SILVER (Step 3)                        │
│   Left-join events & users               │
│   + event_date, is_purchase,             │
│   + days_since_signup                    │
└──────────────────────────────────────────┘
        │
        ▼
┌──────────────────┐
│   GOLD (Step 4)  │  event_date × country
│   Aggregation    │  metrics table
└──────────────────┘
```

**Output layout:**
```
output/
├── bronze/      event_date=.../   clean events (Parquet, partitioned by event_date)
├── silver/      event_date=.../   enriched events (Parquet, partitioned by event_date)
├── gold/        event_date=.../   daily aggregates (Parquet, partitioned by event_date)
└── quarantine/                    rejected rows with reject reason + timestamp
```

---

## Data Quality Rules (Bronze)

| Rule | Action |
|------|--------|
| `null event_id` | → quarantine (`null_event_id`) |
| `null or empty user_id` | → quarantine (`null_or_empty_user_id`) |
| `null or unrecognised event_type` | → quarantine (`invalid_event_type`) |
| `event_type` casing (e.g. `click`) | Normalise to `UPPER` before validation |
| `null or unparseable event_ts` | → quarantine (`invalid_or_null_event_ts`) |
| `value` as string (e.g. `"30"`) | Cast to `Double`; `null` → `0.0` |
| Duplicate `(event_id, event_ts)` | Drop duplicate, keep last occurrence |

User reference cleaning:
- `null` or empty `country` → `"UNKNOWN"`
- Invalid `signup_date` → `null` signup_date; `days_since_signup` will be `null`
- Duplicate `user_id` in reference → deduplicated (keep last)

---

## Incremental & Late Data Strategy

### Idempotency
The pipeline is idempotent by design:

1. **Bronze dedup**: `dropDuplicates(["event_id", "event_ts"])` ensures the same raw event processed multiple times produces exactly one clean record.
2. **Dynamic partition overwrite**: `spark.sql.sources.partitionOverwriteMode = dynamic` means each write only overwrites the `event_date` partitions present in the current batch — all other partitions remain untouched.

### Late-arriving events
Late events (e.g. `e13` timestamped `2024-12-31` arriving in the `day_2025-01-02` file) are handled naturally:

- They land in Bronze/Silver partitioned under their **actual** `event_date` (not the file date).
- When Gold is computed, those partitions are overwritten with the updated aggregate.
- Re-running the full pipeline re-computes all affected partitions idempotently.

**Chosen merge strategy: dynamic partition overwrite (not MERGE/upsert)**

*Why*: The input data is batch files by calendar day. For each run we have the complete picture for every `event_date` that appears in the batch, so a full partition overwrite is safe, simpler (no delta tables needed), and achieves the same idempotency guarantee as a row-level MERGE.

---

## Other Notes / Assumptions

- `value` can be a JSON string (`"30"`) due to upstream schema drift — cast to `Double` in Bronze.
- Events from users not present in the reference CSV are kept (LEFT JOIN); their `country` defaults to `"UNKNOWN"` and `days_since_signup` is `null`.
- `is_purchase` is derived from `event_type == "PURCHASE"` (boolean column for easy aggregation).
- The pipeline reads **all files** under `data/raw/events/` each run. For production scale, add file-level watermarking or a manifest table.

---

## How to Run

### Prerequisites
- Python 3.12 or 3.13
- [uv](https://docs.astral.sh/uv/) (recommended) **or** pip

### Setup

```bash
# With uv (recommended)
uv sync

# Or with pip
pip install pyspark pyyaml
```

### Run the pipeline

```bash
# From the project root (distribute/)
uv run python job/pipeline.py --config config/pipeline.yaml

# Or directly
python job/pipeline.py --config config/pipeline.yaml
```

### Run tests

```bash
uv run pytest job/test_pipeline.py -v

# With coverage (optional)
uv run pytest job/test_pipeline.py -v --tb=short
```

---

## Project Structure

```
bvarta-challenge/
├── config/
│   └── pipeline.yaml          # Path configuration
├── data/
│   ├── raw/events/            # Input JSONL files (one per day)
│   └── reference/users.csv    # User dimension table
|   └── output/                    # Generated at runtime
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── quarantine/
├── job/
│   ├── pipeline.py            # Main pipeline (Bronze → Silver → Gold)
│   └── test_pipeline.py       # Unit tests (pytest + PySpark local)
├── pyproject.toml
├── README.md
└── INSTRUCTION.md
```
