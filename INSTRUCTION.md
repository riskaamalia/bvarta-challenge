# Data Engineer – Take-Home Exercise (PySpark)

## Context

You’re building a batch data pipeline for a data platform team.

The pipeline ingests raw user event data, cleans and validates it, enriches it with reference data, and produces analytics-ready aggregates.
The data is intentionally messy: duplicates, nulls, invalid, and late-arriving records.

This exercise is designed to reflect real production concerns, not toy ETL.

**Expected time:** -

**References:** 
- [Spark](https://spark.apache.org/docs/latest/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)


---

## Tasks

### Prerequisite

* Setup python project based on given file
* Use python 3.12 or 3.13
* Feel free to containerized the project (optional)


---


### 1. Ingestion & Schema Enforcement

* Read raw event data using **explicit schemas**
* Track rejected records separately

---

### 2. Data Quality & Cleaning (Bronze Layer)

Apply defensive data engineering:  

Apply below steps if possible.

* Drop invalid records
* Deduplicate
* Normalize values if needed
* Fill missing values where appropriate

**Deliverable:** clean events + quarantined/rejected events.

---

### 3. Enrichment (Silver Layer)

* Join events with user reference data
* Handle missing dimension records gracefully
* Add derived fields:

  * `event_date`
  * `is_purchase`
  * `days_since_signup`

---

### 4. Aggregations (Gold Layer)

Produce daily, country-level metrics:

| event_date | country | total_events | total_value | total_purchases | unique_users |

---

### 5. Incremental & Late Data Handling

* Pipeline must be **idempotent**
* Reprocessing the same data should not duplicate results
* Late events must correctly update previous aggregates

Assumptions you may make:

* Partitioning by `event_date`
* Overwrite or merge logic is acceptable (explain your choice)

---

### 6. Output & Structure

* Write outputs in **Parquet**
* Partition where it makes sense
* Organize outputs into:

  * `bronze/`
  * `silver/`
  * `gold/`


## Expectations

We’re looking for:

* Correct Spark usage
* Clear Bronze / Silver / Gold separation
* Sensible handling of bad and late data
* Clean, readable code

Optional:

* Tests will be great
* Improvement and creativity.


If something is unclear, make a reasonable assumption and document it.

---

## Submission

Include a short README explaining:

* Pipeline design
* Data quality rules
* Incremental / late data strategy
* Other initiatives
* Write down **clear instruction** to run the pipeline or any other commands available in the project (like tests if any)


Submit a GitHub repo or zip file, good luck!