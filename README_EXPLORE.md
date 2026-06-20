# Pipeline Output — Exploration & Visualization

Dokumen ini menjelaskan hasil pipeline yang divisualisasikan melalui `explore.ipynb`.
Notebook dibaca dari folder `data/output/` hasil `pipeline.py`.

---

## Ringkasan Layer

| Layer | Rows | Keterangan |
|-------|------|------------|
| Bronze | 11 | Event bersih setelah validasi & dedup |
| Silver | 11 | Event terenrich dengan data user |
| Gold | 6 | Agregat harian per country |
| Quarantine | 5 | Event yang ditolak karena tidak valid |

Dari 16 raw events di input, **11 lolos** ke Bronze dan **5 ditolak** ke Quarantine (rejection rate ~31%).

---

## Bronze — Clean Events

11 event bersih dengan skema: `event_id`, `user_id`, `event_type`, `value`, `event_ts`, `event_date`.

```
event_id | user_id | event_type | value | event_ts            | event_date
---------|---------|------------|-------|---------------------|------------
e13      | u2      | VIEW       | 1.0   | 2024-12-31 16:50:00 | 2024-12-31  ← late event
e1       | u1      | CLICK      | 3.0   | 2025-01-01 03:00:00 | 2025-01-01
e2       | u2      | VIEW       | 0.0   | 2025-01-01 03:05:00 | 2025-01-01
e3       | u1      | PURCHASE   | 25.0  | 2025-01-01 03:10:00 | 2025-01-01
e5       | u3      | CLICK      | 1.0   | 2025-01-01 04:00:00 | 2025-01-01
e6       | u1      | CLICK      | 2.0   | 2025-01-01 04:05:00 | 2025-01-01
e8       | u2      | PURCHASE   | 30.0  | 2025-01-01 04:30:00 | 2025-01-01
e9       | u1      | VIEW       | 0.0   | 2025-01-01 05:00:00 | 2025-01-01
e10      | u1      | CLICK      | 1.0   | 2025-01-02 02:00:00 | 2025-01-02
e11      | u2      | VIEW       | 2.0   | 2025-01-02 02:10:00 | 2025-01-02
e12      | u1      | PURCHASE   | 20.0  | 2025-01-02 02:30:00 | 2025-01-02
```

**Hal yang diperhatikan:**
- `e13` adalah **late-arriving event** — timestampnya `2024-12-31` tapi datang di file `day_2025-01-02.jsonl`. Pipeline tetap meletakkannya di partisi `event_date=2024-12-31` yang benar.
- `e3` yang muncul duplikat di dua file berhasil dideduplikasi menjadi satu baris.
- `event_type` yang lowercase (misal `click`) sudah dinormalisasi ke `CLICK`.

---

## Quarantine — Rejected Events

5 event ditolak dengan alasan masing-masing:

```
event_id | user_id | event_type | event_ts             | _reject_reason
---------|---------|------------|----------------------|-------------------------
e4       | (null)  | CLICK      | invalid_ts           | null_or_empty_user_id
e7       | u2      | VIEW       | (null)               | invalid_or_null_event_ts
e14      | (empty) | CLICK      | 2025-01-02T10:00:00Z | null_or_empty_user_id
e15      | u1      | (null)     | 2025-01-02T10:05:00Z | invalid_event_type
e16      | u1      | PURCHASE   | 2025-13-01T00:00:00Z | invalid_or_null_event_ts
```

**Breakdown reject reason:**
- `null_or_empty_user_id` — 2 event (e4 user_id null, e14 user_id string kosong)
- `invalid_or_null_event_ts` — 2 event (e7 timestamp null, e16 timestamp dengan bulan 13 tidak valid)
- `invalid_event_type` — 1 event (e15 event_type null)

> `e16` menarik: value dan user_id valid, tapi timestamp `2025-13-01` (bulan 13 tidak ada) menyebabkan parse gagal dan masuk quarantine.

---

## Silver — Enriched Events

11 event terenrich dengan data user reference (LEFT JOIN), skema tambahan: `country`, `signup_date`, `is_purchase`, `days_since_signup`.

```
user_id | event_id | event_type | country | signup_date | is_purchase | days_since_signup
--------|----------|------------|---------|-------------|-------------|------------------
u2      | e13      | VIEW       | US      | 2024-12-15  | False       | 16
u1      | e1       | CLICK      | ID      | 2024-12-01  | False       | 31
u2      | e2       | VIEW       | US      | 2024-12-15  | False       | 17
u1      | e3       | PURCHASE   | ID      | 2024-12-01  | True        | 31
u3      | e5       | CLICK      | SG      | (null)      | False       | NaN   ← signup_date invalid
u1      | e6       | CLICK      | ID      | 2024-12-01  | False       | 31
u2      | e8       | PURCHASE   | US      | 2024-12-15  | True        | 17
u1      | e9       | VIEW       | ID      | 2024-12-01  | False       | 31
u1      | e10      | CLICK      | ID      | 2024-12-01  | False       | 32
u2      | e11      | VIEW       | US      | 2024-12-15  | False       | 18
u1      | e12      | PURCHASE   | ID      | 2024-12-01  | True        | 32
```

**Hal yang diperhatikan:**
- `u3` (event e5) memiliki `signup_date` yang tidak valid di reference data, sehingga `days_since_signup` menjadi `NaN` — pipeline tidak crash, hanya null.
- Semua event berhasil di-JOIN ke user reference. Tidak ada user yang benar-benar tidak ditemukan (yang akan menghasilkan country `UNKNOWN`).
- `days_since_signup` mencerminkan berapa hari sejak user signup sampai event terjadi — u1 sudah 31–32 hari, u2 sudah 17–18 hari.

---

## Gold — Daily × Country Aggregates

6 baris agregat: 3 kombinasi tanggal × country.

```
country | total_events | total_value | total_purchases | unique_users | event_date
--------|--------------|-------------|-----------------|--------------|------------
US      | 1            | 1.0         | 0               | 1            | 2024-12-31
ID      | 4            | 30.0        | 1               | 1            | 2025-01-01
SG      | 1            | 1.0         | 0               | 1            | 2025-01-01
US      | 2            | 30.0        | 1               | 1            | 2025-01-01  ← termasuk e8 PURCHASE
ID      | 2            | 21.0        | 1               | 1            | 2025-01-02
US      | 1            | 2.0         | 0               | 1            | 2025-01-02
```

**Insight dari data:**
- **ID (Indonesia) mendominasi** — 6 total events dari u1 saja, dengan 2 purchases senilai total 45.0.
- **US** punya purchase terbesar single transaction: e8 senilai 30.0 (u2, `2025-01-01`).
- **SG** hanya punya 1 event (u3, CLICK, value 1.0) — dan u3 tidak punya signup_date valid.
- **Late event `e13`** (u2, VIEW, `2024-12-31`) muncul sebagai partisi tersendiri `2024-12-31` di Gold, membuktikan late data handling berjalan benar.
- Semua `unique_users` bernilai 1 per kombinasi — setiap country hanya diwakili satu user unik per hari di dataset ini.

---

## Cara Menjalankan Notebook

```bash
# Pastikan pipeline sudah dirun dulu
uv run python job/pipeline.py --config config/pipeline.yaml

# Buka notebook dan gunakan tombol Run All di feature vs code yang kamu gunakan
