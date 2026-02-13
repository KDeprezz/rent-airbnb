# rent-airbnb

Data engineering assessment: compare Amsterdam postcodes for Airbnb vs long-term rental profitability using **Spark Declarative Pipelines** (formerly DLT) and a **Medallion architecture** (Bronze → Silver → Gold).

This repository was bootstrapped from the **RevoData Asset Bundle Templates** (`0.20.0`) and then adapted for this assessment.

---

## Assessment Summary

### Goal
Identify postal codes with investment potential and determine whether **long-term renting (Kamernet)** or **short-term rental (Airbnb)** is more profitable at **PC4 (first 4 digits)** level.

### What this project delivers
**Delta tables (UC)** following Medallion architecture:

- **Bronze (raw ingest)**
  - `bronze_airbnb` (from `airbnb.csv`)
  - `bronze_rentals` (from `rentals.json`)
  - `bronze_postcodes_geojson` (from `geo/post_codes.geojson`)

- **Silver (clean + standardized)**
  - `silver_airbnb_clean` (parsed PC4, stable `property_id`, price parsing)
  - `silver_postcodes_centroids` (official centroid per PC4 from GeoJSON `properties.geo_point_2d`)
  - `silver_airbnb_backfilled` (PC4 backfilled for missing Airbnb zipcodes)
  - `silver_rentals_clean` (rent parsing, PC4 normalization, basic cleaning)

- **Gold (business metrics)**
  - `gold_property_revenue` (gross annual revenue per property, by source)
  - `gold_airbnb_postcode` (Airbnb metrics aggregated per PC4)
  - `gold_rentals_postcode` (Rentals metrics aggregated per PC4)
  - `gold_postcode_comparison` (side-by-side comparison + recommendation)
  - `gold_airbnb_backfill_stats` (quality statistics for postcode backfill)

**Artifacts**
- **Parquet exports** (for hand-in / portability): `/Volumes/workspace/rent_airbnb/invest_output/<table_name>`
- **Dashboard**: “Revenue Insights Amsterdam” (screenshots in `docs/dashboard/`, optional JSON export if supported)

---

## Key assumptions (why, not how)

This is a **gross revenue comparison**, meant to rank areas for potential, not to build a full investment model.

- **Airbnb gross annual revenue** is estimated from nightly price:
  - `airbnb_annual_rev_eur = price_eur_night * 365 * occupancy_rate`
  - (Occupancy is a configurable constant in the pipeline; see `docs/assumptions.md`.)
- **Long-term rental gross annual revenue**:
  - `rentals_annual_rev_eur = rent_month_eur * 12`
- **Costs not included**: platform fees, taxes, mortgage/interest, vacancy, maintenance, furnishing, cleaning.
- **Comparison grain**: PC4 postal codes (e.g., `1016`), because Airbnb zipcodes contain noise and Kamernet is not limited to Amsterdam only.
- **Missing Airbnb zipcodes** are backfilled using nearest PC4 centroid from official GeoJSON (see `docs/backfill_strategy.md`), and provenance is stored in `pc4_source`.

---

## Repository structure

```text
.
├── notebooks/
│   ├── pipelines/
│   │   ├── transformations/        # Declarative pipeline datasets (Bronze/Silver/Gold)
│   │   └── utilities/              # Pipeline helpers/config
│   └── 90_export_parquet.py        # Parquet export script (optional)
├── scratch/
│   ├── validate_airbnb.ipynb
│   ├── validate_rentals.ipynb
│   ├── geo_backfill_prototype.ipynb
│   └── gold_sanity_check.ipynb
├── src/
│   └── rent_airbnb/
│       ├── __init__.py
│       ├── parsing.py
│       └── revenue.py
├── tests/
│   ├── test_parsing.py
│   └── test_revenue.py
├── dashboards/
│   └── revenue_insights_amsterdam.lvdash.json
├── docs/
│   ├── images/
│   ├── README.md
│   ├── architecture.md
│   ├── assumptions.md
│   └── backfill_strategy.md
├── resources/
├── data/
│   └── output/
├── databricks.yml
├── pyproject.toml
└── README.md


---

## Run in Databricks Free Edition (recommended)

### 0) Unity Catalog setup (tables & volumes)
This project assumes a UC catalog + schema like:

- **Catalog**: `workspace`
- **Schema**: `rent_airbnb`

Tables are materialized under: `workspace.rent_airbnb.*`

### 1) Upload input data to a UC Volume
Create (or use) a UC Volume and upload the raw input files to:

- `/Volumes/workspace/rent_airbnb/invest_input/airbnb.csv`
- `/Volumes/workspace/rent_airbnb/invest_input/rentals.json`
- `/Volumes/workspace/rent_airbnb/invest_input/geo/post_codes.geojson`

> Tip: In Databricks UI → Catalog Explorer → Volumes → upload files into the correct folders.

### 2) Run the Declarative Pipeline
Open the pipeline (e.g., `rent-airbnb-dev`) and run:

- **Run pipeline → Refresh all** (first run or after logic changes)
- **Run pipeline** (incremental refresh)

This will create/update all Bronze/Silver/Gold tables listed above.

### 3) Validate (exploration notebooks)
Run the notebooks in `scratch/`:

- `scratch/00_validate_airbnb.ipynb` — nulls / zipcode noise / price distribution
- `scratch/01_validate_rentals.ipynb` — rent parsing quality / coverage
- `scratch/02_geo_backfill_prototype.ipynb` — centroid NN backfill prototype + distance sanity
- `scratch/03_gold_sanity_check.ipynb` — sanity checks on gold metrics

### 4) Export Parquet (for submission)
The export script writes Parquet to a Volume path (portable hand-in).  
Run:

- `notebooks/90_export_parquet.py`

Configuration is provided via environment variables (safe for Spark Connect / Serverless):
- `OUTPUT_ROOT` (default `/Volumes/workspace/rent_airbnb/invest_output`)
- `TARGET_SCHEMA` (default `workspace.rent_airbnb`)

Example output:
- `/Volumes/workspace/rent_airbnb/invest_output/gold_postcode_comparison/`
- `/Volumes/workspace/rent_airbnb/invest_output/gold_airbnb_postcode/`
- etc.

---

## Dashboard

A Databricks dashboard (“Revenue Insights Amsterdam”) visualizes:

- Map of PC4s colored by `recommended_strategy`
- Top-20 postcodes by Airbnb revenue
- Scatter plot: rentals vs Airbnb revenue
- Data coverage: `both` vs `airbnb_only` vs `rentals_only`

Dashboard artifacts are included in:
- `docs/dashboard/00_overview.png`

If your Databricks UI supports export, also include:
- `dashboards/revenue_insights_amsterdam.json` (optional)

---

## Local development (optional)

If you want to run lint/tests locally:

### With `just` (recommended)
```bash
just
just lint
just test
````

### Without `just`

```bash
uv sync
uv run ruff check .
uv run pytest
```

---

## Notes on data coverage (important for interpretation)

Some postcodes appear as `airbnb_only` or `rentals_only` due to dataset coverage differences:

* Airbnb data contains listings with missing/noisy zipcodes (handled via centroid backfill).
* Kamernet data contains many listings outside Amsterdam; filtering/aggregation focuses on PC4.

The `gold_postcode_comparison` table includes a `recommended_strategy` field that is:

* **Airbnb (gross)** when both sources exist and Airbnb revenue is higher
* **Long-term rent (gross)** when both exist and rentals revenue is higher
* **Airbnb only (no rentals data)** / **Long-term only (no Airbnb data)** when coverage exists for only one source
  (these are flagged for coverage awareness rather than “absolute truth”)

See `docs/assumptions.md` and `docs/backfill_strategy.md` for details.

---

## Documentation (deep dive)

* `docs/architecture.md` — pipeline/dataflow overview (Medallion)
* `docs/backfill_strategy.md` — centroid NN backfill approach + quality checks
* `docs/assumptions.md` — revenue formulas + modeling assumptions

---

## Troubleshooting

* Declarative Pipelines: ensure the pipeline’s **Target schema** is set to `workspace.rent_airbnb`.
* Volumes: confirm input files exist under `/Volumes/workspace/rent_airbnb/invest_input/`.
* Spark Connect: avoid relying on `spark.conf.get()` for custom keys in scripts; use env vars instead.
