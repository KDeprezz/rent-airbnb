# Architecture

## Goal
Identify Amsterdam postal codes (PC4) with investment potential and compare gross revenue potential between:
- **Short-term (Airbnb)**: nightly price-based estimate
- **Long-term (Kamernet rentals)**: monthly rent-based estimate

## Medallion Architecture
This project follows the Medallion Architecture:

### Bronze (raw, immutable)
- `bronze_airbnb`: raw CSV ingestion with ingest metadata
- `bronze_rentals`: raw JSON ingestion with ingest metadata
- `bronze_postcodes_geojson`: raw GeoJSON ingestion

### Silver (cleaned, conformed, enriched)
- `silver_airbnb_clean`: cast types, normalize postcode, filter implausible prices
- `silver_airbnb_backfilled`: backfill missing PC4 using GeoJSON-derived nearest centroid and track provenance
- `silver_rentals_clean`: extract `_id`, cast types, parse rent fields, filter to Amsterdam

### Gold (decision-ready)
- `gold_property_revenue`: property-level gross revenue estimates per channel
- `gold_*_postcode`: postcode-level aggregates for both channels
- `gold_postcode_comparison`: recommended strategy by PC4 based on gross revenue comparison
- `gold_airbnb_backfill_stats`: shows impact of backfill

## Why Declarative Pipelines
Spark Declarative Pipelines keep dataset definitions explicit, composable, and easy to review. The pipeline structure mirrors production patterns:
- reproducible runs
- built-in data quality expectations
- clear lineage from Bronze → Silver → Gold
