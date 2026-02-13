# Backfill Strategy (Airbnb postcode)

## Problem
Airbnb contains a significant number of records with missing or invalid zipcodes, which prevents postcode-level aggregation.

## Strategy
1. Extract **PC4** from the `zipcode` field when possible.
2. For records with missing PC4:
   - Compute a **centroid per PC4** from the provided `post_codes.geojson`.
   - Assign the **nearest centroid** to the Airbnb listing based on (lat, lon).
3. Track lineage with a provenance column:
   - `pc4_source = original` if PC4 came from the record
   - `pc4_source = geo_centroid_nn` if backfilled

## Why this approach
- Uses only provided datasets (no external dependency)
- Deterministic and reproducible
- Enables complete postcode coverage while remaining auditable

## Limitations
Nearest-centroid is an approximation. In production, this could be replaced or validated with:
- point-in-polygon mapping (more accurate)
- external geocoding services with caching and rate limiting
