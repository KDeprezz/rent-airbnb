# Diagrams

## Data flow (Medallion + Declarative Pipeline)

```mermaid
flowchart LR
  A[Volumes: raw inputs<br/>airbnb.csv, rentals.json, geojson] --> B[Bronze<br/>raw ingestion<br/>schema-on-read]
  B --> C[Silver<br/>clean + standardize<br/>parse pc4, parse EUR, expectations]
  C --> D[Silver Backfill<br/>geo-based pc4 backfill]
  C --> E[Gold<br/>property revenue]
  D --> F[Gold<br/>postcode aggregates]
  F --> G[Gold<br/>postcode comparison]
  G --> H[Gold<br/>dashboard-ready + centroids]
  H --> I[AI/BI Dashboard<br/>map + ranking + scatter]
  H --> J[Export Parquet<br/>/data/output]
