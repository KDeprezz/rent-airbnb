# Assumptions

## Revenue estimation (gross)
This assessment estimates gross annual revenue potential:

### Airbnb (short-term)
`annual_revenue = nightly_price * occupancy_rate * days_per_year`

Defaults are configurable in the pipeline:
- `occupancy_rate = 0.65`
- `days_per_year = 365`

### Rentals (long-term)
`annual_revenue = monthly_rent * 12`

## Scope
- Analysis is scoped to **Amsterdam** rentals (filter is configurable).
- Postal code is standardized to **PC4** to support consistent aggregation.

## Data quality rules
- Airbnb listings with implausible nightly prices are excluded (soft threshold).
- Rentals require a parseable monthly rent value to be used in revenue calculations.
