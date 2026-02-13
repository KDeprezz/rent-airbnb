from dataclasses import dataclass


@dataclass(frozen=True)
class AirbnbAssumptions:
    """
    Assumptions for estimating Airbnb gross annual revenue.

    occupancy_rate: fraction of nights booked (0..1)
    days_per_year: number of days per year (default 365)
    """
    occupancy_rate: float = 0.65
    days_per_year: float = 365.0


def airbnb_annual_revenue(price_per_night: float, a: AirbnbAssumptions = AirbnbAssumptions()) -> float:
    """
    Gross annual revenue estimate for short-term rental.
    """
    return float(price_per_night) * float(a.occupancy_rate) * float(a.days_per_year)


def rental_annual_revenue(rent_per_month: float) -> float:
    """
    Gross annual revenue estimate for long-term rental.
    """
    return float(rent_per_month) * 12.0
