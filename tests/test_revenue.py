from rent_airbnb.revenue import AirbnbAssumptions, airbnb_annual_revenue, rental_annual_revenue


def test_rental_annual_revenue() -> None:
    assert rental_annual_revenue(1000.0) == 12000.0


def test_airbnb_annual_revenue() -> None:
    a = AirbnbAssumptions(occupancy_rate=0.5, days_per_year=365.0)
    assert airbnb_annual_revenue(100.0, a) == 18250.0
