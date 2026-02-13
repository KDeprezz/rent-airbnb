from rent_airbnb.parsing import extract_pc4, parse_eur_amount


def test_extract_pc4() -> None:
    assert extract_pc4("1018AS") == "1018"
    assert extract_pc4("1018 AS") == "1018"
    assert extract_pc4("  1018-as  ") == "1018"
    assert extract_pc4("342HUIS") is None
    assert extract_pc4("b") is None
    assert extract_pc4("0") is None
    assert extract_pc4("") is None
    assert extract_pc4(None) is None


def test_parse_eur_amount() -> None:
    assert parse_eur_amount("€ 950,-  Utilities incl.") == 950.0
    assert parse_eur_amount("€ 50") == 50.0
    assert parse_eur_amount("-") is None
    assert parse_eur_amount("") is None
    assert parse_eur_amount(None) is None
    assert parse_eur_amount("950..") == 950.0

