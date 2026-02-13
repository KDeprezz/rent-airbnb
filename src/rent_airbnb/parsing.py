import re
from typing import Optional

_PC4_RE = re.compile(r"(\d{4})")
_NUM_TOKEN_RE = re.compile(r"(\d+(?:\.\d+)?)")


def extract_pc4(text: Optional[str]) -> Optional[str]:
    """
    Extract a Dutch PC4 (first 4 digits) from a zipcode-ish string.

    Examples:
      "1018AS"   -> "1018"
      "1018 AS"  -> "1018"
      "342HUIS"  -> None
      None       -> None
    """
    if text is None:
        return None
    s = str(text).strip().upper()
    if s in {"", "NULL", "NONE"}:
        return None
    m = _PC4_RE.search(s)
    return m.group(1) if m else None


def parse_eur_amount(text: Optional[str]) -> Optional[float]:
    """
    Parse an amount in EUR from messy strings.

    Handles values like:
      "€ 950,-  Utilities incl." -> 950.0
      "€ 50"                     -> 50.0
      "-"                        -> None
      "950.."                    -> 950.0 (by extracting numeric token)

    Notes:
    - This is intentionally conservative: it extracts the first numeric token.
    - We normalize commas to dots and collapse repeated dots.
    """
    if text is None:
        return None

    s = str(text).strip().upper()
    if s in {"", "-", "N/A", "NULL", "NONE"}:
        return None

    # Keep only digits and separators
    s = re.sub(r"[^0-9\.,]", "", s)
    s = s.replace(",", ".")
    s = re.sub(r"\.+", ".", s)

    m = _NUM_TOKEN_RE.search(s)
    if not m:
        return None

    token = m.group(1)
    try:
        return float(token)
    except ValueError:
        return None
