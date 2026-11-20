"""Parse user-supplied speed values into metres per second.

Supported formats:
- Plain number (with optional ``kmh`` suffix): treated as km/h.
  Examples: ``180``, ``180kmh``, ``50.5``
- Colon notation (with optional ``/km`` suffix): treated as min:sec per km.
  Examples: ``7:30``, ``7:30/km``
"""

from __future__ import annotations

import re

from mi_fitness_sync.exceptions import MiFitnessError

_PACE_RE = re.compile(r"^(\d+):(\d{1,2})(?:/km)?$")
_KMH_RE = re.compile(r"^(\d+(?:\.\d+)?)(?:kmh)?$", re.IGNORECASE)
_SECONDS_PER_MINUTE = 60


def parse_speed_input(value: str) -> float:
    """Convert a CLI speed string to metres per second.

    Raises :class:`MiFitnessError` when *value* cannot be parsed.
    """
    value = value.strip()

    # Pace format: colon means min:sec per km
    m = _PACE_RE.match(value)
    if m is not None:
        minutes = int(m.group(1))
        seconds = int(m.group(2))
        if seconds >= _SECONDS_PER_MINUTE:
            msg = f"Invalid pace '{value}': seconds must be 0-59."
            raise MiFitnessError(msg)
        total_seconds = minutes * 60 + seconds
        if total_seconds <= 0:
            msg = f"Invalid pace '{value}': pace must be greater than zero."
            raise MiFitnessError(msg)
        # pace in s/km converts to m/s as 1000 / total_seconds
        return 1000.0 / total_seconds

    # km/h format: plain number with optional 'kmh' suffix
    m = _KMH_RE.match(value)
    if m is not None:
        kmh = float(m.group(1))
        if kmh <= 0:
            msg = f"Invalid speed '{value}': speed must be greater than zero."
            raise MiFitnessError(msg)
        return kmh / 3.6

    msg = (
        f"Cannot parse speed '{value}'. Use km/h (e.g. '180' or '180kmh') or pace "
        "(e.g. '7:30' or '7:30/km')."
    )
    raise MiFitnessError(msg)
