from __future__ import annotations

import pytest

from mi_fitness_sync.activity.region_mapping import region_for_country_code
from mi_fitness_sync.exceptions import MiFitnessError


def test_region_for_country_code_maps_id_to_sg():
    assert region_for_country_code("ID") == "sg"


def test_region_for_country_code_rejects_unknown_country_code():
    with pytest.raises(MiFitnessError, match="Unsupported Mi Fitness country override: ZZ."):
        region_for_country_code("ZZ")