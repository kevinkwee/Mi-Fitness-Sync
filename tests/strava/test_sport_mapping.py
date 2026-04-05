from __future__ import annotations

from mi_fitness_sync.strava.sport_mapping import strava_sport_type


def test_outdoor_running_maps_to_run():
    assert strava_sport_type(1) == "Run"


def test_trail_running_maps_to_trail_run():
    assert strava_sport_type(5) == "TrailRun"


def test_outdoor_cycling_maps_to_ride():
    assert strava_sport_type(6) == "Ride"


def test_pool_swimming_maps_to_swim():
    assert strava_sport_type(9) == "Swim"


def test_hiking_maps_to_hike():
    assert strava_sport_type(15) == "Hike"


def test_yoga_maps_to_yoga():
    assert strava_sport_type(12) == "Yoga"


def test_alpine_skiing_maps():
    assert strava_sport_type(21) == "AlpineSki"


def test_weight_training_maps():
    assert strava_sport_type(308) == "WeightTraining"


def test_unknown_type_returns_none():
    assert strava_sport_type(99999) is None


def test_none_type_returns_none():
    assert strava_sport_type(None) is None


def test_rock_climbing_maps():
    assert strava_sport_type(1000) == "RockClimbing"
    assert strava_sport_type(1001) == "RockClimbing"


def test_snowboard_maps():
    assert strava_sport_type(708) == "Snowboard"
