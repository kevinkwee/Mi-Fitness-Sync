from __future__ import annotations


# Maps Mi Fitness sport_type integer → Strava sport_type string.
# Only types with a clear Strava equivalent are included; unmapped types
# return None so Strava auto-detects from the FIT session message instead.
_SPORT_TYPE_MAP: dict[int, str] = {
    # Core sport types
    1: "Run",                               # Outdoor Running
    2: "Walk",                              # Outdoor Walking
    3: "Run",                               # Indoor Running (Treadmill)
    4: "Hike",                              # Mountaineering
    5: "TrailRun",                          # Trail Running
    6: "Ride",                              # Outdoor Cycling
    7: "Ride",                              # Indoor Cycling
    8: "Workout",                           # Free Training
    9: "Swim",                              # Pool Swimming
    10: "Swim",                             # Open Water Swimming
    11: "Elliptical",                       # Elliptical
    12: "Yoga",                             # Yoga
    13: "Rowing",                           # Rowing Machine
    14: "Workout",                          # Jump Rope
    15: "Hike",                             # Hiking
    16: "HighIntensityIntervalTraining",    # HIIT
    17: "Workout",                          # Triathlon
    19: "Workout",                          # Basketball
    20: "Golf",                             # Golf
    21: "AlpineSki",                        # Skiing
    # Water sports
    100: "Sail",                            # Sailing
    101: "StandUpPaddling",                 # Paddle Board
    105: "Kayaking",                        # Kayaking
    106: "Kayaking",                        # Kayak Rafting
    113: "Kitesurf",                        # Kite Surfing
    114: "Surfing",                         # Indoor Surfing
    # Adventure & outdoor
    200: "RockClimbing",                    # Rock Climbing
    202: "InlineSkate",                     # Roller Skating
    207: "Walk",                            # Nordic Walking
    # Indoor fitness
    301: "StairStepper",                    # Stair Climbing
    302: "StairStepper",                    # Stepper
    305: "Pilates",                         # Pilates
    308: "WeightTraining",                  # Strength Training
    313: "WeightTraining",                  # Dumbbell Training
    314: "WeightTraining",                  # Barbell Training
    315: "WeightTraining",                  # Weight Lifting
    316: "WeightTraining",                  # Deadlift
    320: "WeightTraining",                  # Upper Limb Training
    321: "WeightTraining",                  # Lower Limb Training
    322: "WeightTraining",                  # Waist & Abdomen
    323: "WeightTraining",                  # Back Training
    324: "Ride",                            # Spinning
    333: "Walk",                            # Indoor Walking
    # Ball sports
    600: "Soccer",                          # Football
    609: "Tennis",                          # Tennis
    # Snow & ice
    700: "IceSkate",                        # Outdoor Skating
    707: "IceSkate",                        # Indoor Skating
    708: "Snowboard",                       # Snowboarding
    709: "NordicSki",                       # Skiing (General)
    710: "NordicSki",                       # Cross-Country Skiing
    # Climbing
    1000: "RockClimbing",                   # Indoor Rock Climbing
    1001: "RockClimbing",                   # Outdoor Rock Climbing
}


def strava_sport_type(mi_fitness_sport_type: int | None) -> str | None:
    """Map a Mi Fitness sport_type to the Strava upload sport_type string.

    Returns None when no clear mapping exists, letting Strava auto-detect from
    the FIT file's session sport/sub_sport fields.
    """
    if mi_fitness_sport_type is None:
        return None
    return _SPORT_TYPE_MAP.get(mi_fitness_sport_type)
