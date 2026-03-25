import pandas as pd
from ingestion.functions import standardise

def test_standardise():
    raw = pd.DataFrame([{
        "TIME_PERIOD: Time Period":          "2024-01",
        "OBS_VALUE":                         "42.5",
        "OBS_STATUS: Observation Status":    "A",
        "OBS_COMMENT: Observation Comment":  None,
        "DATAFLOW":                          "ABS,BA_SA2,2.0.0",
        "MEASURE: Measure":                  "1: Number",
        "SECTOR: Sector of Ownership":       "1: Private",
        "WORK_TYPE: Type of work":           "1: New",
        "BUILDING_TYPE: Type of building":   "100: House",
        "REGION_TYPE: Region Type":          "SA2",
        "REGION: Region":                    "101021007: Braidwood",
        "FREQ: Frequency":                   "M: Monthly",
        "UNIT_MEASURE: Unit of Measure":     "NUM: Number",
        "UNIT_MULT: Unit of Multiplier":     "0: Units",
    }])

    result = standardise(raw, "Greater_Sydney", "2024-01-01", "run_123")

    assert "time_period" in result.columns
    assert result["obs_value"].dtype == "float64"
    assert result["ingestion_date"].iloc[0] == "2024-01-01"
    assert result["run_id"].iloc[0] == "run_123"