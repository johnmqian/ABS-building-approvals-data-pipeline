import pandas as pd
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SA2s_FOLDER = SCRIPT_DIR / "2021_SA2_lists"

BUILDING_TYPES = [100, 110, 120, 121, 122, 130, 131, 132, 133, 134, 150]

ABS_BASE_URL = "https://data.api.abs.gov.au/rest/data/ABS,BA_SA2,2.0.0"
ABS_API_HEADERS = {"Accept": "application/vnd.sdmx.data+csv;labels=both"}

def list_region_files() -> list[str]:
    # Returns a list of region files
    return [file.stem for file in SA2s_FOLDER.glob("*.txt")]

def load_sa2_codes(region_file: str) -> list[str]:
    # Returns a list of SA2 codes for a region file
    path = SA2s_FOLDER / f"{region_file}.txt"
    with open(path, "r", encoding="utf-8") as open_region_file:
        return [line.strip() for line in open_region_file if line.strip().isdigit()]

def build_abs_url(sa2_codes: list[str], start_period: str, end_period: str) -> str:
    # Builds a string for ABS API for the get request
    measures = "1+2.1+5+9.1+8." + "+".join(map(str, BUILDING_TYPES))
    regions = "+".join(sa2_codes)
    return (
        f"{ABS_BASE_URL}/{measures}.SA2.{regions}.M"
        f"?startPeriod={start_period}&endPeriod={end_period}"
    )

def standardise(df: pd.DataFrame, region_file: str, ingestion_date: str, run_id: str) -> pd.DataFrame:
    # Standardises the ABS data to ensure consistency in column names etc.
    df = df.copy()
    rename_column_map = {
        "TIME_PERIOD: Time Period": "time_period",
        "OBS_VALUE": "obs_value",
        "OBS_STATUS: Observation Status": "obs_status",
        "OBS_COMMENT: Observation Comment": "obs_comment",
        "DATAFLOW": "dataflow",
        "MEASURE: Measure": "measure",
        "SECTOR: Sector of Ownership": "sector",
        "WORK_TYPE: Type of work": "work_type",
        "BUILDING_TYPE: Type of building": "building_type",
        "REGION_TYPE: Region Type": "region_type",
        "REGION: Region": "region",
        "FREQ: Frequency": "frequency",
        "UNIT_MEASURE: Unit of Measure": "unit_measure",
        "UNIT_MULT: Unit of Multiplier": "unit_mult",
    }

    df = df.rename(columns=rename_column_map)
    df["time_period"] = pd.to_datetime(df["time_period"], format="%Y-%m", errors="coerce").dt.date
    df["obs_value"] = pd.to_numeric(df["obs_value"], errors="coerce").astype("float64")

    df["region_file"] = region_file
    df["ingestion_date"] = ingestion_date
    df["run_id"] = run_id

    return df