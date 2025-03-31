from dataclasses import dataclass
from os import environ as env
from pathlib import Path

import pandas as pd
import yaml
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


@dataclass(frozen=True)
class DBConfig:
    DB_USER = env.get("DB_USER")
    DB_PASS = env.get("DB_PASS")
    DB_HOST = env.get("DB_HOST")
    DB_PORT = env.get("DB_PORT")
    DB_NAME = env.get("DB_NAME")

    def get_db_url(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


def load_map_settings():
    map_settings = pd.read_csv(Path(__file__).parent / "metadata.csv", skiprows=[0])
    map_settings.loc[:, ["record_source", "source_table"]] = map_settings[["record_source", "source_table"]].ffill()
    map_settings = map_settings[map_settings.target_table.notna()].reset_index()
    return map_settings


DV_METADATA = load_map_settings()

temp_file = Path(__file__).parent / "templates.yaml"

with Path(temp_file).open("r", encoding="utf-8") as f:
    TEMPLATES = yaml.safe_load(f)


if __name__ == '__main__':
    df = load_map_settings()