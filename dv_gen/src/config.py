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


def load_map_settings(metadata_path: Path):
    map_settings = pd.read_csv(metadata_path, skiprows=[0])
    map_settings.loc[:, ["record_source", "source_table"]] = map_settings[["record_source", "source_table"]].ffill()
    map_settings = map_settings[map_settings.target_table.notna()].reset_index()
    return map_settings

_templates_dir = Path(__file__).parent / "templates"

TEMPLATES: dict = {"insert": {}}
for _f in sorted(_templates_dir.glob("*.yaml")):
    _data = yaml.safe_load(_f.read_text(encoding="utf-8"))
    if _data:
        TEMPLATES["insert"].update(_data)


if __name__ == '__main__':
    df = load_map_settings(Path(__file__).parent / "metadata.csv")