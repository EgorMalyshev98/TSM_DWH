import argparse
import subprocess
import sys
from pathlib import Path

from loguru import logger

from config import load_map_settings
from data_vault_generator import ALL_MODEL_TYPES, DataVaultGenerator


def _dbt_exe() -> Path:
    """Return path to dbt executable in the same venv as the current Python."""
    scripts_dir = Path(sys.executable).parent
    dbt = scripts_dir / "dbt.exe" if sys.platform == "win32" else scripts_dir / "dbt"
    return dbt if dbt.exists() else Path("dbt")


def run_dbt_macro_gen_sources(dv_source_path: Path, target_dir: Path):
    result = subprocess.run(
        [str(_dbt_exe()), "--quiet", "run-operation", "dv_generate_source"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=target_dir,
    )

    if result.returncode:
        logger.error(result.stdout)
        return

    with open(dv_source_path, "w", encoding="utf-8") as f:
        f.write(result.stdout)

    logger.info("sources.yml generated")


def main():
    parser = argparse.ArgumentParser(
        prog="dvgen",
        description="Generate dbt Data Vault 2.0 models from metadata CSV",
    )
    parser.add_argument(
        "--target-dir",
        type=Path,
        required=True,
        metavar="DIR",
        help="Path to the dbt project directory (e.g. tsm_dwh)",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=Path.cwd() / "metadata.csv",
        metavar="FILE",
        help="Path to metadata CSV (default: metadata.csv in current directory)",
    )
    parser.add_argument(
        "--no-sources",
        action="store_true",
        help="Skip generating sources.yml via dbt macro",
    )
    parser.add_argument(
        "--model-type",
        nargs="+",
        metavar="TYPE",
        choices=sorted(ALL_MODEL_TYPES),
        help=(
            "Model types to generate (default: all). "
            f"Choices: {', '.join(sorted(ALL_MODEL_TYPES))}. "
            "Example: --model-type hub sat"
        ),
    )

    args = parser.parse_args()

    target_dir: Path = args.target_dir.resolve()
    metadata_path: Path = args.metadata.resolve()

    if not target_dir.exists():
        parser.error(f"Target directory not found: {target_dir}")

    if not metadata_path.exists():
        parser.error(f"Metadata file not found: {metadata_path}")

    stg_dir = target_dir / "models" / "stage"
    dv_dir = target_dir / "models" / "dv"
    dv_source_path = dv_dir / "sources.yml"

    metadata = load_map_settings(metadata_path)

    model_types = set(args.model_type) if args.model_type else None

    if not args.no_sources:
        run_dbt_macro_gen_sources(dv_source_path, target_dir)

    DataVaultGenerator(stg_dir, dv_dir, metadata, model_types=model_types).run()
    logger.info("data vault generated")
