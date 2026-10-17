"""Download the Amazon Books Reviews dataset into the raw data directory."""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Load KAGGLE_API_TOKEN / KAGGLE_USERNAME from the project .env before
# kagglehub resolves credentials. Variables already set in the shell win.
load_dotenv(PROJECT_ROOT / ".env")

import kagglehub  # noqa: E402


DATASET_HANDLE = "mohamedbakhet/amazon-books-reviews"
EXPECTED_FILES = ("books_data.csv", "Books_rating.csv")


def find_dataset_files(dataset_path: Path) -> dict[str, Path]:
    """Find the two source CSVs regardless of the cache layout."""
    files: dict[str, Path] = {}
    for expected_name in EXPECTED_FILES:
        matches = list(dataset_path.rglob(expected_name))
        if len(matches) > 1:
            raise RuntimeError(
                f"Found multiple copies of {expected_name} under {dataset_path}."
            )
        if matches:
            files[expected_name] = matches[0]

    missing = [name for name in EXPECTED_FILES if name not in files]
    if missing:
        raise RuntimeError(
            "Kaggle dataset did not contain the expected files: "
            + ", ".join(missing)
        )
    return files


def check_credentials() -> None:
    """Fail early with a clear message when no Kaggle credentials are set."""
    token = os.getenv("KAGGLE_API_TOKEN", "").strip()
    if token.startswith("replace_with"):
        token = ""
    legacy = os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")
    kaggle_dir = Path.home() / ".kaggle"
    has_file = any(
        (kaggle_dir / name).exists()
        for name in ("access_token", "access_token.txt", "kaggle.json")
    )
    if not (token or legacy or has_file):
        raise SystemExit(
            "Missing Kaggle credentials. Set KAGGLE_API_TOKEN in "
            f"{PROJECT_ROOT / '.env'} (see .env.example)."
        )


def download_dataset(raw_dir: Path, force_download: bool = False) -> None:
    """Download and place the dataset files at the paths consumed by Spark."""
    check_credentials()
    raw_dir.mkdir(parents=True, exist_ok=True)

    try:
        dataset_path = Path(
            kagglehub.dataset_download(
                DATASET_HANDLE,
                force_download=force_download,
            )
        )
    except Exception as exc:
        raise RuntimeError(
            f"Kaggle download failed ({exc}). Check KAGGLE_API_TOKEN in .env "
            "(https://www.kaggle.com/docs/api), then retry."
        ) from exc

    source_files = find_dataset_files(dataset_path)
    for file_name, source_path in source_files.items():
        destination = raw_dir / file_name
        if source_path.resolve() == destination.resolve():
            continue
        temporary_destination = destination.with_name(f".{destination.name}.part")
        try:
            shutil.copy2(source_path, temporary_destination)
            os.replace(temporary_destination, destination)
        finally:
            temporary_destination.unlink(missing_ok=True)
        print(f"Downloaded {file_name} ({destination.stat().st_size:,} bytes)")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download Amazon Books Reviews CSVs into data/raw."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw",
        help="Directory for books_data.csv and Books_rating.csv.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force KaggleHub to download a fresh dataset copy.",
    )
    args = parser.parse_args()
    download_dataset(args.output_dir, force_download=args.force)


if __name__ == "__main__":
    main()
