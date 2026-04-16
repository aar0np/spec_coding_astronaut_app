#!/usr/bin/env python3
"""
Astronaut CSV Loader

Reads astronaut data from data/astronauts.csv and loads it into the Astra DB astronauts table.
"""
from __future__ import annotations

import csv
import sys
from datetime import date
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from astrapy import DataAPIClient

from app.core.config import ConfigurationError, get_settings


def parse_date(date_str: str) -> date:
    """Parse a date string in YYYY-MM-DD format."""
    try:
        year, month, day = date_str.split("-")
        return date(int(year), int(month), int(day))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid date format: {date_str}") from exc


def validate_row(row: dict[str, str], row_num: int) -> tuple[bool, str]:
    """
    Validate a CSV row for required fields.

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    name = row.get("name", "").strip()
    if not name:
        return False, f"Row {row_num}: Missing required field 'name'"

    birthplace = row.get("birthplace", "").strip()
    if not birthplace:
        return False, f"Row {row_num}: Missing required field 'birthplace'"

    dob = row.get("dob", "").strip()
    if not dob:
        return False, f"Row {row_num}: Missing required field 'dob'"

    # Validate date format
    try:
        parse_date(dob)
    except ValueError as exc:
        return False, f"Row {row_num}: Invalid date format for 'dob': {exc}"

    return True, ""


def load_astronauts(csv_path: str, skip_duplicates: bool = True) -> dict[str, int]:
    """
    Load astronaut data from CSV into Astra DB.

    Args:
        csv_path: Path to the CSV file
        skip_duplicates: If True, skip rows with duplicate names; if False, update existing records

    Returns:
        Dictionary with counts: inserted, updated, skipped, failed
    """
    stats = {
        "inserted": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }

    # Get settings and database connection
    try:
        settings = get_settings()
        client = DataAPIClient(settings.astra_db_application_token)
        db = client.get_database(
            settings.astra_db_api_endpoint,
            keyspace=settings.astra_db_keyspace
        )
    except ConfigurationError as exc:
        print(f"ERROR: Failed to connect to database: {exc}")
        return stats

    # Read and process CSV
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"ERROR: CSV file not found: {csv_path}")
        return stats

    print(f"Loading astronauts from {csv_path}...")
    print(f"Target keyspace: {settings.astra_db_keyspace}")
    print(f"Duplicate handling: {'skip' if skip_duplicates else 'update'}")
    print("-" * 60)

    # Track existing names if skip_duplicates is enabled
    existing_names = set()
    if skip_duplicates:
        try:
            # Try to get existing astronaut names
            # Using Data API to query the table
            collection = db.get_collection("astronauts")
            cursor = collection.find({}, projection={"name": 1})
            for doc in cursor:
                if "name" in doc:
                    existing_names.add(doc["name"])
        except Exception as e:
            # If collection doesn't exist or query fails, continue without checking
            print(f"Note: Could not check for existing records: {e}")

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # Validate row
            is_valid, error_msg = validate_row(row, row_num)
            if not is_valid:
                print(f"FAILED: {error_msg}")
                stats["failed"] += 1
                continue

            # Extract and clean fields
            name = row["name"].strip()
            birthplace = row["birthplace"].strip()
            dob_str = row["dob"].strip()
            university_name = row.get("university_name", "").strip() or None

            try:
                dob = parse_date(dob_str)

                # Check if astronaut already exists
                if name in existing_names and skip_duplicates:
                    print(f"SKIPPED: {name} (already exists)")
                    stats["skipped"] += 1
                    continue

                # Prepare document for Data API (no _id for CQL tables)
                document = {
                    "name": name,
                    "dob": dob.isoformat(),  # Store as ISO format string
                    "birthplace": birthplace,
                }
                if university_name:
                    document["university_name"] = university_name

                # Insert astronaut using Data API
                collection = db.get_collection("astronauts")

                if name in existing_names:
                    # Update existing using name as filter
                    collection.update_one({"name": name}, {"$set": document}, upsert=True)
                    print(f"UPDATED: {name}")
                    stats["updated"] += 1
                else:
                    # Insert new
                    collection.insert_one(document)
                    print(f"INSERTED: {name}")
                    stats["inserted"] += 1
                    existing_names.add(name)

            except Exception as exc:
                print(f"FAILED: Row {row_num} ({name}): {exc}")
                stats["failed"] += 1

    return stats


def main():
    """Main entry point for the loader script."""
    # Determine CSV path relative to script location
    script_dir = Path(__file__).parent
    csv_path = script_dir.parent / "data" / "astronauts.csv"

    print("=" * 60)
    print("Astronaut CSV Loader")
    print("=" * 60)

    try:
        stats = load_astronauts(str(csv_path), skip_duplicates=True)

        print("-" * 60)
        print("Load Summary:")
        print(f"  Inserted: {stats['inserted']}")
        print(f"  Updated:  {stats['updated']}")
        print(f"  Skipped:  {stats['skipped']}")
        print(f"  Failed:   {stats['failed']}")
        print("=" * 60)

        # Exit with appropriate code
        if stats["failed"] > 0:
            print("WARNING: Some rows failed to load")
            sys.exit(1)
        elif stats["inserted"] == 0 and stats["updated"] == 0:
            print("WARNING: No rows were loaded")
            sys.exit(1)
        else:
            print("SUCCESS: Load completed")
            sys.exit(0)

    except KeyboardInterrupt:
        print("\nINTERRUPTED: Load cancelled by user")
        sys.exit(130)
    except Exception as exc:
        print(f"FATAL ERROR: {exc}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

# Made with Bob
