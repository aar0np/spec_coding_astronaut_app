#!/usr/bin/env python3
"""
Mission Assignment CSV Loader

Reads mission assignment data from data/astronauts_by_mission.csv and loads it
into the Astra DB astronauts_by_mission table using the Table API.
"""
from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add parent directory to path to import app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from astrapy import DataAPIClient

from app.core.config import ConfigurationError, get_settings


def parse_timestamp(timestamp_str: str) -> datetime:
    """
    Parse a timestamp string in format 'YYYY-MM-DD HH:MM:SS.mmm+0000'.

    Args:
        timestamp_str: Timestamp string from CSV

    Returns:
        datetime object with UTC timezone

    Raises:
        ValueError: If timestamp format is invalid
    """
    try:
        # Remove timezone suffix and milliseconds for parsing
        # Format: 1969-07-16 18:32:00.000+0000
        timestamp_str = timestamp_str.strip()

        # Split on the + to remove timezone
        if '+' in timestamp_str:
            timestamp_str = timestamp_str.split('+')[0]

        # Parse the datetime and add UTC timezone
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S.%f")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid timestamp format: {timestamp_str}") from exc


def validate_row(row: dict[str, str], row_num: int) -> tuple[bool, str]:
    """
    Validate a CSV row for required fields.

    Args:
        row: Dictionary of CSV row data
        row_num: Row number for error reporting

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields for the astronauts_by_mission table
    mission_name = row.get("mission_name", "").strip()
    if not mission_name:
        return False, f"Row {row_num}: Missing required field 'mission_name'"

    astronaut_name = row.get("astronaut_name", "").strip()
    if not astronaut_name:
        return False, f"Row {row_num}: Missing required field 'astronaut_name'"

    mission_start_date = row.get("mission_start_date", "").strip()
    if not mission_start_date:
        return False, f"Row {row_num}: Missing required field 'mission_start_date'"

    mission_end_date = row.get("mission_end_date", "").strip()
    if not mission_end_date:
        return False, f"Row {row_num}: Missing required field 'mission_end_date'"

    # ship_name is optional but should be present
    ship_name = row.get("ship_name", "").strip()
    if not ship_name:
        return False, f"Row {row_num}: Missing field 'ship_name'"

    return True, ""


def load_missions(csv_path: str, skip_duplicates: bool = True) -> None:
    """
    Load mission assignments from CSV into Astra DB.

    Args:
        csv_path: Path to the CSV file
        skip_duplicates: If True, skip existing records; if False, fail on duplicates
    """
    print(f"Loading mission assignments from {csv_path}")

    # Get configuration
    try:
        settings = get_settings()
    except ConfigurationError as exc:
        print(f"Configuration error: {exc}")
        sys.exit(1)

    # Connect to Astra DB using Table API
    try:
        client = DataAPIClient(settings.astra_db_application_token)
        database = client.get_database(
            settings.astra_db_api_endpoint,
            keyspace=settings.astra_db_keyspace
        )
        table = database.get_table("astronauts_by_mission")
        print(f"Connected to Astra DB table: astronauts_by_mission")
        print(f"Keyspace: {settings.astra_db_keyspace}")
    except Exception as exc:
        print(f"Failed to connect to Astra DB: {exc}")
        sys.exit(1)

    # Track statistics
    total_rows = 0
    inserted_count = 0
    skipped_count = 0
    failed_count = 0

    # Track static column values per mission (ship_name should be consistent)
    mission_ship_names: dict[str, str] = {}
    conflicts_detected = 0

    # Read and process CSV
    try:
        with open(csv_path, "r", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                total_rows += 1

                # Validate row
                is_valid, error_msg = validate_row(row, row_num)
                if not is_valid:
                    print(f"Validation error: {error_msg}")
                    failed_count += 1
                    continue

                # Extract and clean fields
                mission_name = row["mission_name"].strip()
                astronaut_name = row["astronaut_name"].strip()
                ship_name = row["ship_name"].strip()

                # Check for static column conflicts
                if mission_name in mission_ship_names:
                    if mission_ship_names[mission_name] != ship_name:
                        print(
                            f"WARNING Row {row_num}: Static column conflict detected! "
                            f"Mission '{mission_name}' has conflicting ship_name values: "
                            f"'{mission_ship_names[mission_name]}' vs '{ship_name}'"
                        )
                        conflicts_detected += 1
                else:
                    mission_ship_names[mission_name] = ship_name

                # Parse timestamps
                try:
                    mission_start_date = parse_timestamp(row["mission_start_date"])
                    mission_end_date = parse_timestamp(row["mission_end_date"])
                except ValueError as exc:
                    print(f"Row {row_num}: {exc}")
                    failed_count += 1
                    continue

                # Prepare document for insertion
                # Note: In Cassandra, the primary key is (mission_name, astronaut_name)
                # ship_name is a static column (same for all rows with same mission_name)
                document = {
                    "mission_name": mission_name,
                    "astronaut_name": astronaut_name,
                    "ship_name": ship_name,
                    "mission_start_date": mission_start_date,
                    "mission_end_date": mission_end_date,
                }

                # Check if record already exists using table API
                if skip_duplicates:
                    try:
                        # Query by primary key (mission_name, astronaut_name)
                        existing = table.find_one(
                            filter={
                                "mission_name": mission_name,
                                "astronaut_name": astronaut_name,
                            }
                        )
                        if existing:
                            print(
                                f"Row {row_num}: Skipping duplicate mission assignment: "
                                f"{mission_name} - {astronaut_name}"
                            )
                            skipped_count += 1
                            continue
                    except Exception as exc:
                        # If query fails, try to insert anyway
                        print(f"Row {row_num}: Warning - could not check for duplicate: {exc}")

                # Insert into database using table API
                try:
                    table.insert_one(document)
                    inserted_count += 1
                    print(
                        f"Row {row_num}: Inserted mission assignment: "
                        f"{mission_name} - {astronaut_name}"
                    )
                except Exception as exc:
                    print(
                        f"Row {row_num}: Failed to insert mission assignment "
                        f"{mission_name} - {astronaut_name}: {exc}"
                    )
                    failed_count += 1

    except FileNotFoundError:
        print(f"Error: CSV file not found: {csv_path}")
        sys.exit(1)
    except Exception as exc:
        print(f"Error reading CSV file: {exc}")
        sys.exit(1)

    # Print summary
    print("\n" + "=" * 60)
    print("Mission Assignment Load Summary")
    print("=" * 60)
    print(f"Total rows processed: {total_rows}")
    print(f"Successfully inserted: {inserted_count}")
    print(f"Skipped (duplicates): {skipped_count}")
    print(f"Failed: {failed_count}")
    if conflicts_detected > 0:
        print(f"Static column conflicts detected: {conflicts_detected}")
        print("WARNING: Some missions have inconsistent ship_name values!")
    print("=" * 60)

    # Exit with appropriate code
    if failed_count > 0:
        sys.exit(1)
    else:
        sys.exit(0)


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Load mission assignments from CSV into Astra DB"
    )
    parser.add_argument(
        "--csv",
        default="data/astronauts_by_mission.csv",
        help="Path to CSV file (default: data/astronauts_by_mission.csv)",
    )
    parser.add_argument(
        "--no-skip-duplicates",
        action="store_true",
        help="Fail on duplicate records instead of skipping them",
    )

    args = parser.parse_args()

    load_missions(args.csv, skip_duplicates=not args.no_skip_duplicates)


if __name__ == "__main__":
    main()


# Made with Bob%
