"""Delta table housekeeping: compact small files (streaming creates many) and vacuum old versions."""
from deltalake import DeltaTable

from common.lake import TABLES, storage_options

RETENTION_HOURS = 168  # 7 days of time travel


def main() -> None:
    for name, uri in TABLES.items():
        try:
            table = DeltaTable(uri, storage_options=storage_options())
        except Exception as exc:
            print(f"skip {name}: {exc}")
            continue
        metrics = table.optimize.compact()
        removed = table.vacuum(retention_hours=RETENTION_HOURS, dry_run=False)
        print(f"{name}: compaction={metrics} vacuumed_files={len(removed)}")


if __name__ == "__main__":
    main()
