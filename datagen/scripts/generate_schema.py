"""Read all Parquet output dirs and write master-schema.json.

Run after the pipeline completes:
    python3.11 datagen/scripts/generate_schema.py

Writes to: datagen/output/master-schema.json
"""
import json, os, sys

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "output")


def _parquet_schema(table_path):
    """Extract column names, types, and row count from a Parquet directory."""
    import pyarrow.parquet as pq
    ds = pq.ParquetDataset(table_path)
    schema = ds.schema
    row_count = sum(f.metadata.num_rows for f in ds.fragments)
    columns = []
    for i in range(len(schema)):
        field = schema.field(i)
        columns.append({"name": field.name, "type": str(field.type), "nullable": field.nullable})
    return {"columns": columns, "row_count": row_count}


def main():
    tables = {}
    for name in sorted(os.listdir(OUTPUT_PATH)):
        path = os.path.join(OUTPUT_PATH, name)
        if not os.path.isdir(path) or name.startswith("_"):
            continue
        try:
            info = _parquet_schema(path)
            tables[name] = info
        except Exception as e:
            print(f"  SKIP {name}: {e}", file=sys.stderr)

    out = {
        "description": "Schema for all synthetic data output tables.",
        "table_count": len(tables),
        "total_rows": sum(t["row_count"] for t in tables.values()),
        "tables": tables,
    }

    out_path = os.path.join(OUTPUT_PATH, "master-schema.json")
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)

    print(f"master-schema.json: {len(tables)} tables, {out['total_rows']:,} total rows → {out_path}")


if __name__ == "__main__":
    main()
