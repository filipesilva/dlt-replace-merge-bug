import argparse
import sys
import os
import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from dlt.sources.sql_database import sql_database

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('destination')
    args = parser.parse_args()
    destination = args.destination

    if destination != "postgres" and destination != "clickhouse":
        print("Must provide postgres or clickhouse arg")
        sys.exit(1)        

    pipeline = dlt.pipeline(
        pipeline_name="repro_pipeline",
        destination=destination,
        dataset_name="repro_dataset",
        # progress='log',
    )

    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    source_creds = ConnectionStringCredentials(f"sqlite:///{current_file_dir}/source.db")

    print("\n=== Replace with cursor then merge with cursor ===\n")

    source = sql_database(credentials=source_creds, backend="pyarrow").with_resources("jobs")

    source.jobs.apply_hints(incremental=dlt.sources.incremental("number"))
    info = pipeline.run(source, write_disposition="replace")
    print(info)

    source.jobs.apply_hints(incremental=dlt.sources.incremental("number"))
    info = pipeline.run(source, write_disposition="merge")
    print(info)

    print("\n=== Replace without cursor then merge with cursor ===\n")

    source = sql_database(credentials=source_creds, backend="pyarrow").with_resources("jobs")

    info = pipeline.run(source, write_disposition="replace")
    print(info)

    # fails on clickhouse
    source.jobs.apply_hints(incremental=dlt.sources.incremental("number"))
    info = pipeline.run(source, write_disposition="merge")
    print(info)  