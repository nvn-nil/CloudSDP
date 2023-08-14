import os

from cloudsdp.api.bigquery import BigQuery

PROJECT_NAME = os.environ.get("EXAMPLE_PROJECT_NAME", "DUMMY_PROJECT")
BUCKET_NAME = os.environ.get("EXAMPLE_BUCKET_NAME", "DUMMY_BUCKET")


def main():
    bq = BigQuery(PROJECT_NAME)
    dataset_name = "dataset_1"
    table_name = "table_1"

    data_schema = [
        {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "field_type": "INTEGER", "mode": "REQUIRED"},
    ]

    print("Creating dataset")
    bq.create_dataset(dataset_name)

    print("Creating table")
    bq.create_table(table_name, data_schema, dataset_name)

    csv_uris = ["gs://mybucket/name_age_data_1.csv", "gs://mybucket/name_age_data_2.csv"]

    print("Ingesting data")
    result = bq.ingest_csvs_from_cloud_bucket(
        csv_uris, dataset_name, table_name, skip_leading_rows=1, autodetect_schema=False, timeout=120
    )
    print(result)

    print("Deleting dataset and contents")
    bq.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    main()
