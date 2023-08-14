import os

from cloudsdp.api.bigquery import BigQuery

PROJECT_NAME = os.environ.get("EXAMPLE_PROJECT_NAME", "DUMMY_PROJECT")


def main():
    bq = BigQuery(PROJECT_NAME)
    dataset_name = "dataset_1"
    table_name = "table_1"

    data = [{"name": "Someone", "age": 29}, {"name": "Something", "age": 22}]

    data_schema = [
        {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "field_type": "INTEGER", "mode": "REQUIRED"},
    ]

    print("Creating dataset")
    bq.create_dataset(dataset_name)

    print("Creating table")
    bq.create_table(table_name, data_schema, dataset_name)

    print("Ingesting data")
    errors = bq.ingest_rows_json(data, dataset_name, table_name)
    if errors:
        print("Errors", ";".join(errors))

    print("Deleting dataset and contents")
    bq.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    main()
