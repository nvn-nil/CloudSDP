from cloudsdp.api.bigquery import BigQuery


def main():
    bq = BigQuery("cloudsdp")
    dataset_name = "test_data_set_askdjalksdj"
    table_name = "asdjahskdjhashdalskjdlajs"

    data = [{"name": "Naveen", "age": 29}, {"name": "Something", "age": 22}]

    data_schema = [
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "type": "INTEGER", "mode": "REQUIRED"},
    ]

    print("Creating dataset")
    bq.create_dataset(dataset_name)

    print("Creating table")
    bq.create_table(table_name, data_schema, dataset_name)

    print("Ingesting data")
    errors = bq.ingest_json(data, dataset_name, table_name)
    if errors:
        print("Errors", ";".join(errors))

    print("Deleting dataset and contents")
    bq.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    main()
