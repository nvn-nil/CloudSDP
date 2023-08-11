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

    bq.create_dataset(dataset_name)
    bq.create_table(table_name, data_schema, dataset_name)

    result = bq.ingest_json(data, dataset_name, table_name)
    print(result)


if __name__ == "__main__":
    main()
