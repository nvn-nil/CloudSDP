[![codecov](https://codecov.io/gh/nvn-nil/CloudSDP/branch/main/graph/badge.svg?token=P0U7YNO17D)](https://codecov.io/gh/nvn-nil/CloudSDP)
[![test](https://github.com/nvn-nil/CloudSDP/actions/workflows/ci.yaml/badge.svg)](https://github.com/nvn-nil/CloudSDP/actions/workflows/ci.yaml)

# CloudSDP Library

The CloudSDP library is designed to simplify the creation and management of serverless data pipelines between Google Cloud Run and Google BigQuery. It provides a developer-friendly interface to extract data from various sources, transform it, and seamlessly load it into BigQuery tables, all while leveraging the power of serverless architecture.

## Features

WIP:

- **Data Extraction and Ingestion**: Extract data from various sources, convert it into a common format, and ingest it into BigQuery tables.

TODO:

- **Data Transformation**: Perform data transformations, such as cleaning, enrichment, and normalization, before loading into BigQuery.
- **Scheduled Jobs and Triggers**: Schedule data pipeline jobs based on time triggers using Cloud Scheduler.
- **Data Pipeline Workflow**: Define and orchestrate data pipeline workflows with configurable execution order and dependencies.
- **Conflict Resolution and Error Handling**: Implement conflict resolution strategies and error handling mechanisms for reliable data processing.
- **Monitoring and Logging**: Monitor job progress, resource utilization, and performance metrics using integrated logging and monitoring tools.
- **Documentation and Examples**: Comprehensive documentation and code examples to guide developers in using the library effectively.

## Installation

Install the library using pip:

`pip install cloudsdp`

Or, install the library using poetry:

`poetry add cloudsdp`

## QuickStart

### Data Ingestion

#### Create dataset, ingest data and cleanup

From a python dict:

```py
import os

from cloudsdp.api.bigquery import BigQuery

PROJECT_NAME = "project_name"


def main():
    bq = BigQuery(PROJECT_NAME)
    dataset_name = "dataset_1"
    table_name = "table_1"

    data = [{"name": "Someone", "age": 29}, {"name": "Something", "age": 22}]

    data_schema = [
        {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "field_type": "INTEGER", "mode": "REQUIRED"},
    ]

    bq.create_dataset(dataset_name)

    bq.create_table(table_name, data_schema, dataset_name)

    errors = bq.ingest_rows_json(data, dataset_name, table_name)
    if errors:
        print("Errors", ";".join(errors))

    bq.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    main()

```

From csv files stored in GCS:

```py

import os

from cloudsdp.api.bigquery import BigQuery


PROJECT_NAME = "project_name"


def main():
    bq = BigQuery(PROJECT_NAME)
    dataset_name = "dataset_1"
    table_name = "table_1"

    data_schema = [
        {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "age", "field_type": "INTEGER", "mode": "REQUIRED"},
    ]

    bq.create_dataset(dataset_name)

    bq.create_table(table_name, data_schema, dataset_name)

    csv_uris = ["gs://mybucket/name_age_data_1.csv", "gs://mybucket/name_age_data_2.csv"]

    result = bq.ingest_csvs_from_cloud_bucket(
        csv_uris, dataset_name, table_name, skip_leading_rows=1, autodetect_schema=False, timeout=120
    )
    print(result)

    bq.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)


if __name__ == "__main__":
    main()


```
