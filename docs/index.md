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

```py

from cloudsdp.api.bigquery import BigQuery

project_name = "projectname
dataset_name = "dataset_name_1"
table_name = "table_name_1"

data_schema = [
    {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
    {"name": "age", "field_type": "INTEGER", "mode": "REQUIRED"},
]
data = [{"name": "Someone", "age": 29}, {"name": "Something", "age": 92}]


bq = BigQuery(project_name)

bq.create_dataset(dataset_name, recreate=False) # recreate False is the default to prevent deletion of data
bq.create_table(table_name, data_schema, dataset_name, recreate=True) # recreate False is the default to prevent deletion of data

errors = bq.ingest_rows_json(data, dataset_name, table_name)

bq.delete_dataset(dataset_name, delete_contents=True)

```
