from google.cloud import bigquery


# table_schema = [
#     {
#         'name': 'id',
#         'type': 'INTEGER',
#         'mode': 'REQUIRED'
#     },
#     {
#         'name': 'first_name',
#         'type': 'STRING',
#         'mode': 'NULLABLE'
#     },
#     {
#         'name': 'last_name',
#         'type': 'STRING',
#         'mode': 'NULLABLE'
#     }
# ]
def format_schema(schema):
    formatted_schema = []
    for row in schema:
        formatted_schema.append(bigquery.SchemaField(row["name"], row["type"], row["mode"]))
    return formatted_schema


class BigQuery:
    def __init__(self, project_id, location="EU"):
        self.project_id = project_id
        self.location = location

        self.client = bigquery.Client()

    def _get_dataset_id(self, dataset_name):
        return f"{self.project_id}.{dataset_name}"

    def _get_table_id(self, table_name, dataset_name):
        return f"{self.project_id}.{dataset_name}.{table_name}"

    def create_dataset(self, dataset_name):
        dataset_id = self._get_dataset_id(dataset_name)

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, timeout=30)
        return dataset

    def create_table(self, table_name, table_schema, dataset_name):
        table_id = self._get_table_id(table_name, dataset_name)

        schema = format_schema(table_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)
        return table

    def delete_dataset(self, dataset_name, delete_contents=False, not_found_ok=False):
        dataset_id = self._get_dataset_id(dataset_name)
        self.client.delete_dataset(dataset_id, delete_contents=delete_contents, not_found_ok=not_found_ok)

    def query(self, query):
        query_job = self.client.query(query)
        rows = query_job.result()

        return rows

    def ingest_json(self, data_rows, dataset_name, table_name):
        table_id = self._get_table_id(table_name, dataset_name)
        errors = self.client.insert_rows_json(table_id, data_rows)
        return errors
