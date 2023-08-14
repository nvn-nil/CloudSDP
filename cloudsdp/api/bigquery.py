from concurrent.futures import TimeoutError

from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def compare_schema(schema_a, schema_b):
    """
    Compare two schemas
    :param schema_a: first list of schema fields.
    :param schema_b: second list of schema fields.
    :return:      if there is difference between both schema.
    """
    diff = [i for i in schema_a + schema_b if i not in schema_a or i not in schema_b]
    schema_equal = len(diff) == 0
    return schema_equal


def construct_schema_fields(schema):
    deserialized_schema = []
    for row in schema:
        deserialized_schema.append(bigquery.SchemaField(row["name"], row["field_type"], row["mode"]))
    return deserialized_schema


def deconstruct_schema_fields(schema):
    serialized_schema = []
    for field in schema.fields:
        serialized_schema.append({"name": field.name, "field_type": field.field_type, "mode": field.mode})

    return serialized_schema


class BigQuery:
    def __init__(self, project_id, location="EU"):
        self.project_id = project_id
        self.location = location

        self.client = bigquery.Client()

    def __repr__(self):
        return f"<BigQuery(project_id={self.project_id}, location={self.location})>"

    def _get_dataset_id(self, dataset_name):
        return f"{self.project_id}.{dataset_name}"

    def _get_table_id(self, table_name, dataset_name):
        return f"{self.project_id}.{dataset_name}.{table_name}"

    def _unguarded_create_table(self, table_name, table_schema, dataset_name, timeout=None):
        table_id = self._get_table_id(table_name, dataset_name)

        schema = construct_schema_fields(table_schema)
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table, timeout=timeout)

        return table

    def _unguarded_create_dataset(self, dataset_name, timeout=None):
        dataset_id = self._get_dataset_id(dataset_name)

        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.location
        dataset = self.client.create_dataset(dataset, timeout=timeout)
        return dataset

    def create_dataset(self, dataset_name, recreate=True):
        dataset = self.get_dataset(dataset_name, not_found_ok=True)

        if dataset and recreate:
            self.delete_dataset(dataset_name, delete_contents=True, not_found_ok=True)
        elif dataset:
            raise Exception("Dataset already exists")

        dataset = self._unguarded_create_dataset(dataset_name)
        return dataset

    def create_table(self, table_name, table_schema, dataset_name, recreate_if_schema_different=False, recreate=False):
        table = self.get_table(table_name, dataset_name, not_found_ok=True)

        if table and not (recreate or recreate_if_schema_different):
            return table

        schema_equal = compare_schema(deconstruct_schema_fields(table.schema), table_schema)

        if table and (recreate or (not schema_equal and recreate_if_schema_different)):
            self.delete_table(table_name, dataset_name, not_found_ok=True)
        elif table:
            raise Exception("Table already exists")

        table = self._unguarded_create_table(table_name, table_schema, dataset_name)
        return table

    def delete_dataset(self, dataset_name, delete_contents=False, not_found_ok=False):
        dataset_id = self._get_dataset_id(dataset_name)
        self.client.delete_dataset(dataset_id, delete_contents=delete_contents, not_found_ok=not_found_ok)

    def delete_table(self, table_name, dataset_name, not_found_ok=False):
        table_id = self._get_table_id(table_name, dataset_name)
        self.client.delete_table(table_id, not_found_ok=not_found_ok)

    def get_table(self, table_name, dataset_name, not_found_ok=False):
        table_id = self._get_table_id(table_name, dataset_name)

        try:
            table = self.client.get_table(table_id)
            return table
        except NotFound:
            if not not_found_ok:
                raise

            return None

    def get_dataset(self, dataset_name, not_found_ok=False):
        dataset_id = self._get_dataset_id(dataset_name)

        try:
            dataset = self.client.get_dataset(dataset_id)
            return dataset
        except NotFound:
            if not not_found_ok:
                raise

            return None

    def query(self, query):
        query_job = self.client.query(query)
        rows = query_job.result()

        return rows

    def ingest_rows_json(self, data_rows, dataset_name, table_name):
        table_id = self._get_table_id(table_name, dataset_name)
        errors = self.client.insert_rows_json(table_id, data_rows)
        return errors

    def ingest_csvs_from_cloud_bucket(
        self,
        csv_uris,
        dataset_name,
        table_name,
        skip_leading_rows=1,
        autodetect_schema=False,
        job_id=None,
        job_prefix=None,
        job_config=None,
        retry=None,
        timeout=None,
    ):

        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = autodetect_schema
        job_config.skip_leading_rows = skip_leading_rows
        job_config.source_format = bigquery.SourceFormat.CSV

        table_id = self._get_table_id(table_name, dataset_name)
        # NOTE: uri should be a google cloud bucket path, like "gs://mybucket/mydata.csv"
        job = self.client.load_table_from_uri(
            csv_uris,
            table_id,
            job_id=job_id,
            job_prefix=job_prefix,
            job_config=job_config,
            retry=retry,
            timeout=timeout,
        )

        try:
            # wait for load job to complete
            result = job.result()
            return result
        except GoogleAPICallError:
            raise Exception("Job failed")
        except TimeoutError:
            raise Exception("Timeout reached before load finished")
