import logging
from concurrent.futures import TimeoutError

import pyarrow
from google.api_core.exceptions import GoogleAPICallError
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from cloudsdp.utilities import (
    clean_dataframe_using_schema,
    compare_schema,
    construct_schema_fields,
    deconstruct_schema_fields,
)

logger = logging.getLogger(__name__)


class WriteDisposition:
    WRITE_IF_TABLE_EMPTY = "WRITE_EMPTY"
    WRITE_AFTER_TABLE_TRUNCATE = "WRITE_TRUNCATE"
    WRITE_APPEND = "WRITE_APPEND"


class BigQuery:
    def __init__(self, project_id, location="EU"):
        self.project_id = project_id
        self.location = location

        self.client = bigquery.Client()

    def __repr__(self):
        return f"<BigQuery(project_id={self.project_id}, location={self.location})>"

    def _check_table_name(self, table_name):
        if table_name == "table":
            raise Exception("'table' is a reserved table id, use something else")

    def _get_dataset_id(self, dataset_name):
        return f"{self.project_id}.{dataset_name}"

    def _get_table_id(self, table_name, dataset_name):
        return f"{self.project_id}.{dataset_name}.{table_name}"

    def _unguarded_create_table(self, table_name, table_schema, dataset_name, timeout=None):
        self._check_table_name(table_name)

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
        self._check_table_name(table_name)

        table = self.get_table(table_name, dataset_name, not_found_ok=True)

        if table and not (recreate or recreate_if_schema_different):
            raise Exception("Table already exists")

        if table:
            schema_equal = compare_schema(deconstruct_schema_fields(table.schema), table_schema)
            if recreate or (not schema_equal and recreate_if_schema_different):
                self.delete_table(table_name, dataset_name, not_found_ok=True)
            else:
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

    def ingest_from_dataframe(
        self,
        dataframe,
        dataset_name,
        table_name,
        table_schema=None,
        source_format="PARQUET",
        write_disposition=WriteDisposition.WRITE_IF_TABLE_EMPTY,
    ):
        """Ingest data from a dataframe. Writes the dataframe using the specified source_format and write_disposition

        :param dataframe: _description_
        :type dataframe: _type_
        :param dataset_name: _description_
        :type dataset_name: _type_
        :param table_name: _description_
        :type table_name: _type_
        :param source_format: _description_, defaults to "PARQUET"
        :type source_format: str, optional
        :param write_disposition: Action to take when writing data to table, defaults to "WRITE_EMPTY"
            possible options:
            - WRITE_TRUNCATE: If the table already exists, BigQuery overwrites the data, removes the
                constraints, and uses the schema from the query result.
            - WRITE_APPEND: If the table already exists, BigQuery appends the data to the table.
            - WRITE_EMPTY: If the table already exists and contains data, a 'duplicate' error is
                returned in the job result.
        :type write_disposition: str, optional
        :raises exceptions.ConversionError: _description_
        """
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = write_disposition
        job_config.source_format = source_format

        table = self.get_table(table_name, dataset_name, not_found_ok=False)

        schema = table.schema if table_schema is None else table_schema
        schema = construct_schema_fields(schema)

        try:
            cleaned_df = clean_dataframe_using_schema(dataframe, schema)
            job = self.client.load_table_from_dataframe(
                cleaned_df,
                table,
                job_config=job_config,
                location=self.location,
            )
            job.result()
        except pyarrow.lib.ArrowInvalid as ex:
            raise Exception("Could not convert DataFrame to Parquet.") from ex

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
