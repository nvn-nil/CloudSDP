from test.base import BaseTestCase
from unittest.mock import patch

import numpy as np
import pandas as pd
from google.cloud import bigquery

from cloudsdp.api.bigquery import (
    BigQuery,
    WriteDisposition,
    clean_dataframe_using_schema,
    construct_schema_fields,
)


class TestBigQuery(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)

        self.project_id = "sample_project"
        self.location = "US"

        self.bigquery_clien_patcher = patch("cloudsdp.api.bigquery.bigquery.Client")
        self.patched_bigquery_client = self.bigquery_clien_patcher.start()

    def tearDown(self) -> None:
        super().tearDown()

        self.bigquery_clien_patcher.stop()

    def test_initialization(self):
        bq = BigQuery(self.project_id)

        self.assertEqual(self.patched_bigquery_client.call_count, 1)
        self.assertEqual(self.patched_bigquery_client.assert_called_once_with(), None)

        self.assertEqual(bq.project_id, self.project_id)
        self.assertEqual(bq.location, "EU")

        bq = BigQuery(self.project_id, location=self.location)

        self.assertEqual(self.patched_bigquery_client.call_count, 2)
        self.assertEqual(self.patched_bigquery_client.assert_called_with(), None)

        self.assertEqual(bq.project_id, self.project_id)
        self.assertEqual(bq.location, self.location)

        self.assertEqual(repr(bq), f"<BigQuery(project_id={self.project_id}, location={self.location})>")

    def test_dataset_id(self):
        bq = BigQuery(self.project_id)
        self.assertEqual(bq._get_dataset_id("dataset"), f"{self.project_id}.dataset")

    def test_table_id(self):
        bq = BigQuery(self.project_id)
        self.assertEqual(bq._get_table_id("table", "dataset"), f"{self.project_id}.dataset.table")

    def test_unguarded_create_table(self):
        bq = BigQuery(self.project_id)

        table_name = "table_name"
        dataset_name = "dataset"
        table_id = bq._get_table_id(table_name, dataset_name)

        schema_raw = [{"name": "name", "field_type": "STRING", "mode": "REQUIRED"}]
        schema = construct_schema_fields(schema_raw)

        table_ref = bigquery.Table(table_id, schema=schema)

        bq._unguarded_create_table(table_name, schema_raw, dataset_name)
        bq.client.create_table.assert_called_with(table_ref, timeout=None)

    def test_unguarded_create_dataset(self):
        bq = BigQuery(self.project_id)

        dataset_name = "dataset"
        dataset_id = bq._get_dataset_id(dataset_name)
        dataset_ref = bigquery.Dataset(dataset_id)
        dataset_ref.location = bq.location

        bq._unguarded_create_dataset(dataset_name)
        bq.client.create_dataset.assert_called_once()
        # bq.client.create_dataset.assert_called_once_with(dataset_ref, timeout=None) <- This does not work
        # So, check equality manually
        args = bq.client.create_dataset.call_args
        self.assertEqual(args.args[0].reference, dataset_ref.reference)
        self.assertEqual(args.kwargs, {"timeout": None})

    def test_ingest_from_dataframe_cleaning(self):
        df = pd.DataFrame(
            {
                "name": ["A", "B", "C", "D"],
                "score": [1.0, 2.0, 3.0, 4.0],
                "gender": ["male", "female", "male", "female"],
            }
        )
        schema = [
            {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
            {"name": "score", "field_type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "gender", "field_type": "STRING", "mode": "REQUIRED"},
        ]

        with self.subTest("No actions required to clean the dataframe"):
            cleaned_df = clean_dataframe_using_schema(df, schema)
            pd.testing.assert_frame_equal(df, cleaned_df)

        with self.subTest("Dataframe has extra columns not specified in the schema, drop them"):
            df_test = df.copy()
            df_test["field"] = 1

            cleaned_df = clean_dataframe_using_schema(df_test, schema)
            pd.testing.assert_frame_equal(df, cleaned_df)

        with self.subTest("One of the required fields is missing in the dataframe, raises Exception"):
            df_test = df.copy()
            df_test.drop(["gender"], axis=1, inplace=True)

            with self.assertRaisesRegexp(
                Exception, r"DataFrame is missing required fields from the schema: \[gender\]"
            ):
                clean_dataframe_using_schema(df_test, schema)

        with self.subTest("Mismatching type of a field in the schema, raise an exception"):
            df_test = df.copy()
            df_test["score"] = df_test["score"].astype(str)

            with self.assertRaisesRegexp(Exception, r"DataFrame column types do not match with schema: \[score\]"):
                clean_dataframe_using_schema(df_test, schema)

        with self.subTest("NaN in a REQUIRED field, raise an exception"):
            df_test = df.copy()
            df_test.at[0, "score"] = np.nan

            with self.assertRaisesRegexp(Exception, r"DataFrame has NaNs in non nullable columns: \[score\]"):
                clean_dataframe_using_schema(df_test, schema)

    def test_ingest_from_dataframe(self):
        bq = BigQuery(self.project_id)

        data = {"name": ["A", "B", "C", "D"], "score": [1.0, 2.0, 3.0, 4.0]}
        schema_raw = [
            {"name": "name", "field_type": "STRING", "mode": "REQUIRED"},
            {"name": "score", "field_type": "NUMERIC", "mode": "REQUIRED"},
        ]

        dataset_name = "dataset_name"
        table_name = "table_name"
        df = pd.DataFrame(data)

        with patch("cloudsdp.api.bigquery.BigQuery.get_table") as patched_get_table:
            table_id = bq._get_table_id(table_name, dataset_name)
            schema = construct_schema_fields(schema_raw)
            table = bigquery.Table(table_id, schema=schema)

            patched_get_table.return_value = table

            bq.ingest_from_dataframe(df, "dataset_name", "table_name")

            bq.client.load_table_from_dataframe.assert_called_once()
            args = bq.client.load_table_from_dataframe.call_args

            source_format = "PARQUET"
            pd.testing.assert_frame_equal(df, args.args[0])
            self.assertEqual(table, args.args[1])
            self.assertEqual(args.kwargs["location"], bq.location)
            self.assertEqual(args.kwargs["job_config"].source_format, source_format)
            self.assertEqual(args.kwargs["job_config"].write_disposition, WriteDisposition.WRITE_IF_TABLE_EMPTY)
