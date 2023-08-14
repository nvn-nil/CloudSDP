from test.base import BaseTestCase
from unittest.mock import patch

from google.cloud import bigquery

from cloudsdp.api.bigquery import BigQuery, construct_schema_fields


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

        table_name = "table"
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
