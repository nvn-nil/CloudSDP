from test.base import BaseTestCase

from cloudsdp.api.bigquery import BigQuery


class TestBigQuery(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)

        self.project_id = "sample_project"
        self.location = "US"

    def test_initialization(self):
        bq = BigQuery(self.project_id)

        self.assertEqual(bq.project_id, self.project_id)
        self.assertEqual(bq.location, "EU")

        bq = BigQuery(self.project_id, location=self.location)

        self.assertEqual(bq.project_id, self.project_id)
        self.assertEqual(bq.location, self.location)

        self.assertEqual(repr(bq), f"<BigQuery(project_id={self.project_id}, location={self.location})>")
