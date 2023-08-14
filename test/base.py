from unittest import TestCase


class BaseTestCase(TestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
