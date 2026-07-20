import unittest

from logfire_ingest import normalize_rows


class LogfireIngestTests(unittest.TestCase):
    def test_normalize_rows_converts_columns_to_dicts(self):
        columns = [
            {"name": "id", "type": "Int64"},
            {"name": "message", "type": "String"},
        ]
        rows = [[1, "hello"], [2, "world"]]

        normalized = normalize_rows(columns, rows)

        self.assertEqual(
            normalized,
            [{"id": 1, "message": "hello"}, {"id": 2, "message": "world"}],
        )


if __name__ == "__main__":
    unittest.main()
