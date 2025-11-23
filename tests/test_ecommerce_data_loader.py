import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# Make sure the parent folder (project root) is on sys.path
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

import ecommerce_data_loader  # noqa: E402


class TestEcommerceLoader(unittest.TestCase):
    @patch("ecommerce_data_loader.requests.get")
    def test_fetch_paginated_multiple_pages(self, mock_get):
        # Page 1
        mock_resp_page1 = MagicMock()
        mock_resp_page1.status_code = 200
        mock_resp_page1.json.return_value = {
            "data": [{"id": "1"}, {"id": "2"}],
            "pagination": {"page": 1, "per_page": 100, "total": 4, "total_pages": 2}
        }

        # Page 2
        mock_resp_page2 = MagicMock()
        mock_resp_page2.status_code = 200
        mock_resp_page2.json.return_value = {
            "data": [{"id": "3"}, {"id": "4"}],
            "pagination": {"page": 2, "per_page": 100, "total": 4, "total_pages": 2}
        }

        mock_get.side_effect = [mock_resp_page1, mock_resp_page2]

        records = ecommerce_data_loader.fetch_paginated("/api/customers")

        self.assertEqual(len(records), 4)
        self.assertEqual(records[0]["id"], "1")
        self.assertEqual(records[-1]["id"], "4")

    @patch("ecommerce_data_loader.requests.get")
    def test_fetch_paginated_http_error_raises(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.text = "Internal Server Error"
        mock_get.return_value = mock_resp

        with self.assertRaises(ecommerce_data_loader.ApiError):
            ecommerce_data_loader.fetch_paginated("/api/customers")

    def test_transform_customers_adds_metadata_and_flattens(self):
        raw = [
            {
                "id": "cust_001",
                "email": "john.doe@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "phone": "+1-555-0123",
                "address": {
                    "street": "123 Main St",
                    "city": "New York",
                    "state": "NY",
                    "zip_code": "10001",
                    "country": "USA"
                },
                "created_at": "2024-01-15T10:30:00Z",
                "updated_at": "2024-01-15T10:30:00Z"
            }
        ]

        transformed = ecommerce_data_loader.transform_customers(raw)
        self.assertEqual(len(transformed), 1)
        rec = transformed[0]

        self.assertEqual(rec["address_city"], "New York")
        self.assertIn("_loaded_at", rec)
        self.assertIn("_source", rec)

    def test_transform_orders_metadata(self):
        raw = [
            {
                "id": "ord_001",
                "customer_id": "cust_001",
                "order_number": "ORD-001",
                "status": "completed",
                "total_amount": 100.0,
                "currency": "USD",
                "order_date": "2024-01-01T00:00:00Z",
                "shipped_date": None,
                "delivered_date": None,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            }
        ]

        transformed = ecommerce_data_loader.transform_orders(raw)
        self.assertEqual(len(transformed), 1)
        rec = transformed[0]
        self.assertEqual(rec["total_amount"], 100.0)
        self.assertIn("_loaded_at", rec)
        self.assertIn("_source", rec)

    def test_transform_order_line_items_metadata(self):
        raw = [
            {
                "id": "oli_001",
                "order_id": "ord_001",
                "product_id": "prod_001",
                "product_name": "Wireless Headphones",
                "quantity": 2,
                "unit_price": 149.99,
                "total_price": 299.98,
                "created_at": "2024-01-15T14:30:00Z",
                "updated_at": "2024-01-15T14:30:00Z"
            }
        ]

        transformed = ecommerce_data_loader.transform_order_line_items(raw)
        self.assertEqual(len(transformed), 1)
        rec = transformed[0]
        self.assertEqual(rec["quantity"], 2)
        self.assertIn("_loaded_at", rec)
        self.assertIn("_source", rec)

    @patch("ecommerce_data_loader.load_to_snowflake")
    def test_load_entity_no_records_returns_zero(self, mock_loader):
        stats = ecommerce_data_loader.load_entity(
            ecommerce_data_loader.SCHEMA_CUSTOMERS, [], "CUSTOMERS"
        )
        self.assertEqual(stats["records_loaded"], 0)
        self.assertEqual(stats["records_skipped"], 0)
        mock_loader.assert_not_called()

    @patch("ecommerce_data_loader.load_to_snowflake")
    def test_load_entity_calls_loader(self, mock_loader):
        mock_loader.return_value = {"records_loaded": 1, "records_skipped": 0}
        records = [{"id": "1"}]

        stats = ecommerce_data_loader.load_entity(
            ecommerce_data_loader.SCHEMA_CUSTOMERS, records, "CUSTOMERS"
        )

        mock_loader.assert_called_once()
        self.assertEqual(stats["records_loaded"], 1)
        self.assertEqual(stats["records_skipped"], 0)


if __name__ == "__main__":
    unittest.main()
