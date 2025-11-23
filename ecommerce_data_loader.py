import os
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any
import requests

# -------------------------------------------------------------------
# Assume this function is implemented and available to import
# from snowflake_loader import load_to_snowflake
# -------------------------------------------------------------------
def load_to_snowflake(
    schema: dict,
    records: List[dict],
    database: str,
    schema_name: str,
    table: str
) -> Dict[str, int]:
    """
    Dummy signature for reference. Implementation is assumed to exist.
    """
    raise NotImplementedError("load_to_snowflake should be provided by the environment.")


# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------

API_BASE_URL = os.getenv("API_BASE_URL", "https://myshop.com")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "RAW")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "ECOMMERCE")
PIPELINE_SOURCE = os.getenv("PIPELINE_SOURCE", "myshop_rest_api")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


# -------------------------------------------------------------------
# Snowflake table schemas
# -------------------------------------------------------------------

SCHEMA_CUSTOMERS: Dict[str, str] = {
    "id": "VARCHAR",
    "email": "VARCHAR",
    "first_name": "VARCHAR",
    "last_name": "VARCHAR",
    "phone": "VARCHAR",
    "address_street": "VARCHAR",
    "address_city": "VARCHAR",
    "address_state": "VARCHAR",
    "address_zip_code": "VARCHAR",
    "address_country": "VARCHAR",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "VARCHAR"
}

SCHEMA_ORDERS: Dict[str, str] = {
    "id": "VARCHAR",
    "customer_id": "VARCHAR",
    "order_number": "VARCHAR",
    "status": "VARCHAR",
    "total_amount": "NUMBER(18,2)",
    "currency": "VARCHAR",
    "order_date": "TIMESTAMP_TZ",
    "shipped_date": "TIMESTAMP_TZ",
    "delivered_date": "TIMESTAMP_TZ",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "VARCHAR"
}

SCHEMA_ORDER_LINE_ITEMS: Dict[str, str] = {
    "id": "VARCHAR",
    "order_id": "VARCHAR",
    "product_id": "VARCHAR",
    "product_name": "VARCHAR",
    "quantity": "NUMBER(18,0)",
    "unit_price": "NUMBER(18,2)",
    "total_price": "NUMBER(18,2)",
    "created_at": "TIMESTAMP_TZ",
    "updated_at": "TIMESTAMP_TZ",
    "_loaded_at": "TIMESTAMP_TZ",
    "_source": "VARCHAR"
}


# -------------------------------------------------------------------
# Helper functions
# -------------------------------------------------------------------

class ApiError(Exception):
    """Custom exception for API related issues."""


def current_loaded_at_iso() -> str:
    """Returns current UTC timestamp in ISO 8601 with Z."""
    return datetime.now(timezone.utc).isoformat()


def fetch_paginated(endpoint: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """
    Fetch all pages for a given endpoint.

    Args:
        endpoint: API endpoint path (e.g., "/api/customers").
        params: Query params for the request (page will be injected/overridden).

    Returns:
        List of records (dictionaries) combined from all pages.
    """
    if params is None:
        params = {}

    records: List[Dict[str, Any]] = []
    page = 1
    total_pages = None

    logger.info("Fetching data from endpoint: %s", endpoint)

    while total_pages is None or page <= total_pages:
        params_with_page = dict(params)
        params_with_page["page"] = page

        url = f"{API_BASE_URL}{endpoint}"
        logger.debug("Requesting URL: %s with params: %s", url, params_with_page)

        try:
            response = requests.get(url, params=params_with_page, timeout=30)
        except requests.RequestException as exc:
            logger.error("HTTP request to %s failed: %s", url, exc)
            raise ApiError(f"HTTP request failed: {exc}") from exc

        if response.status_code != 200:
            logger.error("Non-200 response from %s: %s %s", url, response.status_code, response.text)
            raise ApiError(f"Non-200 response from {url}: {response.status_code}")

        try:
            payload = response.json()
        except ValueError as exc:
            logger.error("Failed to parse JSON response from %s: %s", url, exc)
            raise ApiError(f"Invalid JSON response from {url}") from exc

        data = payload.get("data", [])
        pagination = payload.get("pagination", {})
        if total_pages is None:
            total_pages = pagination.get("total_pages", 1)

        logger.info(
            "Fetched page %d/%s from %s with %d records",
            page,
            total_pages,
            endpoint,
            len(data)
        )
        records.extend(data)
        page += 1

        # Safety: break if pagination metadata missing or inconsistent
        if "total_pages" not in pagination and len(data) == 0:
            break

    logger.info("Total records fetched from %s: %d", endpoint, len(records))
    return records


def transform_customers(raw_customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Flatten address object and add metadata columns."""
    transformed: List[Dict[str, Any]] = []
    loaded_at = current_loaded_at_iso()

    for rec in raw_customers:
        address = rec.get("address", {}) or {}
        out = {
            "id": rec.get("id"),
            "email": rec.get("email"),
            "first_name": rec.get("first_name"),
            "last_name": rec.get("last_name"),
            "phone": rec.get("phone"),
            "address_street": address.get("street"),
            "address_city": address.get("city"),
            "address_state": address.get("state"),
            "address_zip_code": address.get("zip_code"),
            "address_country": address.get("country"),
            "created_at": rec.get("created_at"),
            "updated_at": rec.get("updated_at"),
            "_loaded_at": loaded_at,
            "_source": PIPELINE_SOURCE
        }
        transformed.append(out)

    logger.info("Transformed %d customers", len(transformed))
    return transformed


def transform_orders(raw_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Add metadata columns to orders."""
    transformed: List[Dict[str, Any]] = []
    loaded_at = current_loaded_at_iso()

    for rec in raw_orders:
        out = {
            "id": rec.get("id"),
            "customer_id": rec.get("customer_id"),
            "order_number": rec.get("order_number"),
            "status": rec.get("status"),
            "total_amount": rec.get("total_amount"),
            "currency": rec.get("currency"),
            "order_date": rec.get("order_date"),
            "shipped_date": rec.get("shipped_date"),
            "delivered_date": rec.get("delivered_date"),
            "created_at": rec.get("created_at"),
            "updated_at": rec.get("updated_at"),
            "_loaded_at": loaded_at,
            "_source": PIPELINE_SOURCE
        }
        transformed.append(out)

    logger.info("Transformed %d orders", len(transformed))
    return transformed


def transform_order_line_items(raw_olis: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Add metadata columns to order line items."""
    transformed: List[Dict[str, Any]] = []
    loaded_at = current_loaded_at_iso()

    for rec in raw_olis:
        out = {
            "id": rec.get("id"),
            "order_id": rec.get("order_id"),
            "product_id": rec.get("product_id"),
            "product_name": rec.get("product_name"),
            "quantity": rec.get("quantity"),
            "unit_price": rec.get("unit_price"),
            "total_price": rec.get("total_price"),
            "created_at": rec.get("created_at"),
            "updated_at": rec.get("updated_at"),
            "_loaded_at": loaded_at,
            "_source": PIPELINE_SOURCE
        }
        transformed.append(out)

    logger.info("Transformed %d order line items", len(transformed))
    return transformed


def load_entity(
    schema: Dict[str, str],
    records: List[Dict[str, Any]],
    table: str
) -> Dict[str, int]:
    """
    Load a list of records into a Snowflake table using the provided loader.

    Args:
        schema: Column -> Snowflake type mapping.
        records: List of rows to load.
        table: Table name (without db/schema).

    Returns:
        Loader statistics.
    """
    if not records:
        logger.warning("No records to load for table %s", table)
        return {"records_loaded": 0, "records_skipped": 0}

    logger.info(
        "Loading %d records into %s.%s.%s",
        len(records),
        SNOWFLAKE_DATABASE,
        SNOWFLAKE_SCHEMA,
        table
    )

    stats = load_to_snowflake(
        schema,
        records,
        SNOWFLAKE_DATABASE,
        SNOWFLAKE_SCHEMA,
        table
    )

    logger.info(
        "Loaded table %s: %s loaded, %s skipped",
        table,
        stats.get("records_loaded", 0),
        stats.get("records_skipped", 0)
    )
    return stats


def run_pipeline() -> None:
    """Orchestrates the full extract → transform → load pipeline."""
    logger.info("Starting e-commerce data pipeline")

    # Customers
    customers_raw = fetch_paginated("/api/customers")
    customers = transform_customers(customers_raw)
    load_entity(SCHEMA_CUSTOMERS, customers, "CUSTOMERS")

    # Orders
    orders_raw = fetch_paginated("/api/orders")
    orders = transform_orders(orders_raw)
    load_entity(SCHEMA_ORDERS, orders, "ORDERS")

    # Order line items
    olis_raw = fetch_paginated("/api/order-line-items")
    olis = transform_order_line_items(olis_raw)
    load_entity(SCHEMA_ORDER_LINE_ITEMS, olis, "ORDER_LINE_ITEMS")

    logger.info("E-commerce data pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()