"""
Fashion Retailer E-Commerce Platform — Distributed ACID Transactions
=====================================================================
Example code for the 'ecommerce-distributed-transactions' how-to page.

Data model:
  product::{sku}                     — catalog entry
  inventory::{sku}::{size}::{color}  — per-variant stock levels
  customer::{id}                     — profile + loyalty points
  order::{uuid}                      — order document
"""

import uuid
from datetime import timedelta
from typing import TYPE_CHECKING

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.durability import DurabilityLevel, ServerDurability
from couchbase.exceptions import (
    TransactionCommitAmbiguous,
    TransactionFailed,
)
from couchbase.options import ClusterOptions, TransactionConfig

if TYPE_CHECKING:
    from couchbase.transactions import AttemptContext


# tag::connect[]
def connect():
    opts = ClusterOptions(
        authenticator=PasswordAuthenticator("Administrator", "password"),
        transaction_config=TransactionConfig(
            durability=ServerDurability(DurabilityLevel.MAJORITY),
            timeout=timedelta(seconds=30),
        ),
    )
    cluster = Cluster.connect("couchbase://localhost", opts)
    bucket = cluster.bucket("ecommerce")
    return cluster, bucket.default_collection()
# end::connect[]


# tag::seed[]
def seed_data(collection):
    """Upsert sample product, inventory, and customer documents."""
    product_id = "product::DRESS-001"
    inventory_id = "inventory::DRESS-001::S::RED"
    customer_id = "customer::CUST-001"

    collection.upsert(product_id, {
        "type": "product",
        "sku": "DRESS-001",
        "name": "Floral Wrap Dress",
        "brand": "StyleHouse",
        "category": "dresses",
        "price": 89.99,
    })

    collection.upsert(inventory_id, {
        "type": "inventory",
        "sku": "DRESS-001",
        "size": "S",
        "color": "RED",
        "quantity_available": 10,
        "quantity_reserved": 0,
    })

    collection.upsert(customer_id, {
        "type": "customer",
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "loyalty_points": 500,
        "orders": [],
    })

    return product_id, inventory_id, customer_id
# end::seed[]


# tag::place-order[]
def place_order(cluster, collection, customer_id, inventory_id, product_id, quantity=1):
    """
    Atomically:
      1. Reserve inventory  (quantity_available -= N, quantity_reserved += N)
      2. Insert a new order document
      3. Award loyalty points to the customer (1 point per dollar spent)
    """
    order_id = f"order::{uuid.uuid4()}"

    def txn_logic(ctx):  # type: (AttemptContext) -> None
        # Check and reserve stock
        inv_doc = ctx.get(collection, inventory_id)
        inv = inv_doc.content_as[dict]

        if inv["quantity_available"] < quantity:
            raise ValueError(
                f"Insufficient stock: requested {quantity}, "
                f"available {inv['quantity_available']}"
            )

        inv["quantity_available"] -= quantity
        inv["quantity_reserved"] += quantity
        ctx.replace(inv_doc, inv)

        # Use the stamped sale price on the inventory doc if available,
        # otherwise fall back to the product catalog for the current price.
        unit_price = inv.get("current_price")
        if unit_price is None:
            prod_doc = ctx.get(collection, product_id)
            unit_price = prod_doc.content_as[dict]["price"]
        total = round(unit_price * quantity, 2)

        # Create the order
        ctx.insert(collection, order_id, {
            "type": "order",
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "confirmed",
            "items": [{
                "sku": inv["sku"],
                "size": inv["size"],
                "color": inv["color"],
                "quantity": quantity,
                "unit_price": unit_price,
            }],
            "total": total,
            "currency": "USD",
        })

        # Award loyalty points
        cust_doc = ctx.get(collection, customer_id)
        cust = cust_doc.content_as[dict]
        cust["loyalty_points"] += int(total)
        cust.setdefault("orders", []).append(order_id)
        ctx.replace(cust_doc, cust)

    try:
        cluster.transactions.run(txn_logic)
        return order_id
    except TransactionFailed as ex:
        print(f"Order failed (did not commit): {ex}")
        return None
    except TransactionCommitAmbiguous as ex:
        # Transaction may or may not have committed — the caller must query
        # the order document to determine the final state before retrying.
        print(f"Order outcome ambiguous: {ex}")
        raise
# end::place-order[]


# tag::fulfill-order[]
def fulfill_order(cluster, collection, order_id, inventory_id, customer_id):
    """
    Atomically:
      1. Mark order as 'shipped'
      2. Release the inventory reservation
      3. Award a 10-point shipment bonus
    """
    def txn_logic(ctx):  # type: (AttemptContext) -> None
        order_doc = ctx.get(collection, order_id)
        order = order_doc.content_as[dict]

        if order["status"] != "confirmed":
            raise ValueError(
                f"Cannot fulfill order {order_id} with status '{order['status']}'"
            )

        shipped_qty = sum(i["quantity"] for i in order["items"])
        order["status"] = "shipped"
        ctx.replace(order_doc, order)

        inv_doc = ctx.get(collection, inventory_id)
        inv = inv_doc.content_as[dict]
        inv["quantity_reserved"] = max(0, inv["quantity_reserved"] - shipped_qty)
        ctx.replace(inv_doc, inv)

        cust_doc = ctx.get(collection, customer_id)
        cust = cust_doc.content_as[dict]
        cust["loyalty_points"] += 10
        ctx.replace(cust_doc, cust)

    try:
        cluster.transactions.run(txn_logic)
    except TransactionFailed as ex:
        print(f"Fulfillment failed: {ex}")
    except TransactionCommitAmbiguous as ex:
        print(f"Fulfillment outcome ambiguous: {ex}")
# end::fulfill-order[]


# tag::process-return[]
def process_return(cluster, collection, order_id, customer_id, inventory_id):
    """
    Atomically:
      1. Set order status to 'returned'
      2. Restore available inventory
      3. Deduct loyalty points originally awarded for the purchase
    """
    def txn_logic(ctx):  # type: (AttemptContext) -> None
        order_doc = ctx.get(collection, order_id)
        order = order_doc.content_as[dict]

        # Capture original_status before overwriting so we can use it below.
        original_status = order["status"]
        if original_status not in ("confirmed", "shipped"):
            raise ValueError(
                f"Order {order_id} cannot be returned (status: {original_status})"
            )

        returned_qty = sum(i["quantity"] for i in order["items"])
        refund_points = int(order["total"])
        order["status"] = "returned"
        ctx.replace(order_doc, order)

        inv_doc = ctx.get(collection, inventory_id)
        inv = inv_doc.content_as[dict]
        inv["quantity_available"] += returned_qty
        # Only release the reservation if the order was not yet fulfilled.
        # For shipped orders, fulfill_order already cleared quantity_reserved.
        if original_status == "confirmed":
            inv["quantity_reserved"] = max(0, inv["quantity_reserved"] - returned_qty)
        ctx.replace(inv_doc, inv)

        cust_doc = ctx.get(collection, customer_id)
        cust = cust_doc.content_as[dict]
        cust["loyalty_points"] = max(0, cust["loyalty_points"] - refund_points)
        ctx.replace(cust_doc, cust)

    try:
        cluster.transactions.run(txn_logic)
    except TransactionFailed as ex:
        print(f"Return failed: {ex}")
    except TransactionCommitAmbiguous as ex:
        print(f"Return outcome ambiguous: {ex}")
# end::process-return[]


# tag::flash-sale[]
def apply_flash_sale(cluster, collection, product_id, inventory_id, discount_pct):
    """
    Atomically apply a percentage discount to both the product catalog and
    the inventory document so they are never out of sync during a sale.
    """
    def txn_logic(ctx):  # type: (AttemptContext) -> None
        prod_doc = ctx.get(collection, product_id)
        prod = prod_doc.content_as[dict]

        # Use the stored original price as the base so repeated calls
        # to apply_flash_sale do not compound the discount.
        original_price = prod.get("original_price", prod["price"])
        sale_price = round(original_price * (1 - discount_pct / 100), 2)
        prod["original_price"] = original_price
        prod["price"] = sale_price
        prod["on_sale"] = True
        ctx.replace(prod_doc, prod)

        # Stamp the sale price on the inventory doc so the order service
        # sees a consistent price without an extra catalog lookup.
        inv_doc = ctx.get(collection, inventory_id)
        inv = inv_doc.content_as[dict]
        inv["current_price"] = sale_price
        ctx.replace(inv_doc, inv)

    try:
        cluster.transactions.run(txn_logic)
    except TransactionFailed as ex:
        print(f"Flash sale failed: {ex}")
    except TransactionCommitAmbiguous as ex:
        print(f"Flash sale outcome ambiguous: {ex}")
# end::flash-sale[]


# tag::error-handling[]
def place_order_with_full_error_handling(
    cluster, collection, customer_id, inventory_id, product_id, quantity=1
):
    """
    Demonstrates complete error handling for distributed transactions.
    TransactionFailed guarantees the transaction did NOT commit.
    TransactionCommitAmbiguous means the outcome is unknown — re-raise it
    so the caller can query Couchbase to determine the final state.
    """
    order_id = f"order::{uuid.uuid4()}"

    def txn_logic(ctx):  # type: (AttemptContext) -> None
        inv_doc = ctx.get(collection, inventory_id)
        inv = inv_doc.content_as[dict]

        if inv["quantity_available"] < quantity:
            # Raising any exception causes an immediate rollback.
            raise ValueError("Out of stock")

        inv["quantity_available"] -= quantity
        inv["quantity_reserved"] += quantity
        ctx.replace(inv_doc, inv)

        unit_price = inv.get("current_price")
        if unit_price is None:
            prod_doc = ctx.get(collection, product_id)
            unit_price = prod_doc.content_as[dict]["price"]
        total = round(unit_price * quantity, 2)

        ctx.insert(collection, order_id, {
            "type": "order",
            "order_id": order_id,
            "customer_id": customer_id,
            "status": "confirmed",
            "total": total,
        })

    try:
        result = cluster.transactions.run(txn_logic)
        if not result.unstaging_complete:
            # Rare: transaction committed but async cleanup is still running.
            # The data is visible to other transactions; background cleanup
            # will complete without further action from the application.
            print("Transaction committed; async cleanup in progress.")
        return order_id
    except TransactionCommitAmbiguous as ex:
        # The transaction may or may not have reached the commit point.
        # Re-raise so the caller can query the order document to confirm
        # before deciding to retry.
        print(f"Transaction outcome ambiguous — verify order {order_id}: {ex}")
        raise
    except TransactionFailed as ex:
        # The transaction definitely did not commit. Safe to retry or report failure.
        print(f"Transaction did not commit: {ex}")
        return None
# end::error-handling[]
