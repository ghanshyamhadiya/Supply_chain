import sys
sys.path.append('/Workspace/Users/iamhadiya13@gmail.com/Supply Chain/Ingestion/transformations')
from cleaning_raw_silver import clean_carriers, clean_inventory, clean_shipment, clean_supplier, clean_warehouse, clean_orders, clean_customers, clean_products
from schema_raw_silver import Carrier_schema, Warehouse_schema, Inventory_schema, Inventory_snapshot_schema, Orders_schema, Shipment_schema, Supplier_schema, Products_schema, Customer_schema
# from cleaning_silver_gold import 

LOAD_MODE='Full'

Raw_volume_path="/Volumes/workspace/default/supplychain/Raw_data/"

silver_volume_path="/Volumes/workspace/default/supplychain/silver_data/"

processed_volume_path="/Volumes/workspace/default/supplychain/processed_raw_files/"

TABLE_CONFIG = {
    "carriers": {
        "cleaner": clean_carriers,
        "schema": Carrier_schema,
        "gold_transform": "transform_carriers",
        "key": "carrier_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "inventory": {
        "cleaner": clean_inventory,
        "schema": Inventory_schema,
        "gold_transform": "transform_inventory",
        "key": "inventory_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "orders": {
        "cleaner": clean_orders,
        "schema": Orders_schema,
        "gold_transform": "transform_orders",
        "key": "order_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "inventory_snapshot": {
        "cleaner": clean_inventory,
        "schema": Inventory_snapshot_schema,
        "gold_transform": "transform_inventory",
        "key": "inventory_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "shipments": {
        "cleaner": clean_shipment,
        "schema": Shipment_schema,
        "gold_transform": "transform_shipments",
        "key": "shipment_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    "suppliers": {
        "cleaner": clean_supplier,
        "schema": Supplier_schema,
        "gold_transform": "transform_suppliers",
        "key": "supplier_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "warehouse": {
        "cleaner": clean_warehouse,
        "schema": Warehouse_schema,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },
    
    # Aliases for plural variations
    "warehouses": {
        "cleaner": clean_warehouse,
        "schema": Warehouse_schema,
        "gold_transform": "transform_warehouse",
        "key": "warehouse_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "customers": {
        "cleaner": clean_customers,
        "schema": Customer_schema,
        "gold_transform": "transform_customers",
        "key": "customer_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },

    "products": {
        "cleaner": clean_products,
        "schema": Products_schema,
        "gold_transform": "transform_products",
        "key": "product_id",
        "partition": "file_date",
        "load_type": LOAD_MODE
    },    
}

SILVER_PATH = {
    "carriers": silver_volume_path + "carriers/",
    "customers": silver_volume_path + "customers/",
    "inventory_snapshot": silver_volume_path + "inventory_snapshot/",
    "orders": silver_volume_path + "orders/",
    "products": silver_volume_path + "products/",
    "shipments": silver_volume_path + "shipments/",
    "suppliers": silver_volume_path + "suppliers/",
    "warehouses": silver_volume_path + "warehouses/",
}

GOLD_CONFIG = {
    "carriers": {
        "source": "carriers",
        "key": "carrier_id",
        "columns": ["carrier_id", "carrier_name", "carrier_type", "vehicle_type", "max_weight_kg", "coverage_region", "on_time_rate_pct", "damage_rate_pct", "cost_per_km", "is_active", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "customers": {
        "source": "customers",
        "key": "customer_id",
        "columns": ["customer_id", "customer_name", "customer_segment", "customer_tier", "city", "state", "region", "credit_term", "credit_due", "credit_limit", "is_active", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "inventory_snapshot": {
        "source": "inventory_snapshot",
        "key": "inventory_id",
        "columns": ["inventory_id", "warehouse_id", "sku_code", "product_category", "quantity_on_hand", "quantity_reserved", "quantity_available", "reorder_point", "unit_cost", "total_inventory_value", "last_replenishment_date", "snapshot_date", "is_below_reorder", "calculated_quantity_available", "final_quantity_available", "calculated_inventory_value", "quantity_status", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "orders": {
        "source": "orders",
        "key": "order_id",
        "columns": ["order_id", "customer_id", "supplier_id", "warehouse_id", "order_date", "required_delivery", "product_category", "quantity_ordered", "unit_price", "order_value", "priority", "payment_status", "order_status", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "products": {
        "source": "products",
        "key": "product_id",
        "columns": ["product_id", "product_name", "product_category", "primary_supplier_id", "unit_cost", "unit_price", "margin_pct", "weight_kg", "is_fragile", "is_temperature_controlled", "shelf_life_days", "is_active", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "shipments": {
        "source": "shipments",
        "key": "shipment_id",
        "columns": ["shipment_id", "order_id", "supplier_id", "warehouse_id", "carrier_id", "product_category", "shipment_date", "expected_delivery", "actual_delivery", "quantity_ordered", "quantity_shipped", "quantity_delivered", "unit_price", "total_value", "origin_city", "destination_city", "route_code", "delay_days", "created_at", "load_date", "file_date", "is_delayed", "delivery_status", "unit_price_clean", "quantity_delivered_clean", "effective_delivered_value", "lost_in_transit"],
        "load_type": LOAD_MODE
    },
    "suppliers": {
        "source": "suppliers",
        "key": "supplier_id",
        "columns": ["supplier_id", "supplier_name", "contact_person", "email", "phone", "region", "city", "country", "lead_time_days", "rating", "contract_start", "contract_end", "payment_terms", "is_active", "payment_due", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
    "warehouses": {
        "source": "warehouses",
        "key": "warehouse_id",
        "columns": ["warehouse_id", "warehouse_name", "city", "state", "pincode", "region", "capacity_sqft", "current_utilization_pct", "manager_name", "phone", "is_active", "created_at", "load_date", "file_date"],
        "load_type": LOAD_MODE
    },
}

