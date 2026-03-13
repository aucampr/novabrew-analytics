#!/usr/bin/env python3
"""
NovaBrew Coffee — BigQuery Setup Script (Python)
Post 1: Building the Data Foundation

Prerequisites:
  - Google Cloud SDK installed
  - Install: pip install google-cloud-bigquery
  - Authentication (choose ONE):
    1. Run: gcloud auth application-default login
       (If gcloud is not in PATH, run from: C:\\Program Files (x86)\\Google\\Cloud SDK\\google-cloud-sdk\\bin)
    2. OR set GOOGLE_APPLICATION_CREDENTIALS environment variable to your service account JSON file path
    3. OR pass service account key file as second argument to this script

Usage:
  cd data_generation
  python setup_bigquery.py YOUR_PROJECT_ID [SERVICE_ACCOUNT_KEY_PATH]

Example:
  python setup_bigquery.py novabrew-analytics
  python setup_bigquery.py novabrew-analytics "C:\path\to\service-account-key.json"
"""

import sys
import os
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning

# Configuration
DATASET = "novabrew_raw"
LOCATION = "US"
# Get the script's directory and set data_dir to the output folder at project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "output")

# Table schemas
TABLE_SCHEMAS = {
    "customers": [
        SchemaField("customer_id", "STRING"),
        SchemaField("first_name", "STRING"),
        SchemaField("last_name", "STRING"),
        SchemaField("email", "STRING"),
        SchemaField("state", "STRING"),
        SchemaField("acquisition_channel", "STRING"),
        SchemaField("signup_date", "DATE"),
        SchemaField("is_subscriber", "BOOLEAN"),
        SchemaField("email_opt_in", "BOOLEAN"),
        SchemaField("date_of_birth", "DATE"),
    ],
    "orders": [
        SchemaField("order_id", "STRING"),
        SchemaField("customer_id", "STRING"),
        SchemaField("order_date", "DATE"),
        SchemaField("shipped_date", "DATE"),
        SchemaField("delivered_date", "DATE"),
        SchemaField("channel", "STRING"),
        SchemaField("promo_code", "STRING"),
        SchemaField("discount_pct", "FLOAT"),
        SchemaField("subtotal", "FLOAT"),
        SchemaField("shipping_cost", "FLOAT"),
        SchemaField("tax", "FLOAT"),
        SchemaField("order_total", "FLOAT"),
        SchemaField("status", "STRING"),
        SchemaField("is_first_order", "BOOLEAN"),
    ],
    "order_items": [
        SchemaField("item_id", "STRING"),
        SchemaField("order_id", "STRING"),
        SchemaField("customer_id", "STRING"),
        SchemaField("product_id", "STRING"),
        SchemaField("product_name", "STRING"),
        SchemaField("category", "STRING"),
        SchemaField("quantity", "INTEGER"),
        SchemaField("unit_price", "FLOAT"),
        SchemaField("discount_pct", "FLOAT"),
        SchemaField("line_total", "FLOAT"),
        SchemaField("cogs", "FLOAT"),
    ],
    "sessions": [
        SchemaField("session_id", "STRING"),
        SchemaField("session_date", "DATE"),
        SchemaField("channel", "STRING"),
        SchemaField("device", "STRING"),
        SchemaField("landing_page", "STRING"),
        SchemaField("pages_viewed", "INTEGER"),
        SchemaField("duration_seconds", "INTEGER"),
        SchemaField("bounced", "BOOLEAN"),
        SchemaField("utm_source", "STRING"),
        SchemaField("utm_medium", "STRING"),
        SchemaField("utm_campaign", "STRING"),
        SchemaField("converted", "BOOLEAN"),
        SchemaField("order_id", "STRING"),
    ],
    "ad_spend": [
        SchemaField("spend_id", "STRING"),
        SchemaField("date", "DATE"),
        SchemaField("platform", "STRING"),
        SchemaField("campaign_id", "STRING"),
        SchemaField("campaign_name", "STRING"),
        SchemaField("impressions", "INTEGER"),
        SchemaField("clicks", "INTEGER"),
        SchemaField("ctr", "FLOAT"),
        SchemaField("spend", "FLOAT"),
        SchemaField("conversions", "INTEGER"),
        SchemaField("conversion_value", "FLOAT"),
        SchemaField("roas", "FLOAT"),
    ],
    "email_events": [
        SchemaField("event_id", "STRING"),
        SchemaField("customer_id", "STRING"),
        SchemaField("email", "STRING"),
        SchemaField("flow", "STRING"),
        SchemaField("email_number", "INTEGER"),
        SchemaField("send_date", "DATE"),
        SchemaField("sent", "BOOLEAN"),
        SchemaField("opened", "BOOLEAN"),
        SchemaField("clicked", "BOOLEAN"),
        SchemaField("converted", "BOOLEAN"),
        SchemaField("unsubscribed", "BOOLEAN"),
        SchemaField("subject_line", "STRING"),
    ],
}

# Table descriptions
TABLE_DESCRIPTIONS = {
    "customers": "Customer master data",
    "orders": "Order header records",
    "order_items": "Order line items",
    "sessions": "Website session data",
    "ad_spend": "Daily ad spend by platform and campaign",
    "email_events": "Email send and engagement events",
}

# Time partitioning configuration
TIME_PARTITIONING = {
    "orders": ("order_date", "MONTH"),
    "sessions": ("session_date", "MONTH"),
    "ad_spend": ("date", "MONTH"),
    "email_events": ("send_date", "MONTH"),
}


def create_dataset(client, project_id):
    """Create the BigQuery dataset."""
    print(f"[1/3] Creating dataset '{DATASET}'...")
    
    dataset_ref = client.dataset(DATASET)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = LOCATION
    dataset.description = "NovaBrew Coffee — Raw data layer"
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print("  ✓ Dataset created or already exists.")
    except Exception as e:
        print(f"  ✗ Error creating dataset: {e}")
        sys.exit(1)


def create_tables(client, project_id):
    """Create all tables in the dataset."""
    print(f"\n[2/3] Creating tables...")
    
    dataset_ref = client.dataset(DATASET)
    
    for table_name, schema in TABLE_SCHEMAS.items():
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        table.description = TABLE_DESCRIPTIONS[table_name]
        
        # Add time partitioning if configured
        if table_name in TIME_PARTITIONING:
            field, partition_type = TIME_PARTITIONING[table_name]
            table.time_partitioning = TimePartitioning(
                type_=partition_type,
                field=field
            )
        
        try:
            client.create_table(table, exists_ok=True)
            print(f"  ✓ Created table '{table_name}'")
        except Exception as e:
            print(f"  ✗ Error creating table '{table_name}': {e}")
            sys.exit(1)
    
    print(f"  ✓ All {len(TABLE_SCHEMAS)} tables created or replaced.")


def load_data(client, project_id):
    """Load CSV data into tables."""
    print(f"\n[3/3] Loading CSV data...")
    
    dataset_ref = client.dataset(DATASET)
    
    for table_name in TABLE_SCHEMAS.keys():
        csv_path = os.path.join(DATA_DIR, f"{table_name}.csv")
        
        if not os.path.exists(csv_path):
            print(f"  ⚠ Warning: CSV file not found: {csv_path}")
            continue
        
        print(f"  Loading {table_name}...")
        
        table_ref = dataset_ref.table(table_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        try:
            with open(csv_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
                job.result()  # Wait for the job to complete
                print(f"    ✓ Loaded {table_name}")
        except Exception as e:
            print(f"    ✗ Error loading {table_name}: {e}")
            sys.exit(1)


def validate_data(client, project_id):
    """Validate row counts after loading."""
    print(f"\n[Validation] Verifying row counts...")
    
    # Construct a query to count rows for all tables
    queries = []
    for table in TABLE_SCHEMAS.keys():
        queries.append(f"SELECT '{table}' as table_name, COUNT(*) as n FROM `{project_id}.{DATASET}.{table}`")
    
    query = "\nUNION ALL\n".join(queries) + "\nORDER BY 1"
    
    try:
        results = client.query(query).result()
        print(f"  {'Table':<15} | {'Rows':>10}")
        print(f"  {'-'*15}-+-{'-'*10}")
        for row in results:
            print(f"  {row.table_name:<15} | {row.n:>10,}")
    except Exception as e:
        print(f"  ✗ Validation failed: {e}")


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Error: Please provide your Google Cloud Project ID as an argument.")
        print("Usage: python setup_bigquery.py YOUR_PROJECT_ID [SERVICE_ACCOUNT_KEY_PATH]")
        sys.exit(1)
    
    project_id = sys.argv[1]
    credentials = None
    
    # Check for service account key file
    if len(sys.argv) >= 3:
        from google.oauth2 import service_account
        key_path = sys.argv[2]
        print(f"Using service account key: {key_path}")
        try:
            credentials = service_account.Credentials.from_service_account_file(key_path)
        except Exception as e:
            print(f"Error loading service account key: {e}")
            sys.exit(1)
    
    print()
    print("🟤 NovaBrew Coffee — BigQuery Setup")
    print("====================================")
    print(f"  Project:  {project_id}")
    print(f"  Dataset:  {DATASET}")
    print(f"  Location: {LOCATION}")
    print()
    
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id, credentials=credentials)
    
    # Step 1: Create dataset
    create_dataset(client, project_id)
    
    # Step 2: Create tables
    create_tables(client, project_id)
    
    # Step 3: Load data
    load_data(client, project_id)
    
    # Step 4: Validate
    validate_data(client, project_id)
    
    print()
    print("====================================")
    print("✅ Setup complete!")
    print()
    print("Verify with:")
    print(f"  gcloud bq tables list --project={project_id} --dataset={DATASET}")
    print()
    print("Next: Run dbt to build the staging layer.")
    print("  See /dbt/ directory for models.")
    print("====================================")


if __name__ == "__main__":
    main()
