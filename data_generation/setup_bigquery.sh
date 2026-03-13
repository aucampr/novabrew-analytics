#!/bin/bash
# ============================================================
# NovaBrew Coffee — BigQuery Setup Script
# Post 1: Building the Data Foundation
# ============================================================
# Prerequisites:
#   - Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
#   - Run: gcloud auth login
#   - Run: gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   chmod +x setup_bigquery.sh
#   ./setup_bigquery.sh YOUR_PROJECT_ID
# ============================================================

# Exit script if any command fails
set -e

PROJECT_ID=${1:-"your-project-id"}
DATASET="novabrew_raw"
LOCATION="US"
DATA_DIR="./output"

echo ""
echo "🟤 NovaBrew Coffee — BigQuery Setup"
echo "===================================="
echo "  Project:  $PROJECT_ID"
echo "  Dataset:  $DATASET"
echo "  Location: $LOCATION"
echo ""

# ── Step 1: Create Dataset ─────────────────────────────────
echo "[1/3] Creating dataset '$DATASET'..."
bq --project_id="$PROJECT_ID" mk \
  --dataset \
  --location="$LOCATION" \
  --description="NovaBrew Coffee — Raw data layer" \
  "$PROJECT_ID:$DATASET"

echo "  ✓ Dataset created"
echo ""

# ── Step 2: Create Tables ──────────────────────────────────
echo "[2/3] Creating tables..."

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Customer master data" \
  "$PROJECT_ID:$DATASET.customers" \
  "customer_id:STRING,first_name:STRING,last_name:STRING,email:STRING,state:STRING,acquisition_channel:STRING,signup_date:DATE,is_subscriber:BOOLEAN,email_opt_in:BOOLEAN,date_of_birth:DATE"

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Order header records" \
  --time_partitioning_field="order_date" \
  --time_partitioning_type="MONTH" \
  "$PROJECT_ID:$DATASET.orders" \
  "order_id:STRING,customer_id:STRING,order_date:DATE,shipped_date:DATE,delivered_date:DATE,channel:STRING,promo_code:STRING,discount_pct:FLOAT64,subtotal:FLOAT64,shipping_cost:FLOAT64,tax:FLOAT64,order_total:FLOAT64,status:STRING,is_first_order:BOOLEAN"

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Order line items" \
  "$PROJECT_ID:$DATASET.order_items" \
  "item_id:STRING,order_id:STRING,customer_id:STRING,product_id:STRING,product_name:STRING,category:STRING,quantity:INT64,unit_price:FLOAT64,discount_pct:FLOAT64,line_total:FLOAT64,cogs:FLOAT64"

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Website session data" \
  --time_partitioning_field="session_date" \
  --time_partitioning_type="MONTH" \
  "$PROJECT_ID:$DATASET.sessions" \
  "session_id:STRING,session_date:DATE,channel:STRING,device:STRING,landing_page:STRING,pages_viewed:INT64,duration_seconds:INT64,bounced:BOOLEAN,utm_source:STRING,utm_medium:STRING,utm_campaign:STRING,converted:BOOLEAN,order_id:STRING"

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Daily ad spend by platform and campaign" \
  --time_partitioning_field="date" \
  --time_partitioning_type="MONTH" \
  "$PROJECT_ID:$DATASET.ad_spend" \
  "spend_id:STRING,date:DATE,platform:STRING,campaign_id:STRING,campaign_name:STRING,impressions:INT64,clicks:INT64,ctr:FLOAT64,spend:FLOAT64,conversions:INT64,conversion_value:FLOAT64,roas:FLOAT64"

bq --project_id="$PROJECT_ID" mk \
  --table \
  --description="Email send and engagement events" \
  --time_partitioning_field="send_date" \
  --time_partitioning_type="MONTH" \
  "$PROJECT_ID:$DATASET.email_events" \
  "event_id:STRING,customer_id:STRING,email:STRING,flow:STRING,email_number:INT64,send_date:DATE,sent:BOOLEAN,opened:BOOLEAN,clicked:BOOLEAN,converted:BOOLEAN,unsubscribed:BOOLEAN,subject_line:STRING"

echo "  ✓ All 6 tables created"
echo ""

# ── Step 3: Load Data ──────────────────────────────────────
echo "[3/3] Loading CSV data..."

for table in customers orders order_items sessions ad_spend email_events; do
  echo "  Loading $table..."
  bq --project_id="$PROJECT_ID" load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    "$PROJECT_ID:$DATASET.$table" \
    "$DATA_DIR/${table}.csv"
done

echo ""
echo "===================================="
echo "✅ Setup complete!"
echo ""
echo "Verify with:"
echo "  bq --project_id=$PROJECT_ID ls $DATASET"
echo "  bq --project_id=$PROJECT_ID query 'SELECT COUNT(*) FROM \`$PROJECT_ID.$DATASET.orders\`'"
echo ""
echo "Next: Run dbt to build the staging layer."
echo "  See /dbt/ directory for models."
echo "===================================="
