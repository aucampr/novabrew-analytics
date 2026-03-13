@echo off
:: ============================================================
:: NovaBrew Coffee — BigQuery Setup Script (Windows)
:: Post 1: Building the Data Foundation
:: ============================================================
:: Prerequisites:
::   - Google Cloud SDK installed: https://cloud.google.com/sdk/docs/install
::   - Run: gcloud auth login
::   - Run: gcloud config set project YOUR_PROJECT_ID
::
:: Usage (from Command Prompt):
::   cd data_generation
::   setup_bigquery.bat YOUR_PROJECT_ID
:: ============================================================

set "PROJECT_ID=%~1"
if "%PROJECT_ID%"=="" (
    echo "Error: Please provide your Google Cloud Project ID as an argument."
    echo "Usage: setup_bigquery.bat YOUR_PROJECT_ID"
    exit /b 1
)

:: Set Python to use the bundled version to avoid absl-py conflicts
set "CLOUDSDK_PYTHON=C:\Program Files (x86)\Google\Cloud SDK\google-cloud-sdk\platform\bundledpython\python.exe"

set "DATASET=novabrew_raw"
set "LOCATION=US"
set "DATA_DIR=./output"

echo.
echo 🟤 NovaBrew Coffee — BigQuery Setup
echo ====================================
echo   Project:  %PROJECT_ID%
echo   Dataset:  %DATASET%
echo   Location: %LOCATION%
echo.

:: ── Step 1: Create Dataset ─────────────────────────────────
echo [1/3] Creating dataset '%DATASET%'...
bq --project_id="%PROJECT_ID%" mk --dataset --location="%LOCATION%" --description="NovaBrew Coffee — Raw data layer" "%PROJECT_ID%:%DATASET%"
if errorlevel 1 (
    echo   ✗ Error creating dataset.
    echo   Run 'bq --version' to verify bq tool is working.
    echo   If you see 'absl.flags' error, run: gcloud components update
    exit /b 1
)
echo   ✓ Dataset created or already exists.
echo.

:: ── Step 2: Create Tables ──────────────────────────────────
echo [2/3] Creating tables...

bq --project_id="%PROJECT_ID%" mk --replace --table --description="Customer master data" "%PROJECT_ID%:%DATASET%.customers" "customer_id:STRING,first_name:STRING,last_name:STRING,email:STRING,state:STRING,acquisition_channel:STRING,signup_date:DATE,is_subscriber:BOOLEAN,email_opt_in:BOOLEAN,date_of_birth:DATE"
bq --project_id="%PROJECT_ID%" mk --replace --table --description="Order header records" --time_partitioning_field="order_date" --time_partitioning_type="MONTH" "%PROJECT_ID%:%DATASET%.orders" "order_id:STRING,customer_id:STRING,order_date:DATE,shipped_date:DATE,delivered_date:DATE,channel:STRING,promo_code:STRING,discount_pct:FLOAT64,subtotal:FLOAT64,shipping_cost:FLOAT64,tax:FLOAT64,order_total:FLOAT64,status:STRING,is_first_order:BOOLEAN"
bq --project_id="%PROJECT_ID%" mk --replace --table --description="Order line items" "%PROJECT_ID%:%DATASET%.order_items" "item_id:STRING,order_id:STRING,customer_id:STRING,product_id:STRING,product_name:STRING,category:STRING,quantity:INT64,unit_price:FLOAT64,discount_pct:FLOAT64,line_total:FLOAT64,cogs:FLOAT64"
bq --project_id="%PROJECT_ID%" mk --replace --table --description="Website session data" --time_partitioning_field="session_date" --time_partitioning_type="MONTH" "%PROJECT_ID%:%DATASET%.sessions" "session_id:STRING,session_date:DATE,channel:STRING,device:STRING,landing_page:STRING,pages_viewed:INT64,duration_seconds:INT64,bounced:BOOLEAN,utm_source:STRING,utm_medium:STRING,utm_campaign:STRING,converted:BOOLEAN,order_id:STRING"
bq --project_id="%PROJECT_ID%" mk --replace --table --description="Daily ad spend by platform and campaign" --time_partitioning_field="date" --time_partitioning_type="MONTH" "%PROJECT_ID%:%DATASET%.ad_spend" "spend_id:STRING,date:DATE,platform:STRING,campaign_id:STRING,campaign_name:STRING,impressions:INT64,clicks:INT64,ctr:FLOAT64,spend:FLOAT64,conversions:INT64,conversion_value:FLOAT64,roas:FLOAT64"
bq --project_id="%PROJECT_ID%" mk --replace --table --description="Email send and engagement events" --time_partitioning_field="send_date" --time_partitioning_type="MONTH" "%PROJECT_ID%:%DATASET%.email_events" "event_id:STRING,customer_id:STRING,email:STRING,flow:STRING,email_number:INT64,send_date:DATE,sent:BOOLEAN,opened:BOOLEAN,clicked:BOOLEAN,converted:BOOLEAN,unsubscribed:BOOLEAN,subject_line:STRING"

echo   ✓ All 6 tables created or replaced.
echo.

:: ── Step 3: Load Data ──────────────────────────────────────
echo [3/3] Loading CSV data...

FOR %%T IN (customers orders order_items sessions ad_spend email_events) DO (
  echo   Loading %%T...
  bq --project_id="%PROJECT_ID%" load --source_format=CSV --skip_leading_rows=1 --replace "%PROJECT_ID%:%DATASET%.%%T" "%DATA_DIR%/%%T.csv"
)

:: ── Step 4: Validation ─────────────────────────────────────
echo [4/4] Validating row counts...

bq --project_id="%PROJECT_ID%" query --nouse_legacy_sql "SELECT 'customers' as tbl, COUNT(*) as n FROM `%PROJECT_ID%.%DATASET%.customers` UNION ALL SELECT 'orders', COUNT(*) FROM `%PROJECT_ID%.%DATASET%.orders` UNION ALL SELECT 'sessions', COUNT(*) FROM `%PROJECT_ID%.%DATASET%.sessions` ORDER BY 1"

echo.
echo ====================================
echo ✅ Setup complete!
echo.
echo Verify with:
echo   bq --project_id=%PROJECT_ID% ls %DATASET%
echo   bq --project_id=%PROJECT_ID% query "SELECT COUNT(*) FROM `%PROJECT_ID%.%DATASET%.orders`"
echo.
echo Next: Run dbt to build the staging layer.
echo   See /dbt/ directory for models.
echo ====================================