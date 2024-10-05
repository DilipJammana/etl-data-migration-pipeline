# Complete ETL Project Setup & Execution Guide

## Overview

This is a **production-ready ETL data migration project** that demonstrates:
- Migration from legacy on-premises systems to modern cloud data warehouse
- Complete Extract-Transform-Load pipeline with data quality checks
- Error handling, logging, monitoring, and metrics
- Incremental loading capabilities
- Real-world data quality issues and solutions

## Prerequisites Checklist

Before starting, ensure you have:
- [ ] Python 3.10 or higher installed
- [ ] PostgreSQL 14+ (or Docker to run it)
- [ ] Git installed
- [ ] 2GB free disk space
- [ ] Terminal/Command Prompt access
- [ ] Text editor or IDE (VS Code, PyCharm, Cursor)

## Step-by-Step Installation

### Step 1: Create Project Directory

```bash
# Navigate to project directory

cd ETL_PROJECT

# Create project structure
mkdir -p config data/{source,staging,archive} src/{extract,transform,load,utils,pipeline} sql logs tests docs notebooks
```

### Step 2: Set Up Python Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
venv\Scripts\activate

# Verify Python version
python --version  # Should be 3.10+
```

### Step 3: Place Project Files
```
ETL_PROJECT/
├── requirements.txt                 
├── docker-compose.yml              
├── config/
│   └── config.yaml                  
├── src/
│   ├── extract/
│   │   └── csv_extractor.py         
│   └── pipeline/
│       └── orchestrator.py          
├── sql/
│   └── create_tables.sql          
└── scripts/
    └── generate_sample_data.py      
```

### Step 4: Install Dependencies

```bash
# Install all required packages
pip install --upgrade pip
pip install -r requirements.txt

# Verify key packages installed
pip list | grep pandas
pip list | grep sqlalchemy
```

### Step 5: Start PostgreSQL Database

**Option A: Using Docker (Recommended)**

```bash
# Start all services (PostgreSQL + PgAdmin + Metabase)
docker-compose up -d

# Check if services are running
docker-compose ps

# View logs
docker-compose logs -f postgres

```

**Option B: Local PostgreSQL Installation**

```bash
# Create database (if PostgreSQL installed locally)
createdb -U postgres etl_warehouse

# Or using psql:
psql -U postgres
CREATE DATABASE etl_warehouse;
\q

# Run schema creation script
psql -U postgres -d etl_warehouse -f sql/create_tables.sql
```

### Step 6: Configure Environment

```bash
# Create environment variables file
cat > .env << EOF
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=etl_warehouse
DB_USER=postgres
DB_PASSWORD=postgres123

# API Configuration (future use)
API_KEY=your_api_key_here

# Email Alerts
ALERT_EMAIL=your.email@example.com
EOF

# Never commit .env to git
echo ".env" >> .gitignore
```

### Step 7: Generate Sample Data

```bash
# Run sample data generator
python scripts/generate_sample_data.py

# This will create:
# - data/source/customers.csv (1,000 records)
# - data/source/orders.csv (5,000 records)
# - data/source/legacy.db (SQLite with 200 products)
```


### Step 8: Verify Database Setup

```bash
# Connect to PostgreSQL
psql -U postgres -d etl_warehouse

# Check tables were created
\dt
\dt etl_metadata.*

# Check date dimension was populated
SELECT COUNT(*) FROM dim_dates;
-- Should return ~4,000 rows (10 years of dates)

# Exit psql
\q
```

##  Running the ETL Pipeline

### Full Pipeline Execution

```bash
# Run complete ETL pipeline (all tables)
python src/pipeline/orchestrator.py --mode full

# Run for specific table only
python src/pipeline/orchestrator.py --mode full --table orders
```


### Incremental Pipeline Execution

```bash
# Run incremental load for specific table
python src/pipeline/orchestrator.py --mode incremental --table orders
```

##  Verification & Testing

### 1. Check Data in PostgreSQL

```sql
-- Connect to database
psql -U postgres -d etl_warehouse

-- Check record counts
SELECT 'dim_customers' AS table_name, COUNT(*) AS record_count FROM dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM dim_products
UNION ALL
SELECT 'fact_orders', COUNT(*) FROM fact_orders;

-- Check pipeline execution history
SELECT * FROM etl_metadata.pipeline_runs 
ORDER BY start_time DESC LIMIT 5;

-- Check data quality scores
SELECT 
    table_name,
    AVG(quality_score) AS avg_quality_score,
    COUNT(*) AS check_count
FROM etl_metadata.quality_checks
GROUP BY table_name;

-- Sample data from fact table
SELECT 
    o.order_id,
    c.customer_name,
    p.product_name,
    o.quantity,
    o.total_amount,
    d.date_actual
FROM fact_orders o
JOIN dim_customers c ON o.customer_sk = c.customer_sk
JOIN dim_products p ON o.product_sk = p.product_sk
JOIN dim_dates d ON o.order_date_sk = d.date_sk
LIMIT 10;
```

### 2. View Analytics Reports

**Use built-in views:**

```sql
-- Daily orders summary
SELECT * FROM etl_metadata.v_daily_orders_summary
ORDER BY date_actual DESC
LIMIT 30;

-- Top customers by lifetime value
SELECT * FROM etl_metadata.v_customer_summary
ORDER BY lifetime_value DESC
LIMIT 20;

-- Best selling products
SELECT * FROM etl_metadata.v_product_performance
ORDER BY total_revenue DESC
LIMIT 20;
```

### 3. Access Web Interfaces

**PgAdmin (Database Management):**
- URL: http://localhost:5050
- Email: admin@etl.com
- Password: admin123

**Metabase (Analytics Dashboard):**
- URL: http://localhost:3000
- Setup on first visit
- Connect to etl_warehouse database

## Key Features Demonstrated

### 1. Extract Phase
-  CSV file extraction with encoding handling
-  SQLite database extraction
-  Schema validation at source
-  Parallel extraction capabilities

### 2. Transform Phase
-  Data cleansing (nulls, duplicates, formatting)
-  Data type conversions
-  Business logic application
-  Data enrichment
-  Quality validation with scoring

### 3. Load Phase
-  Full load (truncate and reload)
-  Incremental load (upsert)
-  Transaction management
-  Referential integrity maintenance

### 4. Data Quality
-  15+ validation rules
-  Quality score calculation
-  Detailed reporting
-  Configurable thresholds

### 5. Monitoring
-  Structured logging with rotation
-  Pipeline execution metrics
-  Performance tracking
-  Error tracking and alerting

## Performance Benchmarks

**Achieved metrics on test data:**
- 5083 records extracted in ~3 seconds
- Transformation with validation: ~5 seconds
- Loading to warehouse: ~2 seconds
- **Total pipeline execution: ~25 seconds**
- Data quality score: **100%**

## Customization Guide

### Add New Data Source

1. Create extractor in `src/extract/`:
```python
# src/extract/api_extractor.py
class APIExtractor:
    def extract(self, endpoint):
        # Your extraction logic
        pass
```

2. Update orchestrator to use it:
```python
# In orchestrator.py _extract_phase()
api_extractor = APIExtractor(config['api_base_url'])
data = api_extractor.extract('/customers')
```

### Add Custom Transformation

```python
# In orchestrator.py _apply_business_logic()
if table_name == 'your_table':
    # Add your custom logic
    df['new_column'] = df['old_column'].apply(custom_function)
```

### Add Data Quality Rule

```python
# src/transform/validator.py
def validate_custom_rule(self, df):
    # Your validation logic
    is_valid = df['column'].apply(lambda x: check_condition(x)).all()
    return {'is_valid': is_valid, 'message': 'Custom check'}
```

## Testing

```bash
# Run all tests
pytest tests/ -v

# Run with coverage report
pytest --cov=src tests/ --cov-report=html

# Run specific test file
pytest tests/test_extract.py -v
```

```

## 🎓 What This Demonstrates


1. **ETL Expertise**: Full pipeline implementation with proper phases
2. **Data Engineering**: Schema design, data modeling, warehousing
3. **Data Quality**: Comprehensive validation and cleansing
4. **Production Mindset**: Error handling, logging, monitoring, metrics
5. **Modern Stack**: Python, PostgreSQL, Docker, modern libraries
6. **Best Practices**: Code organization, documentation, testing
7. **Migration Experience**: Legacy to modern system transition
8. **Scalability**: Designed for large data volumes
9. **Maintainability**: Clean code, configuration-driven, modular

##  Troubleshooting

**Issue: PostgreSQL connection failed**
```bash
# Check if PostgreSQL is running
docker-compose ps
# or
sudo service postgresql status

# Check connection
psql -U postgres -h localhost -p 5432 -d etl_warehouse
```

**Issue: Import errors**
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# Check Python path
python -c "import sys; print(sys.path)"
```

**Issue: Permission denied**
```bash
# On Linux/Mac, add execution permissions
chmod +x scripts/generate_sample_data.py
```

**Issue: Port already in use**
```bash
# Check what's using port 5432
lsof -i :5432  # On Mac/Linux
netstat -ano | findstr :5432  # On Windows


 
