-- PostgreSQL Data Warehouse Schema
-- Creates target tables for ETL pipeline

-- Drop existing tables if they exist
DROP TABLE IF EXISTS fact_orders CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;
DROP SCHEMA IF EXISTS etl_metadata CASCADE;

-- Create metadata schema for pipeline tracking
CREATE SCHEMA etl_metadata;

-- ======================================================================
-- METADATA TABLES
-- ======================================================================

-- Pipeline execution tracking
CREATE TABLE etl_metadata.pipeline_runs (
    run_id VARCHAR(100) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(50) NOT NULL,
    records_extracted INTEGER,
    records_transformed INTEGER,
    records_loaded INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data quality checks
CREATE TABLE etl_metadata.quality_checks (
    check_id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) REFERENCES etl_metadata.pipeline_runs(run_id),
    table_name VARCHAR(100) NOT NULL,
    check_name VARCHAR(100) NOT NULL,
    check_result VARCHAR(20) NOT NULL,
    quality_score DECIMAL(5,2),
    records_checked INTEGER,
    records_failed INTEGER,
    check_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    details JSONB
);

-- Incremental load tracking
CREATE TABLE etl_metadata.load_checkpoints (
    checkpoint_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_loaded_timestamp TIMESTAMP NOT NULL,
    last_loaded_id BIGINT,
    records_loaded INTEGER,
    load_type VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ======================================================================
-- DIMENSION TABLES
-- ======================================================================

-- Customer Dimension (Type 2 SCD - Slowly Changing Dimension)
CREATE TABLE dim_customers (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200) NOT NULL,
    email VARCHAR(200),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    registration_date DATE,
    -- SCD Type 2 fields
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_load_date DATE DEFAULT CURRENT_DATE
);

CREATE INDEX idx_dim_customers_id ON dim_customers(customer_id);
CREATE INDEX idx_dim_customers_email ON dim_customers(email);
CREATE INDEX idx_dim_customers_current ON dim_customers(is_current) WHERE is_current = TRUE;

-- Product Dimension (Type 1 SCD)
CREATE TABLE dim_products (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(200) NOT NULL,
    product_category VARCHAR(100),
    product_subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier VARCHAR(100),
    unit_cost DECIMAL(12,2),
    unit_price DECIMAL(12,2),
    margin_percent DECIMAL(5,2),
    is_active BOOLEAN DEFAULT TRUE,
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_load_date DATE DEFAULT CURRENT_DATE
);

CREATE INDEX idx_dim_products_id ON dim_products(product_id);
CREATE INDEX idx_dim_products_category ON dim_products(product_category);
CREATE INDEX idx_dim_products_active ON dim_products(is_active) WHERE is_active = TRUE;

-- Date Dimension (pre-populated)
CREATE TABLE dim_dates (
    date_sk INTEGER PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    holiday_name VARCHAR(100)
);

-- ======================================================================
-- FACT TABLES
-- ======================================================================

-- Orders Fact Table
CREATE TABLE fact_orders (
    order_sk SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_date_sk INTEGER NOT NULL REFERENCES dim_dates(date_sk),
    customer_sk INTEGER NOT NULL REFERENCES dim_customers(customer_sk),
    product_sk INTEGER NOT NULL REFERENCES dim_products(product_sk),
    -- Measures
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(12,2) NOT NULL,
    discount_amount DECIMAL(12,2) DEFAULT 0,
    tax_amount DECIMAL(12,2) DEFAULT 0,
    total_amount DECIMAL(12,2) NOT NULL,
    -- Degenerate dimensions
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    shipping_method VARCHAR(50),
    -- Audit fields
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_load_date DATE DEFAULT CURRENT_DATE
);

CREATE INDEX idx_fact_orders_order_id ON fact_orders(order_id);
CREATE INDEX idx_fact_orders_date ON fact_orders(order_date_sk);
CREATE INDEX idx_fact_orders_customer ON fact_orders(customer_sk);
CREATE INDEX idx_fact_orders_product ON fact_orders(product_sk);
CREATE INDEX idx_fact_orders_load_date ON fact_orders(etl_load_date);

-- ======================================================================
-- POPULATE DATE DIMENSION
-- ======================================================================

-- Function to populate date dimension
-- Populate date dimension (run once at init)
DO $$
DECLARE
    current_date DATE := DATE '2020-01-01';
    end_date     DATE := DATE '2030-12-31';
BEGIN
    TRUNCATE TABLE dim_dates;
    
    WHILE current_date <= end_date LOOP
        INSERT INTO dim_dates (
            date_sk,
            date_actual,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month,
            month_name,
            quarter,
            year,
            is_weekend
        ) VALUES (
            TO_CHAR(current_date, 'YYYYMMDD')::INTEGER,
            current_date,
            EXTRACT(DOW FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Day'),
            EXTRACT(DAY FROM current_date)::INTEGER,
            EXTRACT(DOY FROM current_date)::INTEGER,
            EXTRACT(WEEK FROM current_date)::INTEGER,
            EXTRACT(MONTH FROM current_date)::INTEGER,
            TO_CHAR(current_date, 'Month'),
            EXTRACT(QUARTER FROM current_date)::INTEGER,
            EXTRACT(YEAR FROM current_date)::INTEGER,
            EXTRACT(DOW FROM current_date) IN (0, 6)
        );

        current_date := current_date + INTERVAL '1 day';
    END LOOP;
END $$;


-- ======================================================================
-- VIEWS FOR REPORTING
-- ======================================================================

-- Daily orders summary
CREATE OR REPLACE VIEW etl_metadata.v_daily_orders_summary AS
SELECT 
    d.date_actual,
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id) AS order_count,
    COUNT(*) AS order_line_count,
    SUM(f.quantity) AS total_quantity,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.total_amount) AS avg_order_value
FROM fact_orders f
JOIN dim_dates d ON f.order_date_sk = d.date_sk
GROUP BY d.date_actual, d.year, d.month, d.month_name;

-- Customer summary
CREATE OR REPLACE VIEW etl_metadata.v_customer_summary AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.customer_segment,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS lifetime_value,
    MAX(d.date_actual) AS last_order_date
FROM dim_customers c
LEFT JOIN fact_orders f ON c.customer_sk = f.customer_sk
LEFT JOIN dim_dates d ON f.order_date_sk = d.date_sk
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.customer_name, c.email, c.customer_segment;

-- Product performance
CREATE OR REPLACE VIEW etl_metadata.v_product_performance AS
SELECT 
    p.product_id,
    p.product_name,
    p.product_category,
    p.brand,
    COUNT(DISTINCT f.order_id) AS order_count,
    SUM(f.quantity) AS units_sold,
    SUM(f.total_amount) AS total_revenue,
    p.unit_price,
    p.unit_cost,
    p.margin_percent
FROM dim_products p
LEFT JOIN fact_orders f ON p.product_sk = f.product_sk
WHERE p.is_active = TRUE
GROUP BY p.product_id, p.product_name, p.product_category, p.brand,
         p.unit_price, p.unit_cost, p.margin_percent;

-- ======================================================================
-- GRANTS (adjust based on your user setup)
-- ======================================================================

-- GRANT ALL PRIVILEGES ON SCHEMA etl_metadata TO etl_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA etl_metadata TO etl_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO etl_user;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO etl_user;

COMMENT ON SCHEMA etl_metadata IS 'Schema for ETL pipeline metadata and monitoring';
COMMENT ON TABLE dim_customers IS 'Customer dimension with Type 2 SCD for history tracking';
COMMENT ON TABLE dim_products IS 'Product dimension with Type 1 SCD';
COMMENT ON TABLE dim_dates IS 'Date dimension pre-populated for 10 years';
COMMENT ON TABLE fact_orders IS 'Orders fact table containing transactional data';
