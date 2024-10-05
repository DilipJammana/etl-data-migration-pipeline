"""
Generate sample data for ETL pipeline testing
Creates CSV files and SQLite database with realistic e-commerce data
"""

import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta
import random
import numpy as np

# Set random seed for reproducibility
random.seed(42)
np.random.seed(42)


def generate_customers(num_customers=1000):
    """Generate sample customer data"""
    print(f"Generating {num_customers} customers...")
    
    first_names = ['John', 'Jane', 'Michael', 'Emily', 'David', 'Sarah', 'Robert', 'Lisa', 
                   'William', 'Jennifer', 'James', 'Mary', 'Christopher', 'Patricia', 'Daniel']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 
                  'Davis', 'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Wilson']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
              'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']
    
    customers = []
    for i in range(num_customers):
        customer_id = f"CUST{i+1:06d}"
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        customer_name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        phone = f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"
        
        city_idx = random.randint(0, len(cities)-1)
        city = cities[city_idx]
        state = states[city_idx]
        
        registration_date = datetime.now() - timedelta(days=random.randint(1, 1000))
        customer_segment = random.choice(['Premium', 'Standard', 'Basic'])
        
        customers.append({
            'customer_id': customer_id,
            'customer_name': customer_name,
            'email': email,
            'phone': phone,
            'address': f"{random.randint(100, 9999)} Main St",
            'city': city,
            'state': state,
            'zip_code': f"{random.randint(10000, 99999)}",
            'country': 'USA',
            'customer_segment': customer_segment,
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'last_purchase_date': None  # Will be updated based on orders
        })
    
    df = pd.DataFrame(customers)
    return df


def generate_products(num_products=200):
    """Generate sample product data"""
    print(f"Generating {num_products} products...")
    
    categories = {
        'Electronics': ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Camera'],
        'Clothing': ['Shirt', 'Pants', 'Dress', 'Shoes', 'Jacket'],
        'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Bedding', 'Tools'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children', 'Comics']
    }
    
    brands = ['TechCorp', 'FashionHub', 'HomeStyle', 'BookWorld', 'GenericBrand']
    suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E']
    
    products = []
    for i in range(num_products):
        product_id = f"PROD{i+1:06d}"
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        
        product_name = f"{subcategory} - Model {i+1}"
        brand = random.choice(brands)
        supplier = random.choice(suppliers)
        
        unit_cost = round(random.uniform(10, 500), 2)
        margin = random.uniform(0.2, 0.5)  # 20-50% margin
        unit_price = round(unit_cost * (1 + margin), 2)
        margin_percent = round(margin * 100, 2)
        
        created_at = datetime.now() - timedelta(days=random.randint(30, 365))
        
        products.append({
            'product_id': product_id,
            'product_name': product_name,
            'product_category': category,
            'product_subcategory': subcategory,
            'brand': brand,
            'supplier': supplier,
            'unit_cost': unit_cost,
            'unit_price': unit_price,
            'margin_percent': margin_percent,
            'is_active': random.choice([True, True, True, False]),  # 75% active
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    df = pd.DataFrame(products)
    return df


def generate_orders(customers_df, products_df, num_orders=5000):
    """Generate sample order data"""
    print(f"Generating {num_orders} orders...")
    
    orders = []
    order_statuses = ['Completed', 'Completed', 'Completed', 'Pending', 'Shipped', 'Cancelled']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']
    shipping_methods = ['Standard', 'Express', 'Overnight']
    
    active_products = products_df[products_df['is_active'] == True]
    
    for i in range(num_orders):
        order_id = f"ORD{i+1:08d}"
        customer = customers_df.sample(n=1).iloc[0]
        product = active_products.sample(n=1).iloc[0]
        
        order_date = datetime.now() - timedelta(days=random.randint(1, 365))
        ship_date = order_date + timedelta(days=random.randint(1, 7))
        
        quantity = random.randint(1, 10)
        unit_price = product['unit_price']
        subtotal = quantity * unit_price
        
        discount_amount = 0
        if random.random() < 0.3:  # 30% chance of discount
            discount_amount = round(subtotal * random.uniform(0.05, 0.2), 2)
        
        tax_rate = 0.08  # 8% tax
        tax_amount = round((subtotal - discount_amount) * tax_rate, 2)
        total_amount = round(subtotal - discount_amount + tax_amount, 2)
        
        orders.append({
            'order_id': order_id,
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'order_date': order_date.strftime('%Y-%m-%d'),
            'ship_date': ship_date.strftime('%Y-%m-%d') if random.random() > 0.1 else None,
            'quantity': quantity,
            'unit_price': unit_price,
            'discount_amount': discount_amount,
            'tax_amount': tax_amount,
            'total_amount': total_amount,
            'order_status': random.choice(order_statuses),
            'payment_method': random.choice(payment_methods),
            'shipping_method': random.choice(shipping_methods)
        })
    
    df = pd.DataFrame(orders)
    return df


def introduce_data_quality_issues(df, issue_rate=0.05):
    """Introduce realistic data quality issues for testing"""
    print(f"Introducing {issue_rate*100}% data quality issues...")
    
    df_copy = df.copy()
    num_issues = int(len(df) * issue_rate)
    
    # Introduce null values
    for _ in range(num_issues // 3):
        row = random.randint(0, len(df_copy)-1)
        col = random.choice(df_copy.columns)
        df_copy.at[row, col] = None
    
    # Introduce duplicates (for some tables)
    if 'order_id' in df_copy.columns:
        for _ in range(num_issues // 3):
            row = random.randint(0, len(df_copy)-1)
            df_copy = pd.concat([df_copy, df_copy.iloc[[row]]], ignore_index=True)
    
    # Introduce formatting issues
    if 'email' in df_copy.columns:
        for _ in range(num_issues // 3):
            row = random.randint(0, len(df_copy)-1)
            df_copy.at[row, 'email'] = 'invalid-email'
    
    return df_copy


def main():
    """Main function to generate all sample data"""
    print("=" * 60)
    print("ETL Sample Data Generator")
    print("=" * 60)
    
    # Create directories
    source_dir = Path("data/source")
    source_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate data
    customers_df = generate_customers(num_customers=1000)
    products_df = generate_products(num_products=200)
    orders_df = generate_orders(customers_df, products_df, num_orders=5000)
    
    # Introduce some data quality issues (5%)
    customers_df = introduce_data_quality_issues(customers_df, issue_rate=0.05)
    orders_df = introduce_data_quality_issues(orders_df, issue_rate=0.05)
    
    # Save to CSV files
    print("\nSaving CSV files...")
    customers_df.to_csv(source_dir / "customers.csv", index=False)
    print(f"✓ Saved customers.csv ({len(customers_df)} records)")
    
    orders_df.to_csv(source_dir / "orders.csv", index=False)
    print(f"✓ Saved orders.csv ({len(orders_df)} records)")
    
    # Save products to SQLite database
    print("\nCreating SQLite database...")
    db_path = source_dir / "legacy.db"
    conn = sqlite3.connect(db_path)
    
    products_df.to_sql('products', conn, if_exists='replace', index=False)
    print(f"✓ Saved products to legacy.db ({len(products_df)} records)")
    
    conn.close()
    
    # Print summary
    print("\n" + "=" * 60)
    print("Data Generation Complete!")
    print("=" * 60)
    print(f"Customers: {len(customers_df)} records → data/source/customers.csv")
    print(f"Products:  {len(products_df)} records → data/source/legacy.db")
    print(f"Orders:    {len(orders_df)} records → data/source/orders.csv")
    print("\nNote: ~5% data quality issues introduced for testing")
    print("=" * 60)


if __name__ == "__main__":
    main()
