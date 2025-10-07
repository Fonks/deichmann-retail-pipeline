"""
Shoe Retail Data Generator for Deichmann Pipeline Project
Generates realistic sample data for stores, products, customers, and sales
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create output directory
os.makedirs('data/raw', exist_ok=True)

print("🚀 Starting Shoe Retail Data Generation...")

# ============================================================================
# 1. STORES DATA
# ============================================================================
print("\n📍 Generating Stores Data...")

store_cities = [
    'Berlin', 'Hamburg', 'München', 'Köln', 'Frankfurt',
    'Stuttgart', 'Düsseldorf', 'Dortmund', 'Essen', 'Leipzig',
    'Bremen', 'Dresden', 'Hannover', 'Nürnberg', 'Duisburg',
    'Bochum', 'Wuppertal', 'Bielefeld', 'Bonn', 'Münster',
    'Karlsruhe', 'Mannheim', 'Augsburg', 'Wiesbaden', 'Gelsenkirchen',
    'Mönchengladbach', 'Braunschweig', 'Chemnitz', 'Kiel', 'Aachen'
]

store_types = ['City Center', 'Shopping Mall', 'Outlet', 'Flagship Store']
store_sizes = ['Small', 'Medium', 'Large', 'Extra Large']

stores_data = []
for i in range(50):
    store_id = f"ST{str(i+1).zfill(4)}"
    city = random.choice(store_cities)
    store_type = random.choice(store_types)
    size = random.choice(store_sizes)
    
    # Size mapping to square meters
    size_mapping = {
        'Small': random.randint(100, 200),
        'Medium': random.randint(201, 400),
        'Large': random.randint(401, 600),
        'Extra Large': random.randint(601, 1000)
    }
    
    opening_date = datetime(2010, 1, 1) + timedelta(days=random.randint(0, 4000))
    
    stores_data.append({
        'store_id': store_id,
        'store_name': f'Deichmann {city}',
        'city': city,
        'store_type': store_type,
        'size_category': size,
        'size_sqm': size_mapping[size],
        'opening_date': opening_date.strftime('%Y-%m-%d'),
        'is_active': random.choice([True, True, True, False])  # 75% active
    })

df_stores = pd.DataFrame(stores_data)
df_stores.to_csv('data/raw/stores.csv', index=False)
print(f"✅ Generated {len(df_stores)} stores")

# ============================================================================
# 2. PRODUCTS DATA
# ============================================================================
print("\n👟 Generating Products Data...")

brands = ['Deichmann', 'Nike', 'Adidas', 'Puma', 'Vans', 'Converse', 
          'Skechers', 'Tamaris', 'Rieker', 'Graceland', 'Ellie', 'Victory']

categories = ['Sneakers', 'Boots', 'Sandals', 'Formal Shoes', 'Sports Shoes', 
              'Casual Shoes', 'Kids Shoes', 'High Heels', 'Slippers']

genders = ['Men', 'Women', 'Kids', 'Unisex']
seasons = ['Spring/Summer', 'Fall/Winter', 'All Season']

products_data = []
for i in range(200):
    product_id = f"PRD{str(i+1).zfill(5)}"
    brand = random.choice(brands)
    category = random.choice(categories)
    gender = random.choice(genders)
    season = random.choice(seasons)
    
    # Price based on brand and category
    base_price = random.uniform(19.99, 149.99)
    if brand in ['Nike', 'Adidas', 'Puma']:
        base_price *= 1.5
    
    products_data.append({
        'product_id': product_id,
        'product_name': f'{brand} {category} {random.choice(["Classic", "Sport", "Comfort", "Style", "Premium"])}',
        'brand': brand,
        'category': category,
        'gender': gender,
        'season': season,
        'price': round(base_price, 2),
        'cost': round(base_price * 0.6, 2),  # 40% margin
        'size_range': f'{random.randint(35, 38)}-{random.randint(42, 46)}',
        'color_options': random.randint(2, 8),
        'launch_date': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime('%Y-%m-%d'),
        'is_active': random.choice([True, True, True, False])
    })

df_products = pd.DataFrame(products_data)
df_products.to_csv('data/raw/products.csv', index=False)
print(f"✅ Generated {len(df_products)} products")

# ============================================================================
# 3. CUSTOMERS DATA
# ============================================================================
print("\n👥 Generating Customers Data...")

first_names = ['Anna', 'Ben', 'Clara', 'David', 'Emma', 'Felix', 'Greta', 'Hans',
               'Ida', 'Jonas', 'Klara', 'Leon', 'Maria', 'Noah', 'Olivia', 'Paul',
               'Sophie', 'Tim', 'Laura', 'Max']

last_names = ['Müller', 'Schmidt', 'Schneider', 'Fischer', 'Weber', 'Meyer',
              'Wagner', 'Becker', 'Schulz', 'Hoffmann', 'Koch', 'Bauer']

customers_data = []
for i in range(1000):
    customer_id = f"CUST{str(i+1).zfill(6)}"
    
    # Age distribution (realistic for shoe shopping)
    age = int(np.random.normal(35, 15))
    age = max(18, min(75, age))  # Clamp between 18 and 75
    
    registration_date = datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2000))
    
    # Customer segments
    if age < 25:
        segment = 'Young Adults'
    elif age < 40:
        segment = 'Prime Shoppers'
    elif age < 60:
        segment = 'Mature Customers'
    else:
        segment = 'Senior Shoppers'
    
    customers_data.append({
        'customer_id': customer_id,
        'first_name': random.choice(first_names),
        'last_name': random.choice(last_names),
        'email': f'customer{i+1}@email.com',
        'age': age,
        'gender': random.choice(['M', 'F', 'Other']),
        'city': random.choice(store_cities),
        'registration_date': registration_date.strftime('%Y-%m-%d'),
        'customer_segment': segment,
        'loyalty_member': random.choice([True, False]),
        'is_active': random.choice([True, True, True, False])
    })

df_customers = pd.DataFrame(customers_data)
df_customers.to_csv('data/raw/customers.csv', index=False)
print(f"✅ Generated {len(df_customers)} customers")

# ============================================================================
# 4. SALES DATA (Most Important!)
# ============================================================================
print("\n💰 Generating Sales Transactions...")

# Generate sales for the last 2 years
start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)

sales_data = []
transaction_id = 1

# Get active entities
active_stores = df_stores[df_stores['is_active'] == True]['store_id'].tolist()
active_products = df_products[df_products['is_active'] == True]['product_id'].tolist()
active_customers = df_customers[df_customers['is_active'] == True]['customer_id'].tolist()

# Generate approximately 10,000 transactions
for _ in range(10000):
    # Random date with higher probability on weekends
    days_diff = (end_date - start_date).days
    random_day = start_date + timedelta(days=random.randint(0, days_diff))
    
    # Increase sales on weekends
    if random_day.weekday() >= 5:  # Saturday or Sunday
        quantity = random.choices([1, 2, 3, 4], weights=[0.4, 0.3, 0.2, 0.1])[0]
    else:
        quantity = random.choices([1, 2, 3], weights=[0.6, 0.3, 0.1])[0]
    
    # Select random entities
    store_id = random.choice(active_stores)
    product_id = random.choice(active_products)
    customer_id = random.choice(active_customers) if random.random() > 0.2 else None  # 20% anonymous
    
    # Get product price
    product_price = df_products[df_products['product_id'] == product_id]['price'].values[0]
    
    # Calculate discount (seasonal sales)
    month = random_day.month
    if month in [1, 7]:  # January and July = Sale months
        discount_percent = random.choice([0, 10, 20, 30, 50])
    else:
        discount_percent = random.choice([0, 0, 0, 10])  # Mostly no discount
    
    discount_amount = round((product_price * quantity * discount_percent / 100), 2)
    total_amount = round((product_price * quantity) - discount_amount, 2)
    
    # Payment methods
    payment_method = random.choices(
        ['Cash', 'Credit Card', 'Debit Card', 'PayPal', 'Gift Card'],
        weights=[0.2, 0.35, 0.3, 0.1, 0.05]
    )[0]
    
    sales_data.append({
        'transaction_id': f"TXN{str(transaction_id).zfill(8)}",
        'transaction_date': random_day.strftime('%Y-%m-%d'),
        'transaction_time': f"{random.randint(9, 20):02d}:{random.randint(0, 59):02d}:00",
        'store_id': store_id,
        'product_id': product_id,
        'customer_id': customer_id if customer_id else 'ANONYMOUS',
        'quantity': quantity,
        'unit_price': product_price,
        'discount_percent': discount_percent,
        'discount_amount': discount_amount,
        'total_amount': total_amount,
        'payment_method': payment_method,
        'channel': random.choice(['In-Store', 'In-Store', 'In-Store', 'Online'])  # 75% in-store
    })
    
    transaction_id += 1

df_sales = pd.DataFrame(sales_data)
df_sales.to_csv('data/raw/sales.csv', index=False)
print(f"✅ Generated {len(df_sales)} sales transactions")

# ============================================================================
# 5. DATA QUALITY SUMMARY
# ============================================================================
print("\n" + "="*60)
print("📊 DATA GENERATION SUMMARY")
print("="*60)

print(f"\n🏪 Stores: {len(df_stores)} records")
print(f"   - Active: {df_stores['is_active'].sum()}")
print(f"   - Cities: {df_stores['city'].nunique()}")

print(f"\n👟 Products: {len(df_products)} records")
print(f"   - Active: {df_products['is_active'].sum()}")
print(f"   - Brands: {df_products['brand'].nunique()}")
print(f"   - Categories: {df_products['category'].nunique()}")

print(f"\n👥 Customers: {len(df_customers)} records")
print(f"   - Active: {df_customers['is_active'].sum()}")
print(f"   - Loyalty Members: {df_customers['loyalty_member'].sum()}")

print(f"\n💰 Sales: {len(df_sales)} transactions")
print(f"   - Total Revenue: €{df_sales['total_amount'].sum():,.2f}")
print(f"   - Date Range: {df_sales['transaction_date'].min()} to {df_sales['transaction_date'].max()}")
print(f"   - Average Order Value: €{df_sales['total_amount'].mean():.2f}")
print(f"   - With Discount: {(df_sales['discount_percent'] > 0).sum()} ({(df_sales['discount_percent'] > 0).sum()/len(df_sales)*100:.1f}%)")

print("\n" + "="*60)
print("✅ All data files saved in 'data/raw/' directory")
print("="*60)

print("\n📁 Generated files:")
print("   - data/raw/stores.csv")
print("   - data/raw/products.csv")
print("   - data/raw/customers.csv")
print("   - data/raw/sales.csv")

print("\n🚀 Ready to upload to Azure Databricks!")
print("💡 Tip: Check the CSV files to familiarize yourself with the data structure")
