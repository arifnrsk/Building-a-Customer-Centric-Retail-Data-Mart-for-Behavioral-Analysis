import pandas as pd
import random
import os
from datetime import datetime, timedelta

# --- Product Dictionary (Indonesian Market) ---
products = [
    {'StockCode': 'IND-001', 'Description': 'Indomie Goreng Original', 'Price': 3000},
    {'StockCode': 'IND-002', 'Description': 'Telur Ayam (1 butir)', 'Price': 2500},
    {'StockCode': 'IND-003', 'Description': 'Saus Sambal Botol', 'Price': 8000},
    {'StockCode': 'BEV-001', 'Description': 'Teh Botol Sosro Kotak', 'Price': 3500},
    {'StockCode': 'BEV-002', 'Description': 'Aqua Botol 600ml', 'Price': 3000},
    {'StockCode': 'HHLD-001', 'Description': 'Sunlight Pencuci Piring', 'Price': 14000},
    {'StockCode': 'HHLD-002', 'Description': 'Spon Cuci Piring', 'Price': 2000},
    {'StockCode': 'SNK-001', 'Description': 'Qtela Keripik Singkong', 'Price': 9000},
    {'StockCode': 'SNK-002', 'Description': 'Chitato Sapi Panggang', 'Price': 11000},
    {'StockCode': 'RICE-001', 'Description': 'Beras 5kg', 'Price': 65000},
]

# --- Anomaly Items (Non-product) ---
anomalies = [
    {'StockCode': 'BIAYA-ADMIN', 'Description': 'Biaya Admin', 'Price': 2500},
    {'StockCode': 'ONGKIR', 'Description': 'Biaya Pengiriman', 'Price': 15000},
    {'StockCode': 'DISCOUNT', 'Description': 'Potongan Harga', 'Price': 0}, # Price is 0 as it's a discount code
]

def generate_transactions(num_rows, start_date_str):
    """Main function to generate raw transaction data."""
    
    all_transactions = []
    current_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    invoice_num = 1
    
    while len(all_transactions) < num_rows:
        num_items_in_basket = random.randint(1, 5)
        
        # 1% chance of being a non-product anomaly transaction
        if random.random() < 0.01: 
            item = random.choice(anomalies)
            item['Invoice'] = f"ANM-{current_date.strftime('%Y%m%d')}-{invoice_num}"
            item['Quantity'] = 1
            item['InvoiceDate'] = current_date
            item['Customer ID'] = None
            all_transactions.append(item)
        else:
            # Generate a normal transaction basket
            customer_id = random.randint(12000, 18000)
            current_invoice_id = f"INV-{current_date.strftime('%Y%m%d')}-{invoice_num}"

            for _ in range(num_items_in_basket):
                item = random.choice(products).copy()
                
                # Quantity is always positive now
                item['Quantity'] = random.randint(1, 5)

                # 2% chance of having a zero price
                if random.random() < 0.02: 
                    item['Price'] = 0

                item['Invoice'] = current_invoice_id
                item['InvoiceDate'] = current_date
                
                # 10% chance of having a null Customer ID
                if random.random() < 0.1:
                    item['Customer ID'] = None
                else:
                    item['Customer ID'] = customer_id
                    
                all_transactions.append(item)
        
        invoice_num += 1
        # Advance the day every 500 invoices to create daily data
        if invoice_num % 500 == 0: 
            current_date += timedelta(days=1)

    return pd.DataFrame(all_transactions)

if __name__ == "__main__":
    print("Generating raw data...")
    TOTAL_ROWS = 1000000
    START_DATE = '2024-01-01'
    
    df_raw = generate_transactions(TOTAL_ROWS, START_DATE)
    
    output_folder = 'generated_data'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    print(f"Total {len(df_raw)} rows generated.")
    print("Saving data into daily CSV files...")

    df_raw['InvoiceDate'] = pd.to_datetime(df_raw['InvoiceDate'])
    daily_groups = df_raw.groupby(df_raw['InvoiceDate'].dt.date)

    for date, group in daily_groups:
        filename = os.path.join(output_folder, f"transaksi_{date.strftime('%Y-%m-%d')}.csv")
        group.to_csv(filename, index=False)
    
    print(f"Data saved into {len(daily_groups)} daily files in the '{output_folder}' folder.")