import csv
from datetime import datetime, timedelta
import asyncio

##########################################################################################################################
# TRANSACTION DETAILS
async def fetch_txn_details(transaction_file_paths, product_file_path):
    try:
        # Concurrent processing of files
        tasks = []
        for transaction_file_path in transaction_file_paths:
            task = txn_details_process_transaction_file(transaction_file_path, product_file_path)
            tasks.append(task)
        
        # Await results of all tasks
        results = await asyncio.gather(*tasks)
        
        # Merge transaction details from all files
        transactions = {}
        for result in results:
            transactions.update(result)
        
        return transactions
    except Exception as e:
        print("Error fetching transaction details:", e)
        return {}

async def txn_details_process_transaction_file(transaction_file_path, product_file_path):
    try:
        transactions = {}
        
        # Load product names
        product_names = await load_product_names_async(product_file_path)
        
        # Read transaction file and process transactions
        with open(transaction_file_path, 'r') as transaction_file:
            transaction_reader = csv.DictReader(transaction_file)
            for row in transaction_reader:
                transaction_id = int(row['transactionId'])
                transactions[transaction_id] = {
                    "transactionId": transaction_id,
                    "transactionAmount": float(row['transactionAmount']),
                    "transactionDatetime": row['transactionDatetime']
                }
                product_id = int(row['productId'])
                transactions[transaction_id]['productName'] = product_names.get(product_id, "")
        
        return transactions
    except Exception as e:
        print(f"Error processing transaction file {transaction_file_path}: {e}")
        return {}

async def load_product_names_async(product_file_path):
    try:
        product_names = {}
        with open(product_file_path, 'r') as product_file:
            product_reader = csv.DictReader(product_file)
            for row in product_reader:
                product_id = int(row['productId'])
                product_names[product_id] = row['productName']
        return product_names
    except Exception as e:
        print("Error loading product names:", e)
        return {}



##########################################################################################################################
# PRODUCT SUMMARY
async def fetch_product_txn_summary(transaction_file_paths, product_file_path, last_n_days):
    try:
        today = datetime.today()
        n_days_ago = today - timedelta(days=last_n_days)
        
        with open(product_file_path, 'r') as product_file:
            product_reader = csv.DictReader(product_file)
            product_names = {int(row['productId']): row['productName'] for row in product_reader}

        # Concurrent processing of files
        tasks = []
        for transaction_file_path in transaction_file_paths:
            task = product_process_transaction_file(transaction_file_path, product_names, n_days_ago)
            tasks.append(task)
        product_summaries = await asyncio.gather(*tasks)
        
        # Aggregate
        aggregated_summary = {}
        for product_summary in product_summaries:
            for product, amount in product_summary.items():
                if product in aggregated_summary:
                    aggregated_summary[product] += amount
                else:
                    aggregated_summary[product] = amount
        
        return {"summary": [{"productName": name, "totalAmount": total} for name, total in aggregated_summary.items()]}
    except Exception as e:
        print("Error fetching product transaction summary:", e)
        return {}


async def product_process_transaction_file(transaction_file_path, product_names, n_days_ago):
    product_summary = {}
    try:
        with open(transaction_file_path, 'r') as transaction_file:
            transaction_reader = csv.DictReader(transaction_file)
            for row in transaction_reader:
                transaction_datetime = datetime.strptime(row['transactionDatetime'], '%Y-%m-%d %H:%M:%S')
                if transaction_datetime >= n_days_ago:
                    product_id = int(row['productId'])
                    product_name = product_names.get(product_id, "")
                    transaction_amount = float(row['transactionAmount'])
                    if product_name in product_summary:
                        product_summary[product_name] += transaction_amount
                    else:
                        product_summary[product_name] = transaction_amount
    except Exception as e:
        print(f"Error processing transaction file {transaction_file_path}: {e}")
    return product_summary

##########################################################################################################################
# CITY SUMMARY
async def fetch_city_txn_summary(transaction_file_paths, product_file_path, last_n_days):
    try:
        today = datetime.today()
        n_days_ago = today - timedelta(days=last_n_days)
        
        with open(product_file_path, 'r') as product_file:
            product_reader = csv.DictReader(product_file)
            city_names = {int(row['productId']): row['productManufacturingCity'] for row in product_reader}

        # Concurrent processing of files
        tasks = []
        for transaction_file_path in transaction_file_paths:
            task = city_process_transaction_file(transaction_file_path, city_names, n_days_ago)
            tasks.append(task)
        city_summaries = await asyncio.gather(*tasks)
        
        # Aggregate
        aggregated_summary = {}
        for city_summary in city_summaries:
            for city, amount in city_summary.items():
                if city in aggregated_summary:
                    aggregated_summary[city] += amount
                else:
                    aggregated_summary[city] = amount
        
        return {"summary": [{"cityName": name, "totalAmount": total} for name, total in aggregated_summary.items()]}
    except Exception as e:
        print("Error fetching city transaction summary:", e)
        return {}


async def city_process_transaction_file(transaction_file_path, city_names, n_days_ago):
    city_summary = {}
    try:
        with open(transaction_file_path, 'r') as transaction_file:
            transaction_reader = csv.DictReader(transaction_file)
            for row in transaction_reader:
                transaction_datetime = datetime.strptime(row['transactionDatetime'], '%Y-%m-%d %H:%M:%S')
                if transaction_datetime >= n_days_ago:
                    product_id = int(row['productId'])
                    city_name = city_names.get(product_id, "")
                    transaction_amount = float(row['transactionAmount'])
                    if city_name in city_summary:
                        city_summary[city_name] += transaction_amount
                    else:
                        city_summary[city_name] = transaction_amount
    except Exception as e:
        print(f"Error processing transaction file {transaction_file_path}: {e}")
    return city_summary
