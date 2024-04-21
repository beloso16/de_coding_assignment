from flask import Flask, jsonify
import os
import data_processing

app = Flask(__name__)

main_folder_path = '/Users/faustineb/Desktop/de_assignment/'

@app.route('/assignment/transaction/<int:transaction_id>', methods=['GET'])
async def get_transaction(transaction_id):
    transaction_folder = main_folder_path + 'transactions/'
    print("Transaction folder:", transaction_folder) 
    transaction_files = [os.path.join(transaction_folder, file) for file in os.listdir(transaction_folder) if file.endswith('.csv')]
    print("Transaction files:", transaction_files) 
    transactions = await data_processing.fetch_txn_details(transaction_files, main_folder_path + 'products/ProductReference.csv')
    print("All transactions:", transactions) 
    transaction = transactions.get(transaction_id)
    if transaction:
        return jsonify(transaction)
    return jsonify({"message": "Transaction not found"}), 404

if __name__ == '__main__':
    app.run(debug=False, port=8080)