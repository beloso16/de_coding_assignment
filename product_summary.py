from flask import Flask, jsonify, request
import os
import data_processing

app = Flask(__name__)

main_folder_path = '/Users/faustineb/Desktop/de_assignment/'

@app.route('/assignment/transactionSummaryByProducts/<int:last_n_days>', methods=['GET'])
async def get_product_txn_summary(last_n_days):
    transaction_folder = main_folder_path + 'transactions/'
    transaction_files = [os.path.join(transaction_folder, file) for file in os.listdir(transaction_folder) if file.endswith('.csv')]
    product_file_path = main_folder_path + 'products/ProductReference.csv'
    summary = await data_processing.fetch_product_txn_summary(transaction_files, product_file_path, last_n_days)
    return jsonify(summary)

if __name__ == '__main__':
    app.run(debug=False, port=8080)
