from flask import Flask, request, jsonify
import mysql.connector
from mysql.connector import Error
import requests
import csv
import logging
import json

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

# Function to create a table in MySQL
def create_table_if_not_exists(table_name, columns, connection):
    try:
        cursor = connection.cursor()
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, "
        create_table_query += ", ".join([f"{col} TEXT" for col in columns])
        create_table_query += ")"
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        print(f"Table '{table_name}' created successfully")

    except Error as e:
        print(f"Error creating table: {str(e)}")

# Function to insert data into MySQL
# Function to insert data into MySQL
def insert_data_into_mysql(data, table_name, connection):
    try:
        if connection.is_connected():
            cursor = connection.cursor()
            columns = list(data.keys())

            # Check for duplicates based on all columns
            select_query = f"SELECT * FROM {table_name} WHERE "
            select_query += " AND ".join([f"{col} = %s" for col in columns])
            cursor.execute(select_query, tuple(data[col] for col in columns))
            existing_row = cursor.fetchone()

            if existing_row is None:
                insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
                cursor.execute(insert_query, tuple(data.values()))
                connection.commit()
                print("Data inserted into MySQL successfully")
            else:
                print("Data already exists in MySQL, skipping insertion")

            cursor.close()
        else:
            print("Connection to MySQL failed")

    except Error as e:
        print(f"Error inserting data into MySQL: {str(e)}")


# Function to download and process data from the URL
# Function to download and process data from the URL
# Function to download and process data from the URL
def process_url_and_insert_into_mysql(url, connection):
    try:
        if connection.is_connected():
            response = requests.get(url)
            if response.status_code == 200:
                csv_data = response.text
                csv_reader = csv.DictReader(csv_data.splitlines())
                first_row = next(csv_reader)  # Get the first row of data

                # Extract relevant words from the URL to determine the table name
                url_words = url.split('/')
                table_name = 'multi_table'  # Default table name

                if 'user_id' in first_row and all(word in url for word in ['calls', 'stats']):
                    table_name = 'user_daily_stats'
                elif all(word in url for word in ['calls', 'stats']):
                    table_name = 'daily_stats'
                # Add more conditions to match other tables

                # Continue with the word-based table selection
                for word in url_words:
                    # if 's=calls&t=stats' in word:
                    #     table_name = 'daily_stats'
                    #     break
                    if 's=recordings&t=records' in word:
                        table_name = "call_recordings"
                        break
                    elif 's=voicemails&t=records' in word:
                        table_name = "voicemails"
                        break
                    elif 's=calls&t=records' in word:
                        table_name = 'call_logs'
                        break

                create_table_if_not_exists(table_name, first_row.keys(), connection)
                insert_data_into_mysql(first_row, table_name, connection)

                for row in csv_reader:
                    insert_data_into_mysql(row, table_name, connection)

                print(f"Data from URL '{url}' inserted into MySQL table '{table_name}' successfully")

            else:
                print("Failed to fetch data from the URL")
        else:
            print("Connection to MySQL failed")

    except Error as e:
        print(f"Error inserting data into MySQL: {str(e)}")



# Define a route to capture and store URL data
@app.route('/webhook', methods=['POST'])
def capture_and_store_url():
    try:
        raw_request_data = request.data

        # If the Content-Type is not set to "application/json," treat the data as JSON
        if "application/json" not in request.headers.get('Content-Type', ''):
            try:
                # Attempt to parse the request data as JSON
                request_data = json.loads(raw_request_data)
            except json.JSONDecodeError:
                return jsonify({"message": "Invalid JSON data"}), 400
        else:
            # Content-Type is already set to "application/json," so directly parse it
            request_data = request.json

        download_url = request_data.get('download_url')
        connection = mysql.connector.connect(
            host='buyinghomes.cdbvbmz1kpzj.us-east-2.rds.amazonaws.com',
            database='buyinghomes',
            user='pycharm',
            password='pycharm'
        )

        if connection.is_connected():
            process_url_and_insert_into_mysql(download_url, connection)
            connection.close()
            return jsonify({"message": "URL data captured, processed, and stored in MySQL successfully"})
        return jsonify({"message": "Failed to insert URL data into MySQL"})

    except Exception as e:
        return jsonify({"message": f"An error occurred: {str(e)}"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=7000)
