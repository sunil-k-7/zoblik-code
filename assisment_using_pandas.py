import sqlite3
import csv
import pandas as pd

# Specify the file path of the binary SQLite database file
binary_file_path = r'C:\Users\Admin\Desktop\null.bin'

# Open the binary SQLite database file in binary read mode
with open(binary_file_path, 'rb') as binary_file:
    binary_data = binary_file.read()

# Specify the file path of the new SQLite database file
with open('null.db', 'wb') as new_db_file:
    new_db_file.write(binary_data)

print("Conversion completed successfully.")

# Connect to the SQLite database
conn = sqlite3.connect('null.db')
cursor = conn.cursor()

try:
    # SQL query to get total quantities of each item bought per customer aged 18-35
    sql_query = """
                SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
                FROM CUSTOMERS c
                JOIN SALES s ON c.customer_id = s.customer_id
                JOIN ORDERS o ON s.sales_id = o.sales_id
                JOIN ITEMS i ON o.item_id = i.item_id
                WHERE c.age BETWEEN 18 AND 35 AND o.quantity IS NOT NULL
                GROUP BY c.customer_id, i.item_name;
                """

    # Execute the query
    cursor.execute(sql_query)

    # Fetch all results
    results = cursor.fetchall()

    # Write results to a CSV file using pure SQL
    with open('output_sql.csv', 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=';')
        # Write headers
        csv_writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
        # Write rows
        csv_writer.writerows(results)

    print("Data written to output_sql.csv")

except sqlite3.Error as e:
    print("SQLite error:", e)

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection
    conn.close()


# now we are writing the code using the pandas library

# Connect to the SQLite database again
conn = sqlite3.connect('null.db')

try:
    # Read data from database tables into DataFrames
    customers_df = pd.read_sql_query("SELECT * FROM CUSTOMERS", conn)
    sales_df = pd.read_sql_query("SELECT * FROM SALES", conn)
    orders_df = pd.read_sql_query("SELECT * FROM ORDERS", conn)
    items_df = pd.read_sql_query("SELECT * FROM ITEMS", conn)

    # Merge tables to get required data
    merged_df = pd.merge(customers_df, sales_df, on='customer_id')
    merged_df = pd.merge(merged_df, orders_df, on='sales_id')
    merged_df = pd.merge(merged_df, items_df, on='item_id')

    # Filter data for customers aged 18-35 and remove rows with NULL quantity
    filtered_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35) & (merged_df['quantity'].notnull())]

    # Group by customer, item, and age and sum quantities
    grouped_df = filtered_df.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()

    # Write results to a CSV file using Pandas
    grouped_df.to_csv('output_pandas.csv', sep=';', index=False)

    print("Data written to output_pandas.csv")

except Exception as e:
    print("Error:", e)

finally:
    # Close the database connection
    conn.close()
