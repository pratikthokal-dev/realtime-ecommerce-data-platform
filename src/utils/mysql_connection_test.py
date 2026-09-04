import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="2112003",
    database="ecommerce_platform"
)

if connection.is_connected():
    print("MySQL connection successful!")

connection.close()