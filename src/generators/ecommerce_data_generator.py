import random
from faker import Faker
from src.database.mysql_connection import get_mysql_connection
fake=Faker("en_IN")


# -----------------------------
# Product Data
# -----------------------------
PRODUCTS = [
    ("Wireless Bluetooth Headphones", "Electronics", "Boat"),
    ("Mechanical Keyboard", "Electronics", "Redragon"),
    ("Gaming Mouse", "Electronics", "Logitech"),
    ("USB-C Charger", "Electronics", "Anker"),
    ("Laptop Backpack", "Accessories", "American Tourister"),
    ("Running Shoes", "Fashion", "Nike"),
    ("Cotton T-Shirt", "Fashion", "Puma"),
    ("Smart Watch", "Electronics", "Noise"),
    ("Water Bottle", "Lifestyle", "Milton"),
    ("Notebook", "Stationery", "Classmate"),
]


def get_connection():
    connection = get_mysql_connection()

    cursor = connection.cursor()

    cursor.execute("SELECT DATABASE()")
    print("Python connected to:", cursor.fetchone()[0])

    cursor.execute("SHOW COLUMNS FROM customers")
    print("Customers columns:")

    for column in cursor.fetchall():
        print(column[0])

    cursor.close()

    return connection


def generate_customers(cursor, count=100):
    print(f"Generating {count} customers...")

    for _ in range(count):
        cursor.execute(
            """
            INSERT INTO customers
            (first_name, last_name, email, phone, city, state, country)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                fake.first_name(),
                fake.last_name(),
                fake.unique.email(),
                fake.phone_number()[:20],
                fake.city(),
                fake.state(),
                "India"
            )
        )


def generate_products(cursor):
    print("Generating products...")

    for product in PRODUCTS:
        product_name, category, brand = product

        price = round(random.uniform(299, 50000), 2)
        cost_price = round(price * random.uniform(0.5, 0.8), 2)
        stock = random.randint(20, 500)

        cursor.execute(
            """
            INSERT INTO products
            (product_name, category, brand, price, cost_price,
             stock_quantity, reorder_level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                product_name,
                category,
                brand,
                price,
                cost_price,
                stock,
                random.randint(5, 30)
            )
        )


def generate_inventory(cursor):
    print("Generating inventory...")

    cursor.execute(
        "SELECT product_id, stock_quantity FROM products"
    )

    products = cursor.fetchall()

    for product_id, stock_quantity in products:

        cursor.execute(
            """
            INSERT INTO inventory
            (product_id, available_quantity,
             reserved_quantity, warehouse_location)
            VALUES (%s, %s, %s, %s)
            """,
            (
                product_id,
                stock_quantity,
                0,
                random.choice([
                    "Pune",
                    "Mumbai",
                    "Delhi",
                    "Bangalore",
                    "Hyderabad"
                ])
            )
        )


def generate_orders(cursor, count=500):

    print(f"Generating {count} orders...")

    cursor.execute("SELECT customer_id FROM customers")
    customers = [row[0] for row in cursor.fetchall()]

    cursor.execute(
        "SELECT product_id, price FROM products"
    )

    products = cursor.fetchall()

    for _ in range(count):

        customer_id = random.choice(customers)

        selected_products = random.sample(
            products,
            random.randint(1, 4)
        )

        total_amount = 0
        order_items = []

        for product_id, price in selected_products:

            quantity = random.randint(1, 3)

            item_total = price * quantity
            total_amount += item_total

            order_items.append(
                (product_id, quantity, price)
            )

        order_status = random.choice([
            "PLACED",
            "CONFIRMED",
            "SHIPPED",
            "DELIVERED",
            "CANCELLED"
        ])

        order_time = fake.date_time_between(
            start_date="-30d",
            end_date="now"
        )

        cursor.execute(
            """
            INSERT INTO orders
            (customer_id, order_status,
             total_amount, order_timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (
                customer_id,
                order_status,
                round(total_amount, 2),
                order_time
            )
        )

        order_id = cursor.lastrowid

        for product_id, quantity, price in order_items:

            cursor.execute(
                """
                INSERT INTO order_items
                (order_id, product_id,
                 quantity, unit_price)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    order_id,
                    product_id,
                    quantity,
                    price
                )
            )

        payment_status = random.choices(
            ["SUCCESS", "FAILED", "PENDING", "REFUNDED"],
            weights=[80, 10, 7, 3]
        )[0]

        payment_method = random.choice([
            "UPI",
            "CARD",
            "NET_BANKING",
            "COD"
        ])

        cursor.execute(
            """
            INSERT INTO payments
            (order_id, payment_method,
             payment_status, transaction_amount)
            VALUES (%s, %s, %s, %s)
            """,
            (
                order_id,
                payment_method,
                payment_status,
                round(total_amount, 2)
            )
        )


def main():

    connection = get_connection()
    cursor = connection.cursor()

    try:

        generate_customers(cursor, 100)

        generate_products(cursor)

        generate_inventory(cursor)

        generate_orders(cursor, 500)

        connection.commit()

        print("\nData generation completed successfully!")

    except Exception as error:

        connection.rollback()

        print("Error:", error)

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()