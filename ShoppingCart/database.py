# database.py
import mysql.connector

#Defining connection to the database 'ShoppingCartDB'
def connect_to_db():
    """Establish a connection to the MySQL database."""
    return mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="Learning1234!",
        database="shoppingcartdb"
    )

def add_items_to_db(product_id, product_name, quantity, price):
    """Add items to the database."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = "INSERT INTO Products (Product_ID, Product_Name, Quantity, Price) VALUES (%s, %s, %s, %s)"
    values = (product_id, product_name, quantity, price)
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()

def read_items_from_db():
    """Read and return all products from the database."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = "SELECT * FROM Products"
    cursor.execute(sql)
    products = cursor.fetchall()
    cursor.close()
    conn.close()
    return products

def update_item_quantity(product_id, quantity_ordered):
    """Update the quantity of an item in the database after a purchase."""
    conn = connect_to_db()
    cursor = conn.cursor()
    sql = "UPDATE Products SET Quantity = Quantity - %s WHERE Product_ID = %s"
    values = (quantity_ordered, product_id)
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()
    conn.close()
