# -*- coding: utf-8 -*-
"""
Created on Tue Sep 24 16:08:51 2024

@author: siddh
"""

from database import add_items_to_db

def setup_database():
    add_items_to_db(1,"Hoodies", 10, 10.65)
    add_items_to_db(2, "Track suits", 10, 22.45)
    add_items_to_db(3, "Shoes", 10, 50.99)
    add_items_to_db(4, "Tshirts", 10, 5.99)

if __name__ == "__main__":
    setup_database()
    from shopping_cart import shopping_loop
    shopping_loop()
    
    