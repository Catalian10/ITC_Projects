# shopping_cart.py
from database import read_items_from_db, update_item_quantity

def display_items(products):
    """Display available products."""
    print("\nAvailable Products:")
    print("-" * 65)
    print(f"{'Product ID':<12} {'Product_Name':<20} {'Quantity':<10} {'Price (in Pounds)':<10}")
    print("-" * 65)
    
    for product in products:
        print(f"{product[0]:<12} {product[1]:<20} {product[2]:<10} £{product[3]:<10}")

def find_product(products, item_ordered):
    """Find a product in the list by its Product_ID."""
    for product in products:
        if product[0] == item_ordered:
            return product
    return None

def proceed_to_payment(total_price):
    """Handle payment process."""
    while True:
        pay = input("\nDo you want to proceed to payment (Yes/No)? : ").strip().lower()
        if pay == "yes":
            delivery_option(total_price)  # Ask for delivery or pick-up
            break
        elif pay == "no":
            print("Continuing shopping...")
            return
        else:
            print("Invalid input, please enter 'Yes' or 'No'.")

def delivery_option(total_price):
    """Handle delivery options."""
    delivery_choice = input("\nDo you want delivery or pick-up? (Enter 'delivery' or 'pick-up'): ").strip().lower()
    
    if delivery_choice == "delivery":
        delivery_info(total_price)  # If delivery is chosen, proceed to delivery info
    elif delivery_choice == "pick-up":
        print("\nYou have selected pick-up. No delivery charges will apply.")
        print("\nThank you for your payment! Your order is being processed.")
    else:
        print("Invalid option. Please enter 'delivery' or 'pick-up'.")

def delivery_info(total_price):
    """Delivery charges based on distance."""
    while True:
        try:
            distance = float(input("\nPlease enter your delivery distance in kilometers: "))
            if distance <= 15:
                delivery_charges = 50
                print(f"Delivery charge for {distance} km: £{delivery_charges}")
            elif 15 < distance <= 30:
                delivery_charges = 100
                print(f"Delivery charge for {distance} km: £{delivery_charges}")
            else:
                print("Sorry, delivery is not available for distances over 30 km.")
                return

            total_price += delivery_charges
            print(f"\nFinal total including delivery charges: £{total_price}")
            confirm_payment(total_price)
            break
        except ValueError:
            print("Invalid input. Please enter a valid distance.")

def confirm_payment(total_price):
    """Confirm and process the payment."""
    while True:
        confirmation = input("\nDo you confirm the payment? (Yes/No): ").strip().lower()
        if confirmation == "yes":
            print("\nPayment confirmed successfully!")
            print("\nThank you for your payment! Your order is being processed.")
            return True
        elif confirmation == "no":
            print("\nPayment not confirmed. You can continue shopping.")
            return False
        else:
            print("Invalid input, please enter 'Yes' or 'No'.")

def shopping_loop():
    """Main shopping loop."""
    total_price = 0
    products = read_items_from_db()  # Fetch products from database
    display_items(products)

    while True:
        try:
            item_ordered = int(input("\nEnter the Product Code of the item you want to purchase: "))
            look_item = find_product(products, item_ordered)

            if look_item:
                Product_ID, Product_Name, Quantity, Price = look_item
                print(f"You selected {Product_Name} priced at £{Price} each.\n")

                quantity_item_ordered = int(input(f"Enter the quantity you want to purchase (Available: {Quantity}): "))
                if quantity_item_ordered <= Quantity:
                    total_cost = quantity_item_ordered * Price
                    total_price += total_cost
                    print(f"You added {quantity_item_ordered} {Product_Name}(s). Total cost: £{total_cost}")
                    
                    # Update quantity in the database
                    update_item_quantity(Product_ID, quantity_item_ordered)
                else:
                    print(f"Sorry, we only have {Quantity} available.")
            else:
                print("\nProduct not found. Please enter a valid Product Code.")

            continue_shopping = input("\nDo you want to continue shopping (Yes/No)? : ").strip().lower()
            if continue_shopping != "yes":
                break
        except ValueError:
            print("Invalid input. Please enter a number.")

    print(f"Final total: £{total_price}")

    # Call the payment process
    proceed_to_payment(total_price)
