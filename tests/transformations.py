from decimal import Decimal
def calculate_gross_amount(quantity,unit_price): return Decimal(str(quantity))*Decimal(str(unit_price))
def calculate_net_amount(quantity,unit_price,discount_amount): return calculate_gross_amount(quantity,unit_price)-Decimal(str(discount_amount or 0))
def is_valid_quantity(quantity): return quantity is not None and quantity>0
