from decimal import Decimal
from transformations import calculate_gross_amount,calculate_net_amount,is_valid_quantity
def test_gross(): assert calculate_gross_amount(2,69.99)==Decimal('139.98')
def test_net(): assert calculate_net_amount(2,69.99,10)==Decimal('129.98')
def test_qty():
    assert is_valid_quantity(1)
    assert not is_valid_quantity(0)
    assert not is_valid_quantity(None)
