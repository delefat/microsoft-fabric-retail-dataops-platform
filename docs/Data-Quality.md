# Data Quality



Invalid records are not silently discarded.



Records failing validation are written to quarantine tables together

with a validation\_error describing the reason for rejection.



Example:



OrderItemID: OI2000999

Quantity: 0

validation\_error: Quantity must be > 0

