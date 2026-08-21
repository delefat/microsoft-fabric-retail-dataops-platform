# Architecture

# 

# The project implements a Microsoft Fabric medallion architecture.

# 

## Bronze

# 

# Raw source files are ingested through Fabric Data Factory and

# retained in OneLake using batch-based folder structures.

# 

## Silver

# 

# Fabric PySpark notebooks perform:

# 

# \- schema enforcement

# \- deduplication

# \- data validation

# \- referential integrity checking

# \- bad-record quarantine

# \- Delta MERGE

# \- incremental processing

# 

## Gold

# 

# Fabric Warehouse contains a Kimball dimensional model:

# 

# \- DimCustomer

# \- DimProduct

# \- DimStore

# \- DimDate

# \- FactSales

# \- FactInventory

