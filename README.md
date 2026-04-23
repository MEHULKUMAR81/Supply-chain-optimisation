# Supply Chain Optimization using SQL + Linear Programming

## Overview
This project builds an end-to-end supply chain optimization pipeline to assign customer orders to manufacturing plants while minimizing total logistics cost under operational constraints.

The workflow integrates SQL-based data modeling (Bronze → Silver → Gold layers) with a Linear Programming (LP) model implemented in Python (PuLP) for optimal decision-making.

---

## Dataset
Source:  
https://brunel.figshare.com/articles/dataset/Supply_Chain_Logistics_Problem_Dataset/7558679?file=20162015

The dataset contains:
- Orders (product, destination, quantity, weight)
- Freight rates (costs based on weight brackets and routes)
- Warehouse capacities
- Plant-to-product mappings
- VMI (Vendor Managed Inventory) constraints

---

## Problem Statement
For each customer order, determine the optimal plant assignment such that:

- Each order is fulfilled exactly once  
- Total cost (warehouse + shipping) is minimized  
- Plant daily capacity constraints are respected  
- Freight rates are applied based on weight brackets and routes  
- VMI constraints are enforced:
  - Some plants can serve only specific customers  
  - Non-VMI plants can serve any customer  

---

## Solution Approach

### 1. Data Engineering (SQL)
Structured into 3 layers:

**Bronze Layer**
- Raw CSV ingestion using `LOAD DATA INFILE`

**Silver Layer**
- Data cleaning and filtering
- Removal of invalid carriers (e.g., V44_3, CRF)
- Standardization of formats

**Gold Layer**
- Final joined dataset combining:
  - Orders
  - Freight rates
  - Warehouse capacities
  - Costs
- Computation of:
  - Warehouse cost
  - Shipping cost
  - Total cost per option
- Generation of candidate assignment options

---

### 2. Optimization Model (Python - PuLP)

Decision Variable:
- Binary variable \( x_{ij} \)
- 1 if order *i* is assigned to option *j*, else 0

Objective:
- Minimize total cost across all selected assignments

Constraints:
1. Each order must be assigned exactly once  
2. Total assigned quantity to each plant per day must not exceed capacity  

---

## Results

The model outputs:
- Optimal assignment of orders to plants  
- Minimum total logistics cost  
- Plant-wise utilization  

### Key Outputs:
- `lp_final_assignment.csv`
- `lp_plant_utilisation.csv
