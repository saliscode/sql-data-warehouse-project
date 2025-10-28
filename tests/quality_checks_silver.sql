/*
===================================================================================================================
Quality Checks
===================================================================================================================
Script Purpose:
     This script performs various quality checks for data consistency, accuracy, and standardization across the 
     'silver' schemas. It includes checks for:
      - Null or duplicate primary keys.
      - Unwanted spaces in string fields.
      - Data standardization and consistency.
      - Invalid data ranges and orders.
      - Data consistency between related fields.

Usage Notes:
      - Run these checks after loading silver layer data.
      - Investigate and resolve any discrepancies found during the checks.
===================================================================================================================
*/




--Quality checks

---Check for Nulls or Duplicates in Primary key
---Expectation: No results
select
prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

---Check dor unwanted spaces
---Expectation: No results
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

---Check for nulls or negative numbers
---expectation: no results
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

---Data Standardization & Consistency
select distinct prd_line
from silver.crm_prd_info

---Check for Invalid Data Orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt

----======crm_sales_details============

---check for invalid dates orders
select
*
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

---check data consistency: between sales, Quantity and Price
---->> Sales = Quantity * Price
---->> Values must not be null, zero or negative

select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

----Identify Out-Of-Range Dates
select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

---Data Standardization & Consistency
select distinct
gen
from silver.erp_cust_az12


-----------===========================================================

----Check for Unwanted Spaces in silver.erp_px_cat_g1v2
select * from silver.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance);

---Data Standardization & Consistency for silver.erp_px_cat_g1v2
select distinct
cat
from silver.erp_px_cat_g1v2

select * from silver.crm_prd_info

