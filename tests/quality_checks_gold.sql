/*
=========================================================================
Quality Checks
=========================================================================
Script Purpose:
     This scpript performs quality checks to validate the integrity,
     consistency, and accuracy of the gold layer. These checks ensure 
     - Uniqueness of surrogate keys in dimension tables
     - Integration between fact and dimension tables.
     - Validation of relationships in the data model for analytical purposes.

Usage:
  -Run these after data loading silver layer.
  -Investigate and resolve any discrepancies found during the checks.

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

----Data Integration for cst_gndr and gen in Left Join btn 
----silver.crm_cust_info and silver.erp_cust_az1
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr != 'n/a' then ci.cst_gndr   ----CRM is the Master for gender Info
     else coalesce(ca.gen, 'n/a')
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on    ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on    ci.cst_key = la.cid


------Check for Foreign Key Integration for (Dimensions)

select 
*
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
left join gold.dim_products p
on p.product_key = f.product_key
where p.product_key is null
