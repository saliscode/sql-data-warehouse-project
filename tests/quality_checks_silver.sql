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





=================================================================================
----Checking 'silver.erp_px_cat_g1v2'
=================================================================================
----check for unwanted spaces
select * from silver.erp_px_cat_g1v2
where cat != trim(cat)
    or subcat != trim(subcat)
    or maintenance != trim(maintenance);

----Data standardization and consistency
select distinct 
    maintenance 
from silver.erp_px_cat_g1v2;
