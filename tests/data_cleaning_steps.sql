/**********************************************************************************************
 Project: Data Cleaning & Transformation Pipeline  
 File   : Data_Cleaning.sql  
 Author : [Your Name]  
 Purpose:  
 --------  
 This SQL script performs **data cleaning, standardization, validation, and integration**  
 for CRM, ERP, and Sales data in a Data Warehouse environment.  

 Overview of Steps:  
 ------------------
 1. **Primary Key Validation** → Checks for NULLs & duplicates in `cst_id`.  
 2. **Deduplication** → Keeps only the latest record per customer using `ROW_NUMBER()`.  
 3. **Whitespace Cleaning** → Removes leading/trailing spaces from customer names & gender.  
 4. **Data Standardization** → Normalizes values (e.g., `M/F → Male/Female`, `S/M → Single/Married`).  
 5. **Null & Invalid Value Handling** → Handles NULLs, negative numbers, invalid dates.  
 6. **Business Rule Validation** → Ensures `Sales = Quantity * Price` and no invalid values.  
 7. **Data Transformation** → Derived columns, type casting, enrichment.  
 8. **Data Integration** → Combines CRM with ERP systems, resolving conflicts.  
 9. **Surrogate Key Creation** → Generates warehouse keys for dimension modeling.  
 10. **Current vs Historical Data** → Filters for current active records when required.  

 This file ensures that **raw data (bronze layer)** is cleaned, standardized, and  
 transformed into the **silver layer** for reliable downstream analysis.  
**********************************************************************************************/

USE DataWarehouse;
GO


-- ==========================================================================================
-- 1. PRIMARY KEY VALIDATION → Check for duplicates & NULLs in cst_id
-- ==========================================================================================
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Keep only unique & latest valid records (deduplication by cst_create_date)
SELECT * FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;



-- ==========================================================================================
-- 2. WHITESPACE CLEANING → Check and remove unwanted leading/trailing spaces
-- ==========================================================================================
SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);  -- Same logic applies to lastname and gender

-- Import with trimmed values (cleaned version)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;



-- ==========================================================================================
-- 3. DATA STANDARDIZATION → Normalize categorical values (Gender, Marital Status)
-- ==========================================================================================

-- Check distinct values in Gender column
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

-- Replace inconsistent values: M/F → Male/Female, NULLs → n/a, trim spaces, enforce casing
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a' END AS cst_gndr,
    cst_create_date
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;


-- Standardize Marital Status similarly (S/M → Single/Married)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a' END AS cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'n/a' END AS cst_gndr,
    cst_create_date
FROM(
    SELECT *,
    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;



-- ==========================================================================================
-- 4. NULL & INVALID VALUE HANDLING (Numeric + Dates)
-- ==========================================================================================

-- Check product cost anomalies (negative or NULL)
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Common SQL functions to handle NULLs across engines
/* Reference:
ISNULL(), IFNULL(), COALESCE(), NULLIF(), NVL(), CASE WHEN
*/


-- Check invalid Sales Dates (outliers, wrong length, impossible values)
SELECT  
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 202500101
   OR sls_order_dt < 19000101;

-- Fix invalid dates (convert only valid values to DATE)
SELECT 
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
         ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt
FROM bronze.crm_sales_details;



-- ==========================================================================================
-- 5. BUSINESS RULE VALIDATION
-- ==========================================================================================
-- Business Rules:
-- (1) Sales = Quantity * Price
-- (2) None of Sales, Quantity, Price can be NULL, Negative, or Zero

SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- Fix invalid/missing business rule values
SELECT DISTINCT
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0  
              OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales END AS sls_sales,
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity, 0)
         ELSE sls_price END AS sls_price
FROM bronze.crm_sales_details;



-- ==========================================================================================
-- 6. DATA TRANSFORMATION & ENRICHMENT
-- ==========================================================================================
-- Examples: Derived Columns, Type Casting, Removing prefixes, Enrichment with window functions
-- CAST(), SUBSTRING(), REPLACE(), LEAD(), etc.



-- ==========================================================================================
-- 7. DATA INTEGRATION → Resolve conflicts across CRM and ERP sources
-- ==========================================================================================
SELECT DISTINCT 
    ci.cst_gndr,
    ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid;

-- Integration logic: prioritize CRM, fallback on ERP using COALESCE
SELECT DISTINCT 
    ci.cst_gndr,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
         ELSE COALESCE(ca.gen, 'n/a') END AS new_gen,
    ca.gen
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid;



-- ==========================================================================================
-- 8. SURROGATE KEY CREATION (For Data Modeling in DW)
-- ==========================================================================================
-- Use ROW_NUMBER() to generate unique surrogate key for dimensions
SELECT
    ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_name,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
         ELSE COALESCE(ca.gen, 'n/a') END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info AS ci
LEFT JOIN silver.erp_cust_az12 AS ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS la
    ON ci.cst_key = la.cid;



-- ==========================================================================================
-- 9. CURRENT vs HISTORICAL DATA FILTERING
-- ==========================================================================================
-- Keep only current active records (where end_date IS NULL)
SELECT 
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pc.cat,
    pc.subcat,
    pc.maintenance,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL;

