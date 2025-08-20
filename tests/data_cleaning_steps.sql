/*******************************************************************************************
PROJECT: Data Cleaning & Transformation - Customer & Sales Data Warehouse

OVERVIEW:
This SQL script is designed to clean, validate, standardize, and transform raw CRM datasets 
stored in the `bronze` layer before loading them into the `silver` layer for analysis. 

The main objectives of this file are:
1. Ensure Data Quality:
   - Remove duplicates, nulls, invalid values, and unwanted spaces.
   - Validate formats and consistency across fields.
2. Standardize & Normalize:
   - Apply consistent formats for categorical attributes (e.g., Gender, Marital Status).
   - Trim spaces, adjust casing, replace short codes with descriptive values.
3. Business Rule Enforcement:
   - Validate and correct rules such as Sales = Quantity * Price.
   - Ensure no negative or invalid values in critical measures.
4. Date Quality Checks:
   - Validate correct formatting, acceptable ranges, and remove invalid dates.
5. Data Transformation & Enrichment:
   - Perform prefix removal, derived columns, typecasting, and enrichment logic.
6. Final Load:
   - Insert cleaned, transformed, standardized records into `silver` schema tables.

STRUCTURE OF THE FILE:
----------------------------------------------------------------------------------------------
1. Primary Key Validation
2. Whitespace Handling
3. Data Standardization (Gender, Marital Status)
4. Null & Invalid Value Checks (Numbers)
5. Date Validation
6. Business Rule Enforcement
7. String Transformation
8. Typecasting
9. Data Enrichment
10. Final Insert into Silver Tables

*******************************************************************************************/

USE DataWarehouse;
GO


/***********************************************
1. PRIMARY KEY VALIDATION
   - Check duplicates and nulls in `cst_id`.
   - Keep only the latest record for each unique customer.
************************************************/
SELECT cst_id, COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Keep latest non-null record per customer
SELECT * 
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;



/***********************************************
2. WHITESPACE HANDLING
   - Identify and trim leading/trailing spaces in names and gender.
************************************************/
SELECT cst_firstname
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);   

-- Apply trimming during import
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;



/***********************************************
3. DATA STANDARDIZATION - GENDER
   - Normalize Gender values:
     * 'F' -> Female
     * 'M' -> Male
     * NULL/Other -> n/a
************************************************/
SELECT DISTINCT cst_gndr
FROM bronze.crm_cust_info;

SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;



/***********************************************
4. DATA STANDARDIZATION - MARITAL STATUS
   - Normalize Marital Status values:
     * 'S' -> Single
     * 'M' -> Married
     * NULL/Other -> n/a
************************************************/
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;



/***********************************************
5. NULL / INVALID VALUE CHECKS (NUMERIC)
   - Detect negative or null product costs.
   - Replace nulls with 0 where applicable.
************************************************/
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Example handling with ISNULL()
-- ISNULL(prd_cost, 0) AS prd_cost



/***********************************************
6. DATE VALIDATION
   - Ensure order dates are valid:
     * Not zero or negative.
     * Must have length = 8 (YYYYMMDD).
     * Must fall within realistic boundaries.
************************************************/
SELECT  
    NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0
   OR LEN(sls_order_dt) != 8
   OR sls_order_dt > 202500101
   OR sls_order_dt < 19000101;

-- Correct invalid dates
SELECT 
    CASE 
        WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt
FROM bronze.crm_sales_details;



/***********************************************
7. BUSINESS RULE VALIDATION
   - Rule: Sales = Quantity * Price
   - Ensure no Null, Zero, or Negative values.
************************************************/
SELECT 
    sls_sales,
    sls_quantity,
    sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0;

-- Fix business rule violations
SELECT DISTINCT	
    sls_sales AS old_sls_sales,
    sls_quantity,
    sls_price AS old_sls_price,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0  
             OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price
FROM bronze.crm_sales_details;



/***********************************************
8. STRING TRANSFORMATION
   - Remove prefixes and derive new IDs.
************************************************/
-- Example: Remove NAS prefix
-- WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))

-- Example: Derived columns
-- REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
-- SUBSTRING(prd_key, 7, LEN(prd_key))       AS prd_key



/***********************************************
9. TYPECASTING
   - Convert values from one data type to another.
   - Important for handling dates/numbers.
************************************************/
-- Syntax: CAST(expression AS target_data_type)
-- Example: CAST(prd_start_dt AS DATE) AS prd_start_dt



/***********************************************
10. DATA ENRICHMENT
   - Enhance dataset with new derived fields.
************************************************/
-- Example: Derive product end date
-- CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt



/***********************************************
11. FINAL LOAD INTO SILVER LAYER
   - Insert cleaned & standardized data into silver table.
************************************************/
INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
) 
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname)  AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t 
WHERE flag_last = 1;
