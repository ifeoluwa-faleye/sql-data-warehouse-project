INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
SELECT
       [cst_id]
      ,[cst_key]
      ,TRIM([cst_firstname]) AS cst_firstname
      ,TRIM([cst_lastname]) AS cst_lastname
      ,CASE UPPER(TRIM([cst_marital_status]))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status
      ,CASE UPPER(TRIM([cst_gndr]))
            WHEN 'M' THEN 'Male'
            WHEN 'F' THEN 'Female'
            ELSE 'n/a'
        END AS cst_gndr
      ,[cst_create_date]
FROM
(
SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS cust_rank
FROM Bronze.crm_cust_info
WHERE cst_id IS NOT NULL)t 
WHERE cust_rank = 1

SELECT prd_id
      ,prd_key
      ,REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id
      ,SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
FROM
(
SELECT
    prd_id
      ,prd_key
      ,prd_nm
      ,prd_cost
      ,prd_line
      ,prd_start_dt
      ,prd_end_dt
    ,COUNT(prd_id) OVER(PARTITION BY prd_id) AS unique_prd
FROM Bronze.crm_prd_info
)t
WHERE unique_prd = 1
