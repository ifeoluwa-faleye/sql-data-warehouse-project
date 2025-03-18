TRUNCATE TABLE silver.crm_cust_info
INSERT INTO silver.crm_cust_info (
	cst_id, 
	cst_key, 
	cst_firstname, 
	cst_lastname, 
	cst_gndr, 
	cst_marital_status, 
	cst_create_date)

SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE UPPER(TRIM(cst_gndr))
		WHEN 'M' THEN 'Male'
		WHEN 'F' THEN 'Female'
		ELSE 'n/a'
	END AS cst_gndr,
	CASE UPPER(TRIM(cst_marital_status))
		WHEN 'M' THEN 'Married'
		WHEN 'S' THEN 'Single'
		ELSE 'n/a'
	END AS cst_marital_status,
	cst_create_date
FROM(
	SELECT
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Raank
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)t

WHERE Raank = 1
