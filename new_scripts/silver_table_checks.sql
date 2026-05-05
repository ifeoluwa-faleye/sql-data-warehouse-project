-- Quality Checks

SELECT
	cst_id,
	COUNT(cst_id)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id) != 1;


SELECT
	cst_firstname
FROM Silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT
	cst_lastname
FROM Silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

SELECT DISTINCT
	cst_marital_status
FROM Silver.crm_cust_info;

SELECT DISTINCT
	cst_gndr
FROM Silver.crm_cust_info;

SELECT DISTINCT
	MIN(cst_create_date),
	MAX(cst_create_date)
FROM Silver.crm_cust_info;


