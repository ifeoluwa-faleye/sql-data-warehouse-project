/*
=============================================================================
Stored Procedure: Load Silver (Bronze -> Silver)
=============================================================================
Script Purpose:
  This stored procedure performs the ETL process to populate the 
  'silver' Schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates Silver tables.
  - Inserts transformed and cleansed data from Bronze into Silver Tables
Parameters:
  None
  This stored procedure does not accept any parameters or return any value.
Usage Example:
  EXEC Silver,load_silver
=============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '==========================================';
		PRINT '>> Loading Silver Layer';
		PRINT '==========================================';

		PRINT '------------------------------------------';
		PRINT '>> Loading CRM Tables';
		PRINT '------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '******************************************';
		PRINT '>> Truncating Table: silver.crm_cust_info';
		PRINT '******************************************';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '******************************************';
		PRINT '>> Inserting Data: silver.crm_cust_info';
		PRINT '******************************************';
			INSERT INTO silver.crm_cust_info(
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
						WHEN 'M' THEN 'Married'
						WHEN 'S' THEN 'Single'
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
			SELECT [cst_id]
					,[cst_key]
					,[cst_firstname]
					,[cst_lastname]
					,[cst_marital_status]
					,[cst_gndr]
					,[cst_create_date]
					,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS row_id
			FROM [Datawarehouse2].[bronze].[crm_cust_info])t
			WHERE row_id = 1;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'

			-- Loading silver.crm_prd_info
			SET @start_time = GETDATE();
			PRINT '******************************************';
			PRINT '>> Truncating Table: silver.crm_prd_info';
			PRINT '******************************************';
			TRUNCATE TABLE silver.crm_prd_info;
			PRINT '******************************************';
			PRINT '>> Inserting Data: silver.crm_prd_info';
			PRINT '******************************************';
			INSERT INTO silver.crm_prd_info(
				prd_id,
				cat_id,
				prd_key,
				prd_nm,
				prd_cost,
				prd_line,
				prd_start_dt,
				prd_end_dt
			)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
				SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
				prd_nm,
				ISNULL(prd_cost, 0) AS prd_cost,
				CASE UPPER(TRIM(prd_line))
					WHEN 'R' THEN 'Road'
					WHEN 'M' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line,
				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				DATEADD(day, -1, LEAD(CAST(prd_start_dt AS DATE)) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
			FROM bronze.crm_prd_info;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'

			-- Loading silver.crm_sales_details
			SET @start_time = GETDATE();
			PRINT '******************************************';
			PRINT '>> Truncating Table: silver.crm_sales_details';
			PRINT '******************************************';
			TRUNCATE TABLE silver.crm_sales_details;
			PRINT '******************************************';
			PRINT '>> Inserting Data: silver.crm_sales_details';
			PRINT '******************************************';
			INSERT INTO silver.crm_sales_details (
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price
			)
			SELECT
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
				END AS sls_order_dt,
				CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
				END AS sls_ship_dt,
				CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
					ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
				END AS sls_due_dt,
				CASE WHEN sls_sales IS NULL or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
				END AS sls_sales,
				sls_quantity,
				CASE WHEN sls_price IS NULL or sls_price <= 0
					THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'

			PRINT '------------------------------------------';
			PRINT '>> Loading ERP Tables';
			PRINT '------------------------------------------';
			-- Loading silver.erp_cust_az12
			SET @start_time = GETDATE();
			PRINT '******************************************';
			PRINT '>> Truncating Table: silver.erp_cust_az12';
			PRINT '******************************************';
			TRUNCATE TABLE silver.erp_cust_az12;
			PRINT '******************************************';
			PRINT '>> Inserting Data: silver.erp_cust_az12';
			PRINT '******************************************';
			INSERT INTO silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
			SELECT
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
					ELSE cid
				END AS cid,
				CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate 
				END AS bdate,
				CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
					 ELSE 'n/a'
				END AS gen
			FROM bronze.erp_cust_az12;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'

			-- Loading silver.erp_loc_a101
			SET @start_time = GETDATE();
			PRINT '******************************************';
			PRINT '>> Truncating Table: silver.erp_loc_a101';
			PRINT '******************************************';
			TRUNCATE TABLE silver.erp_loc_a101;
			PRINT '******************************************';
			PRINT '>> Inserting Data: silver.erp_loc_a101';
			PRINT '******************************************';
			INSERT INTO silver.erp_loc_a101(
			cid,
			cntry
			)

			SELECT 
				REPLACE(cid,'-','') AS cid,
				CASE WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
					 WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
					 WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry)) IS NULL THEN 'n/a'
					 ELSE TRIM(cntry)
				END AS cntry
			FROM bronze.erp_loc_a101;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'

			-- Loading silver.erp_px_cat_g1v2
			SET @start_time = GETDATE();
			PRINT '******************************************';
			PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
			PRINT '******************************************';
			TRUNCATE TABLE silver.erp_px_cat_g1v2;
			PRINT '******************************************';
			PRINT '>> Inserting Data: silver.erp_px_cat_g1v2';
			PRINT '******************************************';

			INSERT INTO silver.erp_px_cat_g1v2(
				id,
				cat,
				subcat,
				maintenance
			)
			SELECT
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2;
			SET @end_time = GETDATE();

			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '-------------'
			PRINT '==========================================';
			PRINT '>> Silver Layer Loaded';
			PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '==========================================';

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT 'ERROR OCCURRED WHILE LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'Error State' + CAST(ERROR_MESSAGE() AS NVARCHAR);
	END CATCH
END
