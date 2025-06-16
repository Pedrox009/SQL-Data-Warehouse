/*
===============================================================================
Load Silver Layer
===============================================================================
    This script loads data into silver schema from the bronze schema.
    It truncates silver tables before loading then inserts bronze date to silver tables.
    To use: EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @full_start_time DATETIME, @full_end_time DATETIME;
	BEGIN TRY
		SET @full_start_time = GETDATE();
		PRINT '==============================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================';
		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';
	
		SET @start_time = GETDATE();
		-- first csv
		TRUNCATE TABLE silver.crm_cust_info;
		INSERT INTO silver.crm_cust_info (
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
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			ELSE 'n/a'
		END cst_marital_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END cst_gndr,
		cst_create_date
		FROM(
		SELECT *,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		)t WHERE flag_last = 1; 
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		--second csv
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_prd_info;
		INSERT INTO silver.crm_prd_info (
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
		REPLACE(SUBSTRING(prd_key,1,5),',','_') AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test --due to overlaps in output dates
		FROM bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'


		-- sales csv
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.crm_sales_details;
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
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity*ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <=0
				THEN sls_price/NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';
		-- cust csv
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_CUST_AZ12;
		INSERT INTO silver.erp_CUST_AZ12 (cid,bdate,gen)
		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'n/a'
		END AS gen
		FROM bronze.erp_CUST_AZ12;
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		--loc csv
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_LOC_A101;
		INSERT INTO silver.erp_LOC_A101 (cid,cntry)
		SELECT
		REPLACE(cid,'-','') cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_LOC_A101;
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		-- cat csv
		SET @start_time = GETDATE();
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		INSERT INTO silver.erp_PX_CAT_G1V2 
		(id, cat, subcat, maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		PRINT '>> Wall Time: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'

		SET @full_end_time = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Silver Layer Completed';
		PRINT 'Total Wall Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' seconds'
		PRINT '=======================================';
	END TRY
	BEGIN CATCH
		PRINT '=======================================';
		PRINT 'ERROR OCCURED DURING LOADING';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================';
	END CATCH
END
