/*
===============================================================================
Load Bronze Layer
===============================================================================
    This script loads data into bronze schema for CSV files. 
    It truncates bronze tables before loading then sues BULK INSERT to load the CSV data into the tables.
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    -- to show wall time of outputs
    DECLARE @start_time DATETIME, @end_time DATETIME, @bronzelayer_start DATETIME, @bronzelayer_end DATETIME;
    BEGIN TRY
        -- BULK INSERTS for first file path (cust_info, sales_details, prd_info)
        SET @bronzelayer_start = GETDATE();
        PRINT '===================================';
        PRINT 'Loading bronze layer';
        PRINT '===================================';
        PRINT '+++++++++++++++++++++++++++++++++++++';
        PRINT 'Loading CRM tables';
        PRINT '+++++++++++++++++++++++++++++++++++++';
        SET @start_time = GETDATE();
        -- Drop and create staging table fresh
        DROP TABLE IF EXISTS bronze.stg_cust_info;
        CREATE TABLE bronze.stg_cust_info (
            cst_id VARCHAR(MAX),
            cst_key VARCHAR(MAX),
            cst_firstname VARCHAR(MAX),
            cst_lastname VARCHAR(MAX),
            cst_marital_status VARCHAR(MAX),
            cst_gndr VARCHAR(MAX),
            cst_create_date VARCHAR(MAX)
        );

        -- Bulk insert fresh data
        TRUNCATE TABLE bronze.stg_cust_info;
        BULK INSERT bronze.stg_cust_info
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        -- Drop and create final table fresh
        DROP TABLE IF EXISTS bronze.crm_cust_info;
        CREATE TABLE bronze.crm_cust_info (
            cst_id VARCHAR(MAX),
            cst_key VARCHAR(MAX),
            cst_firstname VARCHAR(MAX),
            cst_lastname VARCHAR(MAX),
            cst_marital_status VARCHAR(MAX),
            cst_gndr VARCHAR(MAX),
            cst_create_date DATE
        );
        
        -- Insert cleaned data
        SET NOCOUNT ON;
        INSERT INTO bronze.crm_cust_info (
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
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            TRY_CONVERT(DATE, REPLACE(REPLACE(cst_create_date, CHAR(13), ''), CHAR(10), ''))
        FROM bronze.stg_cust_info
        WHERE TRY_CONVERT(DATE, REPLACE(REPLACE(cst_create_date, CHAR(13), ''), CHAR(10), '')) IS NOT NULL;
        SET NOCOUNT OFF;
        DROP TABLE IF EXISTS bronze.stg_cust_info;
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';
        


        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';


        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';


        PRINT '+++++++++++++++++++++++++++++++++++++';
        PRINT 'Loading ERP tables';
        PRINT '+++++++++++++++++++++++++++++++++++++';
        -- BULK INSERT second file path (CUST_AZ12, LOC_A101, PX_CAT_G1V2)
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_LOC_A101;
        BULK INSERT bronze.erp_LOC_A101
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_CUST_AZ12;
        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';

        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'C:\Users\User\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Wall Time: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
        Print '---------';
        SET @bronzelayer_end = GETDATE();
        PRINT '==================================================================';
        PRINT 'Full Bronze Layer Wall Time: ' + CAST(DATEDIFF(second,@bronzelayer_start,@bronzelayer_end) AS NVARCHAR) + ' seconds'; 
        PRINT '==================================================================';
    END TRY
    BEGIN CATCH --if theres an error in code
        PRINT '==================================================================';
        PRINT 'ERROR OCCURED DURING BRONZE LAYER LOADING';
        PRINT 'Error Message' + ERROR_MESSAGE(); 
        PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR); 
        PRINT '==================================================================';
    END CATCH
END

