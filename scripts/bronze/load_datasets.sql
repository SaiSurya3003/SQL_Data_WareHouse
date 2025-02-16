/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze) in MySQL
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' database from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA INFILE` command to load data from CSV files.

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL load_bronze();
===============================================================================
*/

DELIMITER $$

CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        SELECT '==========================================' AS Message;
        SELECT 'ERROR OCCURRED DURING LOADING BRONZE LAYER' AS Message;
        SELECT 'Error Code:', SQLSTATE, 'Error Message:', MESSAGE_TEXT 
        FROM information_schema.INNODB_TRX;
        SELECT '==========================================' AS Message;
    END;

    SET batch_start_time = NOW();
    SELECT '================================================' AS Message;
    SELECT 'Loading Bronze Layer' AS Message;
    SELECT '================================================' AS Message;

    -- Use bronze database
    USE bronze;

    -- Load CRM Tables
    SELECT '------------------------------------------------' AS Message;
    SELECT 'Loading CRM Tables' AS Message;
    SELECT '------------------------------------------------' AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: crm_cust_info' AS Message;
    TRUNCATE TABLE crm_cust_info;
    SELECT '>> Inserting Data Into: crm_cust_info' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_crm/cust_info.csv'
    INTO TABLE crm_cust_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: crm_prd_info' AS Message;
    TRUNCATE TABLE crm_prd_info;
    SELECT '>> Inserting Data Into: crm_prd_info' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_crm/prd_info.csv'
    INTO TABLE crm_prd_info
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: crm_sales_details' AS Message;
    TRUNCATE TABLE crm_sales_details;
    SELECT '>> Inserting Data Into: crm_sales_details' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_crm/sales_details.csv'
    INTO TABLE crm_sales_details
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    -- Load ERP Tables
    SELECT '------------------------------------------------' AS Message;
    SELECT 'Loading ERP Tables' AS Message;
    SELECT '------------------------------------------------' AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: erp_loc_a101' AS Message;
    TRUNCATE TABLE erp_loc_a101;
    SELECT '>> Inserting Data Into: erp_loc_a101' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_erp/LOC_A101.csv'
    INTO TABLE erp_loc_a101
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: erp_cust_az12' AS Message;
    TRUNCATE TABLE erp_cust_az12;
    SELECT '>> Inserting Data Into: erp_cust_az12' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE erp_cust_az12
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    SET start_time = NOW();
    SELECT '>> Truncating Table: erp_px_cat_g1v2' AS Message;
    TRUNCATE TABLE erp_px_cat_g1v2;
    SELECT '>> Inserting Data Into: erp_px_cat_g1v2' AS Message;
    LOAD DATA INFILE 'G:/Projects/SQL_Data_Warehouse_Project/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE erp_px_cat_g1v2
    FIELDS TERMINATED BY ','
    IGNORE 1 ROWS;
    SET end_time = NOW();
    SELECT CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS Message;

    SET batch_end_time = NOW();
    SELECT '==========================================' AS Message;
    SELECT 'Loading Bronze Layer is Completed' AS Message;
    SELECT CONCAT('   - Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS Message;
    SELECT '==========================================' AS Message;
END $$

DELIMITER ;