/* WARNING:
  Running this script will delete the DataWarehouse database if it already exists.
  This script crewates a database called DataWarehouse and creates 3 schemas: bronze, silver and gold. 
*/
USE master;

-- Creating database
CREATE DATABASE DataWarehouse;
GO
USE DataWarehouse;
GO

-- Creating schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
