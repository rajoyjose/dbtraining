-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Set Up and Load Delta Tables
-- MAGIC
-- MAGIC After extracting data from external data sources, load data into the Lakehouse to ensure that all of the benefits of the Databricks platform can be fully leveraged.
-- MAGIC
-- MAGIC While different organizations may have varying policies for how data is initially loaded into Databricks, we typically recommend that early tables represent a mostly raw version of the data, and that validation and enrichment occur in later stages. This pattern ensures that even if data doesn't match expectations with regards to data types or column names, no data will be dropped, meaning that programmatic or manual intervention can still salvage data in a partially corrupted or invalid state.
-- MAGIC
-- MAGIC This lesson will focus primarily on the pattern used to create most tables, **`CREATE TABLE _ AS SELECT`** (CTAS) statements.
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC By the end of this lesson, you should be able to:
-- MAGIC - Use CTAS statements to create Delta Lake tables
-- MAGIC - Create new tables from existing views or tables
-- MAGIC - Enrich loaded data with additional metadata
-- MAGIC - Declare table schema with generated columns and descriptive comments
-- MAGIC - Set advanced options to control data location, quality enforcement, and partitioning
-- MAGIC - Create shallow and deep clones

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Run Setup
-- MAGIC
-- MAGIC The setup script will create the data and declare necessary values for the rest of this notebook to execute.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.4

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying Files
-- MAGIC In the cell below, we are going to run a query on a directory of parquet files. These files are not currently registered as any kind of data object (i.e., a table), but we can run some kinds of queries exactly as if they were. We can run these queries on many data file types, too (CSV, JSON, etc.).
-- MAGIC
-- MAGIC Most workflows will require users to access data from external cloud storage locations. 
-- MAGIC
-- MAGIC In most companies, a workspace administrator will be responsible for configuring access to these storage locations. In this course, we are simply going to use data files that the `Classroom-Setup` script above installed in our workspace.
-- MAGIC

-- COMMAND ----------

SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical` LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Table as Select (CTAS)
-- MAGIC
-- MAGIC We are going to create a table that contains historical sales data from a previous point-of-sale system. This data is in the form of parquet files.
-- MAGIC
-- MAGIC **`CREATE TABLE AS SELECT`** statements create and populate Delta tables using data retrieved from an input query. We can create the table and populate it with data at the same time.
-- MAGIC
-- MAGIC CTAS statements automatically infer schema information from query results and do **not** support manual schema declaration. 
-- MAGIC
-- MAGIC This means that CTAS statements are useful for external data ingestion from sources with well-defined schema, such as Parquet files and tables.

-- COMMAND ----------

CREATE OR REPLACE TABLE historical_sales_bronze 
  USING DELTA AS
    SELECT * FROM parquet.`${DA.paths.datasets}/ecommerce/raw/sales-historical`;

DESCRIBE historical_sales_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By running `DESCRIBE <table-name>`, we can see column names and data types. We see that the schema of this table looks correct.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Extracting CSV
-- MAGIC We also have data in the form of CSV files. The data files have a header row that contains column names and is delimited with a "|" (pipe) character. 
-- MAGIC
-- MAGIC We can see how this would present significant limitations when trying to ingest data from CSV files, as demonstrated in the cell below.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW sales_unparsed AS
  SELECT * FROM csv.`${da.paths.datasets}/ecommerce/raw/sales-csv`;

SELECT * FROM sales_unparsed LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## The `read_files()` Table-Valued Function
-- MAGIC
-- MAGIC The code in the next cell creates a table using CTAS. The `read_files()` table-valued function (TVF) allows us to read a variety of different file formats. Read more about it [here](https://docs.databricks.com/en/sql/language-manual/functions/read_files.html). The first parameter is a path to the data. The `Classroom-Setup` script (at the top of this notebook) instantiated an object that has a number of useful variables, including a path to our sample data.
-- MAGIC
-- MAGIC We are using these options:
-- MAGIC
-- MAGIC 1. `format => "csv"` -- Our data files are in the `CSV` format
-- MAGIC 1. `sep => "|"` -- Our data fields are separated by the | (pipe) character
-- MAGIC 1. `header => true` -- The first row of data should be used as the column names
-- MAGIC 1. `mode => "FAILFAST"` -- This will cause the statement to throw an error if there is any malformed data
-- MAGIC
-- MAGIC In this case, we are moving existing `CSV` data, but we could just as easily use other data types by using different options.
-- MAGIC
-- MAGIC A `_rescued_data` column is provided by default to rescue any data that doesn’t match the schema.

-- COMMAND ----------

DROP TABLE IF EXISTS sales_bronze;
CREATE TABLE sales_bronze 
  USING DELTA AS
    SELECT * FROM read_files("${da.paths.datasets}/ecommerce/raw/sales-csv",
      format => "csv",
      sep => "|",
      header => true,
      mode => "FAILFAST");

SELECT * FROM sales_bronze LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In the next cell, run `DESCRIBE EXTENDED` on our new table and see that:
-- MAGIC
-- MAGIC 1. The column names and data types were inferred correctly
-- MAGIC 1. The table was created in our catalog and the default schema, not `hive-metastore`. These were both created for us with the `Classroom-Setup` script
-- MAGIC 1. The table is MANAGED, and we can see a path to the data in the metastore's default location
-- MAGIC 1. The table is a Delta table.
-- MAGIC 1. You own the table. This is true of everything you create, unless you change the owner

-- COMMAND ----------

DESCRIBE EXTENDED sales_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Catalogs, Schemas, and Tables on Databricks
-- MAGIC We've created two tables so far: `historical_sales_bronze` and `sales_bronze`. But, we have not specified which schema (database) or catalog in which these tables should live. The `Classroom-Setup` script at the top of the notebook created a catalog for us and a schema. It then ran `USE` statements, so any table we create will live in the `default` schema, which lives in a catalog that is based on our username. 
-- MAGIC
-- MAGIC Running the next cell will show you information about the catalog that was created for you by the setup script above. Normally, you could just run `DESCRIBE CATALOG <catalog_name>`, but since your catalog name was generated for you, we are using the method, `DA.catalog_name` to get this name.  
-- MAGIC   
-- MAGIC Note: The DA object is only used in Databricks Academy courses and is not available outside of these courses.

-- COMMAND ----------

DESCRIBE CATALOG `${DA.catalog_name}`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Run the code below to see information about the schema (database) that was created for you. In the output below, the schema name is in the row called "Namespace Name." You can see that the schema was auto-created when the catalog was created.
-- MAGIC
-- MAGIC Note: The DA object is only used in Databricks Academy courses and is not available outside of these courses.

-- COMMAND ----------

DESCRIBE SCHEMA `${DA.schema_name}`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Managed and External Tables
-- MAGIC Databricks supports tables that copy data into the metastore associated with this Databricks workspace (managed tables), as well as tables that are simply registered with the metastore but do not copy data from object storage outside the metastore location (external tables). With external tables, data remains in its original location, but you can access it from within Databricks the same way as managed tables. In fact, once a table is created, you may not even care whether or not a table is managed or external. So far, all the tables we have created are managed tables. We will not be creating external tables in this course. You can learn about creating external tables [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-tables.html). 
-- MAGIC
-- MAGIC We have been creating our tables with data currently stored in dbfs (Databricks File System). We are copying data from dbfs in this course because of limitations in the learning environment. Databricks does not recommend creating a managed table or an external table using data in dbfs because using dbfs bypasses the Unity Catalog data governance model. In fact, if you attempt to create an external table using data in dbfs, you will receive an error.  
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Load Incrementally
-- MAGIC
-- MAGIC **`COPY INTO`** provides SQL engineers an idempotent option to incrementally ingest data from external systems.
-- MAGIC
-- MAGIC Note that this operation does have some expectations:
-- MAGIC - Data schema should be consistent
-- MAGIC - Duplicate records should try to be excluded or handled downstream
-- MAGIC
-- MAGIC This operation is potentially much cheaper than full table scans for data that grows predictably.
-- MAGIC
-- MAGIC We want to capture new data but not re-ingest files that have already been read. We can use `COPY INTO` to perform this action. 
-- MAGIC
-- MAGIC The first step is to create an empty table. We can then use COPY INTO to infer the schema of our existing data and copy data from new files that were added since the last time we ran `COPY INTO`.
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_bronze;
CREATE TABLE users_bronze USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC COPY INTO loads data from data files into a Delta table. This is a retriable and idempotent operation, meaning that files in the source location that have already been loaded are skipped.
-- MAGIC
-- MAGIC The cell below demonstrates how to use COPY INTO with a parquet source, specifying:
-- MAGIC 1. The path to the data. Note: For purposes of this course, the data is located in dbfs. This would not normally be the case. In the "real world," data would be located in another location, such as an object store or a Databricks volume.
-- MAGIC 1. The FILEFORMAT of the data, in this case, parquet.
-- MAGIC 1. COPY_OPTIONS -- There are a number of key-value pairs that can be used. We are specifying that we want to merge the schema of the data.

-- COMMAND ----------

COPY INTO users_bronze
  FROM '${DA.paths.datasets}/ecommerce/raw/users-30m'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC %md
-- MAGIC ## COPY INTO is Idempotent
-- MAGIC COPY INTO keeps track of the files it has ingested previously. We can run it again, and no additional data is ingested because the files in the source directory haven't changed. Let's run the `COPY INTO` command again to show this. 
-- MAGIC
-- MAGIC The count of total rows is the same as the `number_inserted_rows` above because no new data was copied into the table.

-- COMMAND ----------

COPY INTO users_bronze
  FROM '${DA.paths.datasets}/ecommerce/raw/users-30m'
  FILEFORMAT = parquet
  COPY_OPTIONS ('mergeSchema' = 'true');


SELECT count(*) FROM users_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating External Tables
-- MAGIC
-- MAGIC While Spark will extract some self-describing data sources efficiently using default settings, many formats will require declaration of schema or other options.
-- MAGIC
-- MAGIC External tables are tables whose data is stored outside of the managed storage location specified for the metastore, catalog, or schema. Use external tables only when you require direct access to the data outside of Databricks clusters or Databricks SQL warehouses.
-- MAGIC
-- MAGIC In order to provide access to an external storage location, a user with the necessary privileges must follow the instructions found [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-external-locations.html). Once the external location is properly configured, external tables can be created with code like this:
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC DROP TABLE IF EXISTS sales_csv;<br />
-- MAGIC CREATE TABLE sales_csv<br />
-- MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)<br />
-- MAGIC USING CSV<br />
-- MAGIC OPTIONS (<br />
-- MAGIC   header = "true",<br />
-- MAGIC   delimiter = "|"<br />
-- MAGIC )<br />
-- MAGIC LOCATION "<path-to-external-location>"<br />
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC Note the use of the **`LOCATION`** keyword, followed by a path to the pre-configured external location. When you run **`DROP TABLE`** on an external table, Unity Catalog does not delete the underlying data.
-- MAGIC
-- MAGIC Also note that options are passed with keys as unquoted text and values in quotes. Spark supports many <a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">data sources</a> with custom options, and additional systems may have unofficial support through external <a href="https://docs.databricks.com/libraries/index.html" target="_blank">libraries</a>. 
-- MAGIC
-- MAGIC **NOTE**: Depending on your workspace settings, you may need administrator assistance to load libraries and configure the requisite security settings for some data sources.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limits of Tables with External Data Sources
-- MAGIC
-- MAGIC By using our CTAS example and our `COPY INTO` example as we have so far, we are able to take full advantage of converting our CSV data into the Delta format. This allows us to take advantage of the performance guarantees associated with Delta Lake and the Databricks Data Intelligence Platform.
-- MAGIC
-- MAGIC If we were defining tables or queries against external data sources, we **cannot** expect the performance guarantees associated with Delta Lake and the Data Intelligence Platform.
-- MAGIC
-- MAGIC For example: While Delta Lake tables will guarantee that you always query the most recent version of your source data, tables registered against other data sources may represent older cached versions.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Built-In Functions
-- MAGIC
-- MAGIC Databricks has a vast [number of built-in functions](https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-builtin.html) you can use in your code.
-- MAGIC
-- MAGIC We are going to create a table for user data generated by the previous point-of-sale system, but we need to make some changes. The `first_touch_timestamp` is in the wrong format. We need to divide the timestamp that is currently in microseconds by 1e6 (1 million). We will then use `CAST` to cast the result to a [TIMESTAMP](https://docs.databricks.com/en/sql/language-manual/data-types/timestamp-type.html). Then, we `CAST` to [DATE](https://docs.databricks.com/en/sql/language-manual/data-types/date-type.html).
-- MAGIC
-- MAGIC Since we want to make changes to the `first_touch_timestamp` data, we need to use the `CAST` keyword. The syntax for `CAST` is `CAST(column AS data_type)`. We first cast the data to a `TIMESTAMP` and then to a `DATE`.  To use `CAST` with `COPY INTO`, we need to use a `SELECT` clause (make sure you include the parentheses) after the word `FROM` (in the `COPY INTO`).
-- MAGIC
-- MAGIC Our **`SELECT`** clause leverages two additional built-in Spark SQL commands useful for file ingestion:
-- MAGIC * **`current_timestamp()`** records the timestamp when the logic is executed
-- MAGIC * **`input_file_name()`** records the source data file for each record in the table
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS users_bronze;
CREATE TABLE users_bronze;
COPY INTO users_bronze FROM
  (SELECT *, 
    cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date, 
    current_timestamp() updated,
    input_file_name() source_file
  FROM '${da.paths.datasets}/ecommerce/raw/users-historical/')
  FILEFORMAT = PARQUET
  COPY_OPTIONS ('mergeSchema' = 'true');

SELECT * FROM users_bronze LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  
-- MAGIC Run the following cell to delete the tables and files associated with this lesson.

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC &copy; 2024 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the 
-- MAGIC <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/><a href="https://databricks.com/privacy-policy">Privacy Policy</a> | 
-- MAGIC <a href="https://databricks.com/terms-of-use">Terms of Use</a> | 
-- MAGIC <a href="https://help.databricks.com/">Support</a>
