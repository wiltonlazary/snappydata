# How to Export and Restore Table Data using HDFS

In SnappyData, table data is stored in memory and on disk (depending on the configuration). As SnappyData supports Spark APIs, table data can be exported to HDFS using Spark APIs. This can be used to backup your tables to HDFS. 

!!! Tip
	When performing a backup of your tables to HDFS, it is a good practice to export data during a period of low activity in your system. The export does not block any activities in the distributed system, but it does use file system resources on all hosts in your distributed system and can affect performance.

For example, as shown below you can create a DataFrame for a table and save it as parquet file.

```pre
// created a DataFrame for table "APP.CUSTOMER"
val df = snappySession.table("APP.CUSTOMER")
// save it as parquet file on HDFS
df.write.parquet("hdfs://127.0.0.1:9000/customer")
```

Refer to [How to Run Spark Code inside the Cluster](run_spark_job_inside_cluster.md) to understand how to write a SnappyData job. The above can be added to the `runSnappyJob()` function of the SnappyData job.

You can also import this data back into SnappyData tables.

For example using SQL, create an external table and import the data:

```pre
snappy> CREATE EXTERNAL TABLE CUSTOMER_STAGING_1 USING parquet OPTIONS (path 'hdfs://127.0.0.1:9000/customer', header 'true', inferSchema 'true');
snappy> insert into customer select * from CUSTOMER_STAGING_1;
```

Or by using APIs (as a part of SnappyData job). Refer to [How to Run Spark Code inside the Cluster](run_spark_job_inside_cluster.md) for more information. 

```pre
// create a DataFrame using parquet 
val df2 = snappySession.read.parquet("hdfs://127.0.0.1:9000/customer")
// insetert the data into table
df2.write.mode(SaveMode.Append).saveAsTable("APP.CUSTOMER")
```

!!!Note	
	Snappydata supports  kerberized Hadoop cluster in Smart connector mode only. You need to set **HADOOP_CONF_DIR** in **spark-env.sh** and **snappy-env.sh**. Currently the Embedded mode(Snappy job and Snappy shell) and Smart Connector with standalone mode are NOT supported. Smart connector with local and YARN mode are supported.
