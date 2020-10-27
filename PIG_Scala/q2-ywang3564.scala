// Databricks notebook source
// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
val customSchema = StructType(Array(StructField("lpep_pickup_datetime", StringType, true), StructField("lpep_dropoff_datetime", StringType, true), StructField("PULocationID", IntegerType, true), StructField("DOLocationID", IntegerType, true), StructField("passenger_count", IntegerType, true), StructField("trip_distance", FloatType, true), StructField("fare_amount", FloatType, true), StructField("payment_type", IntegerType, true)))
// display(dbutils.fs.ls("/FileStore/tables"))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX. Edit the filepath on line 7 (.load(...)) to point to your uploaded file
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "true") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/nyc_tripdata-069c2.csv") // UPDATE this line with your filepath. Refer Databricks Setup Guide Step 3.
   .withColumn("pickup_datetime", from_unixtime(unix_timestamp(col("lpep_pickup_datetime"), "MM/dd/yyyy HH:mm")))
   .withColumn("dropoff_datetime", from_unixtime(unix_timestamp(col("lpep_dropoff_datetime"), "MM/dd/yyyy HH:mm")))
   .drop($"lpep_pickup_datetime")
   .drop($"lpep_dropoff_datetime")

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Some commands that you can use to see your dataframes and results of the operations. You can alternatively uncomment the display() and show() functions to see the data differently. These two functions will be useful in reporting the results.

//display(df) //display in a tabular format for easy download

//df.show(5) // view the first 5 rows of the dataframe

// COMMAND ----------

// BEFORE YOU BEGIN: Replace gburdell3 with your GT username.
val gt_username = "ywang3564"
println(gt_username)

// COMMAND ----------

// PART 1: Filter the data to only keep the rows where "PULocationID" and the "DOLocationID" are different and the "trip_distance" is strictly greater than 2.0 (>2.0).
// Hint: Checkout the filter() function.

// VERY VERY IMPORTANT: ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

// ENTER THE CODE BELOW
val fdf = df.filter("trip_distance > 2")
val filtered_df = fdf.filter("PULocationID != DOLocationID")
// df.show(5)
// fdf.show(5)
// filtered_df.show(5)

// COMMAND ----------

// PART 2: The top-5 most popular drop locations - "DOLocationID", sorted in descending order - if there is a tie, then one with lower "DOLocationID" gets listed first
// Hint: Checkout the groupBy(), orderBy() and count() functions.

// ENTER THE CODE BELOW
val count = filtered_df.groupBy("DOLocationID").count()
// count.show(5)
val sorted = count.sort(desc("count"), asc("DOLocationID"))
sorted.show(5)

// COMMAND ----------

// PART 3: The top-5 most popular pickup locations - "PULocationID", sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first 

// ENTER THE CODE BELOW
val count = filtered_df.groupBy("PULocationID").count()
// count.show(5)
val sorted = count.sort(desc("count"), asc("PULocationID"))
sorted.show(5)

// COMMAND ----------

// PART 4: The top-5 most popular pickup-dropoff pairs - sorted in descending order - if there is a tie, then one with lower "PULocationID" gets listed first.

// ENTER THE CODE BELOW
val count = filtered_df.groupBy("PULocationID","DOLocationID").count()
// count.show(5)
val sorted = count.sort(desc("count"), asc("PULocationID"))
sorted.show(5)

// COMMAND ----------

// PART 5: Number of dropoffs over the period from January 1, 2019 (inclusive of January 1) to January 5, 2019 (inclusive of January 5). List the entries by day from January 1 to January 5.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint 1: Observe how we extracted the date from the timestamp in the thrid cell.
// Hint 2: Filter by month as well since there are a few dates for the month of February present in the dataset.

// ENTER THE CODE BELOW

// df.filter("pickup_datetime >= 2019-01-01").show(5)
// unix_timestamp
// filtered_df.select("pickup_datetime",unix_timestamp("pickup_datetime","dd.MM.yyyy HH:mm:ss").alias("A_in_unix_time")).show(5)
// filtered_df.select(
//       col("pickup_datetime"),
//       unix_timestamp(col("pickup_datetime"), "MM/dd/yyyy HH:mm")
//     ).show()
val df_withdate = filtered_df.select(
      col("pickup_datetime"),
      col("dropoff_datetime"),
      year(col("pickup_datetime")).as("year"),
      month(col("pickup_datetime")).as("month"),
      dayofmonth(col("pickup_datetime")).as("dayofmonth"),
      date_trunc("Day",col("pickup_datetime")).as("DropDay")
)
// df_withdate.show(5)
val count = df_withdate.filter( df_withdate("year") === 2019 && df_withdate("month") === 1 && df_withdate("dayofmonth") >=1 && df_withdate("dayofmonth") <= 5 ).groupBy("DropDay").count()
count.sort(asc("DropDay")).show()

// COMMAND ----------

// PART 6: List the top-3 locations with the maximum overall activity, i.e. sum of all pickups and all dropoffs at that LocationID. In case of a tie, the lower LocationID gets listed first.
// Hint: Checkout join() and na.drop() functions. You will need to perform a join operation between two dataframes which you created in earlier parts to get the result.

// ENTER THE CODE BELOW
val df1 = filtered_df.select(
  col("DOLocationID").as("ID")
)
val df2 = filtered_df.select(
  col("PULocationID").as("ID")
)
val merge = df1.union(df2)
val count = merge.groupBy("ID").count()
// count.show(5)
val sorted = count.sort(desc("count"), asc("ID")).show(3)
// count1.join(count2, count2("PULocationID") === count1("DOLocationID")).show(10)


// COMMAND ----------


