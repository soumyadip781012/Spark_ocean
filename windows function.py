#row_number() window function is used to give the sequential row number starting from 1 to the result of each window partition


 Window_df = Window.partitionBy("department").orderBy("salary") 
 df.withColumn("row_number",row_number.over(Window_df)).show()
 
 
 #rank() window function is used to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties. 
 
 df.withColumn("rank",rank().over(window_df)) .show()
 
#dense_rank() window function is used to get the result with rank of rows within a window partition without any gaps. 
#This is similar to rank() function difference being rank function leaves gaps in rank when there are ties. //dens_rank 

df.withColumn("dense_rank",dense_rank().over(windowSpec)).show()
 
 #percent_rank 
 
 df.withColumn("percent_rank",percent_rank().over(windowSpec)).show()
 
 #ntile() window function returns the relative rank of result rows within a window partition.
 #In below example we have used 2 as an argument to ntile hence it returns ranking between 2 values (1 and 2) //ntile 
 
 df.withColumn("ntile",ntile(2).over(windowSpec)).show()
 
 #how to calculate sum, min, max for each department using Spark SQL Aggregate window functions and WindowSpec.
 #When working with Aggregate functions, we don’t need to use order by clause. 
 
 window = Window.partitionBy("department").orderBy("salary")
specAgg = Window.partitionBy("department") 

aggDF = df.withColumn("avg", avg(col("salary")).over(specAgg)).withColumn("sum", sum(col("salary"))
       .over(specAgg)).withColumn("min", min(col("salary"))
       .over(specAgg)) .withColumn("max", max(col("salary"))
       .over(specAgg)) 
       .where(col("row")===1).select("department","avg","sum","min","max").show()
       
       
       
#collect_list() function returns all values from an input column with duplicates //collect_list 

df.select(collect_list("salary")).show(false) 
collect_list(salary)

#collect_set() function returns all values from an input column with duplicate values eliminated. //collect_set 

df.select(collect_set("salary")).show(false)

#countDistinct() function returns the number of distinct elements in a columns //countDistinct 

df2 = df.select(countDistinct("department", "salary")) df2.show(false)

#first function() first() function returns the first element in a column when ignoreNulls is set to true, 
#it returns the first non-null element. //first 

df.select(first("salary")).show(false)

#last() function returns the last element in a column. when ignoreNulls is set to true, 
#it returns the last non-null element. //last 

df.select(last("salary")).show(false)

#first() function returns the first element in a column when ignoreNulls is set to true, 

#it returns the first non-null element. //first 

df.select(first("salary")).show(false)
       
       
    
       