# Databricks notebook source




/FileStore/tables/sales_csv.txt
 /FileStore/tables/menu_csv.txt

# COMMAND ----------

# DBTITLE 1,Sales Data frame
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType 

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True),

])

sales_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/sales_csv.txt")

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Deriving year, month, quarter
from pyspark.sql.functions import month, year ,quarter

sales_df = sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df = sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df = sales_df.withColumn("order_quarter",quarter(sales_df.order_date))

display(sales_df)

# COMMAND ----------

# DBTITLE 1,Menu data frame
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType 

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("product_name",StringType(),True),
    StructField("price",StringType(),True),

])

menu_df = spark.read.format("csv").option("inferschema","true").schema(schema).load("/FileStore/tables/menu_csv.txt")

display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total amount spent by each customer
total_amount_spent= (sales_df.join(menu_df,'product_id').groupby('customer_id').agg({'price':'sum'})
                     .orderBy('customer_id'))

display(total_amount_spent)

# COMMAND ----------

# DBTITLE 1,Total  amount spent by each food category
total_amount_spent_food= (sales_df.join(menu_df,'product_id').groupby('product_name').agg({'price':'sum'})
                     .orderBy('product_name'))

display(total_amount_spent_food)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each month
sales_month= (sales_df.join(menu_df,'product_id').groupby('order_month').agg({'price':'sum'})
              .orderBy('order_month'))

display(sales_month)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each year
sales_year= (sales_df.join(menu_df,'product_id').groupby('order_year').agg({'price':'sum'})
              .orderBy('order_year'))

display(sales_year)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each quarter
sales_quarter= (sales_df.join(menu_df,'product_id').groupby('order_quarter').agg({'price':'sum'})
              .orderBy('order_quarter'))

display(sales_quarter)

# COMMAND ----------

# DBTITLE 1,times each product purchased
from pyspark.sql.functions import count

most_df = (sales_df.join(menu_df,'product_id').groupby('product_id','product_name').
           agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending= 0)
           .drop('product_id')
           )

display(most_df)
              

# COMMAND ----------

# DBTITLE 1,Top 5 order items
from pyspark.sql.functions import count

most_df_5 = (sales_df.join(menu_df,'product_id').groupby('product_id','product_name').
           agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending= 0)
           .drop('product_id').limit(5)
           )

display(most_df_5)

# COMMAND ----------

# DBTITLE 1,Top order item
from pyspark.sql.functions import count

most_df_1 = (sales_df.join(menu_df,'product_id').groupby('product_id','product_name').
           agg(count('product_id').alias('product_count'))
           .orderBy('product_count', ascending= 0)
           .drop('product_id').limit(1)
           )

display(most_df_1)

# COMMAND ----------

# DBTITLE 1,Frequency of the customer visited to restaurant
from pyspark.sql.functions import countDistinct

customer_freq = (sales_df.filter(sales_df.source_order == "Restaurant").groupBy('customer_id')
                 .agg(countDistinct('order_date'))
                 )

display(customer_freq)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each country
total_sales_country= (sales_df.join(menu_df,'product_id').groupby('location').agg({'price':'sum'}))

display(total_sales_country)

# COMMAND ----------

# DBTITLE 1,Total sales by order source
total_sales_source= (sales_df.join(menu_df,'product_id').groupby('source_order').agg({'price':'sum'}))

display(total_sales_source)