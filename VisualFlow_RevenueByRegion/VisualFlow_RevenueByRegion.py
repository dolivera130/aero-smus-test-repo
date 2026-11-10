import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min, sum, count

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# Script generated for node CatalogDataSource
CatalogDataSource_1794327828637 = spark.sql("select * from `orders_analytics_db`.`sales_raw`")
# Script generated for node AggregateTransform
AggregateTransform_1794327918188 = CatalogDataSource_1794327828637.groupBy("region").agg(sum("total_amount").alias("revenue"))
def get_table_columns(spark, database_name, table_name):
    schema_df = spark.sql(f"DESCRIBE `{database_name}`.`{table_name}`").collect()
    # Check if '# Partition Information' is present
    partition_info_exists = any(row.col_name == '# Partition Information' for row in schema_df)
    if partition_info_exists:
        # Find the index where partition information starts
        partition_index = next(i for i, row in enumerate(schema_df) if row.col_name == '# Partition Information')
        all_columns = [row.col_name for row in schema_df[:partition_index] if row.col_name]
    else:
        all_columns = [row.col_name for row in schema_df if row.col_name]
    return all_columns
# Script generated for node CatalogDataSink
all_columns = get_table_columns(spark, "orders_analytics_db", "revenue_by_region")
# enforce the column number and column order by getting all column names from DESCRIBE TABLE query and doing a select first.
assert len(AggregateTransform_1794327918188.columns) == len(all_columns), "the input df has different col number than the target catalog table."
AggregateTransform_1794327918188.select(all_columns).write.insertInto("`orders_analytics_db`.`revenue_by_region`")
