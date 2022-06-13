from pyspark.sql import SparkSession, Window, functions as F

spark = SparkSession.builder.getOrCreate()

input_path =
output_path =

df = spark.read.parquet(input_path)

window_part_by_entity_id = Window.partitionBy('entity_id')
window_part_by_entity_month_ids_order_by_item_id = (Window
                                                    .partitionBy('entity_id', 'month_id')
                                                    .orderBy('item_id'))

(df
 .withColumn('total_signals',
             F.sum('signal_count').over(window_part_by_entity_id))
 .withColumn('min_month_id',
             F.min('month_id').over(window_part_by_entity_id))
 .withColumn('max_month_id',
             F.max('month_id').over(window_part_by_entity_id))
 .filter((F.col('month_id') == F.col('min_month_id')) | (F.col('month_id') == F.col('max_month_id')))
 .withColumn('row_number',
             F.row_number().over(window_part_by_entity_month_ids_order_by_item_id))
 .filter(F.col('row_number') == 1)
 .withColumn('oldest_item_id',
             F.first('item_id').over(window_part_by_entity_id))
 .withColumn('newest_item_id',
             F.last('item_id').over(window_part_by_entity_id))
 .drop('item_id', 'source', 'month_id', 'signal_count', 'min_month_id', 'max_month_id', 'row_number')
 .select('entity_id', 'oldest_item_id', 'newest_item_id', 'total_signals')
 .distinct()
 .orderBy('entity_id')
 .coalesce(1)
 .write
 .parquet(output_path, mode='overwrite'))
