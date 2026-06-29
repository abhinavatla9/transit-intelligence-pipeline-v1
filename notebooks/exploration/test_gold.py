import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from processing.transformations.convert_to_silver import get_spark_sess
from processing.transformations.gold_convert import calculate_route_metrics

print("claculating route metrics...")
calculate_route_metrics()
print("Reading gold layer...")

spark = get_spark_sess()
df = spark.read.parquet("s3a://gold/BART/route_metrics")
df.printSchema()
df.show(truncate=False)
print("Total routes: ", df.count())
spark.stop()