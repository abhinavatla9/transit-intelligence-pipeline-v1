import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from processing.transformations.convert_to_silver import get_spark_sess

spark = get_spark_sess()
df = spark.read.parquet("s3a://silver/BART/trip_updates")
df.show(5)
print("Row Count", df.count())