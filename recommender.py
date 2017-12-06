from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit


if __name__ == "__main__":
  spark = SparkSession.builder.appName("MovieRecommendations").getOrCreate()
  
  # Kill Spark session
  spark.stop()
