from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit


def load_movie_names():
  """ Loads a dictionary of movie_ids as keys and the corresponding movie_name
      as the value.
  """
  movie_names = dict()
  with open("ml-100k/u.item") as f:
    for line in f:
      fields = line.split("|")
      movie_names[int(fields[0])] = fields[1].decode("ascii", "ignore")
  return movie_names

def parse_input(line):
  """ Transforms a line of data into a Row object.
  """
  fields = line.value.split()
  return Row(
    user_id   = int(fields[0]),
    movie_id  = int(fields[1]),
    rating    = int(fields[2])
  )

if __name__ == "__main__":
  spark = SparkSession.builder.appName("MovieRecommendations").getOrCreate()

  # Kill Spark session
  spark.stop()
