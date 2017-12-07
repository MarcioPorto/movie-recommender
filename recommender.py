import sys

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit


def load_movie_names():
  """ Loads a dictionary of movie_ids as keys and the corresponding movie_name
      as the value.
  """
  movie_names = dict()
  with open("ml-100k/u.item", encoding="ISO-8859-1") as f:
    for line in f:
      fields = line.split("|")
      movie_names[int(fields[0])] = fields[1]
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
  # Get command line arguments
  if len(sys.argv) == 2:
    user = int(sys.argv[1])
  else:
    user = 1

  spark = SparkSession.builder.appName("MovieRecommendations").getOrCreate()

  # Set log level
  spark.sparkContext.setLogLevel("FATAL")

  movie_names = load_movie_names()

  lines = spark.read.text("ml-100k/u.data").rdd

  # Converts the input data into an RDD
  ratings_rdd = lines.map(parse_input)

  # Converts previous RDD into a dataframe
  # This will be often reused, so it's a good idea to make sure it's cached
  ratings = spark.createDataFrame(ratings_rdd).cache()

  # ALS initialization
  als = ALS(
    maxIter=5, regParam=0.01, userCol="user_id", itemCol="movie_id", ratingCol="rating"
  )

  # Fit the model to the ratings data
  print("\nStarting to fit model...")
  model = als.fit(ratings)
  print("Finished fitting model.")

  # Print all ratings for the user
  print("\nRatings for user ID %s:" % user)
  user_ratings = ratings.filter("user_id = %s" % user)
  for rating in user_ratings.collect():
    print(
      "%s - %s" % (
        rating["rating"],
        movie_names[rating["movie_id"]]
      )
    )

  # Only pick movies that have been rated more than 100 times
  rating_counts = ratings.groupBy("movie_id").count().filter("count > 100")

  # Run model on a list containing all movies rated more than 100 times
  print("\nRunning recommender...")
  recommendations = model.transform(
    rating_counts.select("movie_id").withColumn("user_id", lit(user))
  )

  # Get the top 20 recommendations
  top_recommendations = recommendations.sort(
    recommendations.prediction.desc()
  ).take(20)

  # Print these recommendations
  print("\nTop 20 recommendations for user %s:" % user)
  for recommendation in top_recommendations:
    print(
      "%s - %s" % (
        recommendation["prediction"], 
        movie_names[recommendation["movie_id"]]
      )
    )

  # Kill Spark session
  spark.stop()
