# Spark Movie Recommender

This is a simple script for a movie recommender system using Apache Spark.

## Download data

I am using the [MovieLends 100K Dataset](https://grouplens.org/datasets/movielens/100k/) for this project. Click the previous link and download the data. Once you unzip the contents, create a folder called `ml-100k`. Move the `u.data` and `u.item` files inside of that new folder.

## Run the script

On Hadoop:

```
spark-submit recommender.py
```

On local computer:

```
python recommender.py
```