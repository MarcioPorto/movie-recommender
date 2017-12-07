# Spark Movie Recommender

This is a simple script for a movie recommender system using Apache Spark.

This is my version of the code on [this](https://www.udemy.com/the-ultimate-hands-on-hadoop-tame-your-big-data/) Udemy course.

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

For those who prefer, the `MovieRecommender.ipynb` file contains a Jupyter version of the code.

### Arguments

You can pass in the id of the user who you want predictions for, by running:

```
python recommender.py _id_
```

## Requirements

Make sure you have Java installed. For Ubuntu 16.04, I was able to install it by following [this guide](https://www.digitalocean.com/community/tutorials/how-to-install-java-with-apt-get-on-ubuntu-16-04).