# ETL with Scala and Spark

This project aims to build an ETL from the public Dota API to measeure diverse KPIs from a player: https://www.opendota.com/

You can find a SBT project with a Docker image, and a Jupyter Notebook.

To run the SBT:

- Option 1 (preferred): If you have SBT installed just type sbt in the terminal:

```
$/sparkers> sbt
```

Afterwards type run and add the number of matches to be evaluated.

```
sbt:sparkers> run 20
```

You will see a Spark dataframe with the solution. The JSON file can be found at src/main/scala/output

```
+----+-----------+-----------+-------+-------+-------+------+------+------+
|game|player_name|total_games|max_kda|min_kda|avg_kda|max_kp|min_kp|avg_kp|
+----+-----------+-----------+-------+-------+-------+------+------+------+
|Dota|   YrikGood|         20|   18.0|    0.5|   3.19| 73.33| 17.78| 47.82|
+----+-----------+-----------+-------+-------+-------+------+------+------+
```
- Option 2 (using Docker): Build the docker image with:

```
$> docker build -t sparkers .
```
and then:

```
$> docker run sparkers
```

Option 3 (Jupyter notebook): If you have the almond kernel installed just type jupyter notebook in the sparkers-jupyter directory.
If you don't, just use docker:

```
$/sparkers-jupyter> docker run -it --rm -p 8888:8888 -p 4040:4040 -m 4g -v "$PWD":/home/jovyan/work almondsh/almond:0.9.1
```
