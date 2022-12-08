import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc

spark = SparkSession.builder.appName("test").getOrCreate()
sc = spark.sparkContext

columns = StructType(
    [
        StructField("Planet_Name", StringType(), False),
        StructField("Resident", StringType(), False),
    ]
)

planets = []
residents = []


def populate_planet_data(data):
    if data is not None:
        for planet in data["results"]:
            for resident in planet["residents"]:
                planets.append(planet["name"])
                residents.append(resident)


def populate_planets(data):
    populate_planet_data(data)
    next_link = data["next"]
    while next_link is not None:
        data = requests.get(next_link).json()
        populate_planet_data(data)
        next_link = data["next"]


def main():
    data = requests.get("https://swapi.dev/api/planets/").json()
    populate_planets(data)
    df_planet = spark.createDataFrame(zip(planets, residents), columns)
    df_planet.show(20, False)
    df_planet.groupBy("Planet_Name").count().sort(desc("count")).show()
    print(
        df_planet.groupBy("Planet_Name").count().sort(desc("count")).toJSON().collect()
    )


if __name__ == "__main__":
    main()
