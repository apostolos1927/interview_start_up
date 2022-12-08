import asyncio
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import desc
import aiohttp

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


async def populate_planet_data(data):
    if data is not None:
        for planet in data["results"]:
            for resident in planet["residents"]:
                planets.append(planet["name"])
                residents.append(resident)


async def run(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=False) as resp:
            data = await resp.json()
            await populate_planet_data(data)
            next_link = data["next"]
            while next_link is not None:
                async with session.get(next_link, ssl=False) as resp:
                    data = await resp.json()
                    populate_planet_data(data)
                    next_link = data["next"]


async def main():

    await run("https://swapi.dev/api/planets/")
    df_planet = spark.createDataFrame(zip(planets, residents), columns)
    df_planet.show(10, False)
    df_planet.groupBy("Planet_Name").count().sort(desc("count")).show()
    print(
        df_planet.groupBy("Planet_Name").count().sort(desc("count")).toJSON().collect()
    )


if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
