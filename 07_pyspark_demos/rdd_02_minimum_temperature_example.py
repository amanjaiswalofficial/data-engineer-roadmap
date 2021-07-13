from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("MinimumTemperature")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(",")
    station_id = fields[0]
    entity_type = fields[2]
    temperature = float(fields[3]) * 0.1 * (9 / 5) + 32
    return (station_id, entity_type, temperature)


base_path = "E:/Projects/data-engineering-learning-path/07_pyspark_demos"
lines = sc.textFile("{}/1800.csv".format(base_path))
parsed_lines = lines.map(parse_line)

min_temps = parsed_lines.filter(lambda x: "TMIN" in x[1])
station_temps = min_temps.map(lambda x: (x[0], x[2]))
min_temps = station_temps.reduceByKey(lambda x, y: min(x, y))
results = min_temps.collect()

for result in results:
    print(result)
