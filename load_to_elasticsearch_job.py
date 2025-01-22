from pyspark.sql import SparkSession
import pyspark.sql.functions as f

es_host = 'localhost'
es_port = 9200

spark = (SparkSession.builder
         .appName("Load to Elasticsearch")
        .config("spark.es.nodes": es_host)
        .comfig("es.port", es_port)
         .getOrCreate())

# params: input_data,  es_resource, es_resource_mapping, output_data

data = spark.read.csv("input_data.csv", header=True)

