from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import requests
import json

def init_spark(app_name):
    """Init spark session"""
    return SparkSession.builder \
    .appName(app_name) \
    .getOrCreate()

def load_data(spark, input_path):
    """Load data from the input path"""
    return spark.read.option("header", True).option("inferSchema", True).csv(f"{input_path}/*.csv")

def cast_cols(df, cols, dtype="string"):
    return df.select([f.col(col).cast(dtype).alias(col) if col in cols else f.col(col) for col in df.columns])
    # for column in cols:
    #     df = df.withColumn(column, f.col(column).cast(dtype))
    # return df


def preprocess_data(df):
    """Data preprocessing"""
    integer_cols = ["Transaction_Hour", "Quantity", "Customer_Age", "Account_Age_Days"]
    float_cols = ["Transaction_Amount"]
    date_cols = ["transaction_date"]
    string_cols = ["Payment_Method", "Product_Category", "Device_Used", "Is_Fraudulent"]

    df = cast_cols(df, integer_cols, "integer")
    df = cast_cols(df, float_cols, "float")
    # df = df.withColumn("transaction_date", f.to_date(f.col("transaction_date"), "yyyy-MM-dd"))
    df = df.withColumn("transaction_date", f.date_format(f.col("transaction_date"), "yyyy-MM-dd"))

    df = df.select(string_cols + date_cols + integer_cols + float_cols)
    df = df.toDF(*[col.lower() for col in df.columns])

    # group data
    group_cols = [col.lower() for col in date_cols + string_cols]
    sum_cols = [c for c in df.columns if c not in group_cols]
    df = df.groupBy(group_cols).agg(
        *[f.sum(f.col(c)).alias(c) for c in sum_cols],
        f.count("*").alias("nb_transaction")
    )
    df = df.withColumn("transaction_amount", f.round(f.col("transaction_amount"), 2))

    return df

def save_data(df, output_path):
    """Save the processed data"""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

def create_index(es_host, es_port, es_index, mapping_file):
    """Create an Elasticsearch index with specified mapping json file"""
    url = f"http://{es_host}:{es_port}/{es_index}"
    if requests.head(url).status_code == 200:
        print(f"📌 Index {url} already exists")
        return
    with open(mapping_file, "r") as f:
        mapping = json.load(f)

    response = requests.put(url, headers={"Content-Type": "application/json"}, data=json.dumps(mapping))
    if response.status_code == 200:
        print(f"✅ Index {url} created")
    else:
        print(f"❌ Index {url} failed, Error: {response.text}")

def save_to_elasticsearch(df, host, port, es_index):
    """index processed data to elasticsearch"""
    es_conf = {
        "es.nodes": host,
        "es.port": port,
        "es.resource": f"{es_index}",
        "es.nodes.wan.only": "true",
        "es.http.timeout": "5m",
        "es.batch.size.entries": "5000"
    }

    try:
        df.write.format("org.elasticsearch.spark.sql").options(**es_conf).mode("overwrite").save(f"{es_index}/")
    except Exception as e:
        print(e)