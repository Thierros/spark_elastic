import sys
import logging
from utils import *

# config for logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():

    if len(sys.argv) != 4:
        print("Usage: spark-submit load_to_elasticsearch_job.py <input_csv_path> <output_dir_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    # output_path = sys.argv[2]
    es_index = sys.argv[2]
    mapping_file = sys.argv[3]
    es_host = "localhost"
    es_port = "9200"


    # init spark session
    logging.info("🚀 Spark session initialization...")
    spark = init_spark("Elastick_spark")

    try:
        # load data
        print(f"📥 Loading data from csv files from : {input_path}")
        data = load_data(spark, input_path)
        print(f"📂 Fichiers chargés : {data.inputFiles()}")

        # preprocess data
        logging.info("🔄 data processing...")
        data = preprocess_data(data)

        # logging.info(f"💾 Sauvegarde des données transformées dans {output_path}")
        # save data
        # save_data(data, output_path)

        # create the index
        create_index(es_host, es_port, es_index, mapping_file)

        # Indexation dans Elasticsearch
        logging.info(f"📤 Indexation des données dans Elasticsearch ({es_index})...")
        save_to_elasticsearch(data, es_host, es_port, es_index)
    except Exception as e:
        logging.error(f"❌ Erreur rencontrée : {e}", exc_info=True)
    finally:
        # close spark session
        logging.info("✅ Fermeture de la session Spark.")
        spark.stop()


if __name__ == "__main__":
    main()

