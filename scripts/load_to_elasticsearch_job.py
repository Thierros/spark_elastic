import sys
import logging
from utils import *

# config for logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():

    if len(sys.argv) != 3:
        print("Usage: spark-submit transformation_job.py <input_csv_path> <output_dir_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # init spark session
    logging.info("🚀 Initialisation de la session Spark...")
    spark = init_spark("Elastick_spark")

    try:
        # logging.info(f"📥 Loading data from {input_path}")
        # load data
        print(f"📥 Chargement des fichiers CSV depuis le répertoire : {input_path}")
        data = load_data(spark, input_path)
        print(f"📂 Fichiers chargés : {data.inputFiles()}")

        logging.info("🔄 Prétraitement des données...")
        # preprocess data
        data = preprocess_data(data)
        logging.info(f"💾 Sauvegarde des données transformées dans {output_path}")
        # save data
        save_data(data, output_path)
    except Exception as e:
        logging.error(f"❌ Erreur rencontrée : {e}", exc_info=True)
    finally:
        # close spark session
        logging.info("✅ Fermeture de la session Spark.")
        spark.stop()


if __name__ == "__main__":
    main()

