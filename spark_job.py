from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)
def load(date):
    
    try:
        spark=SparkSession.builder.appName("Implementing Spark on Hive")\
            .config("spark.sql.warehouse.dir","gs://spark_ex_airflow/assignment1/hive_data/").enableHiveSupport().getOrCreate()
        logger.info("Spark Session started...")

        input_path=f"gs://spark_ex_airflow/assignment1/data/employee-{date}.csv"
        logger.info("Input Path Obtained...")

        data=spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read..")

        filtered_data=data.filter(col("salary")>50000)
        logger.info("Data is Filtered...")

        hive_query1="CREATE DATABASE IF NOT EXISTS EMP_DB"
        spark.sql(hive_query1)
        logger.info("Hive DB Created...")

        hive_query2="USE EMP_DB"
        spark.sql(hive_query2)
        logger.info("EMP_DB in use...")

        hive_query3=''' CREATE TABLE IF NOT EXISTS filtered_employee(
                        emp_id INT,
                        emp_name STRING,
                        dept_id INT,
                        salary INT
                        )
                        STORED AS PARQUET
                    '''
        spark.sql(hive_query3)
        logger.info("Table employee created inside EMP_DB")

        filtered_data.write.mode("append").format("hive").saveAsTable("filtered_employee")
        logger.info("Data Appened to the table")

        # gcs_output_path=f"gs://spark_ex_airflow/assignment1/hive_data/employee-{date}.parquet"
        # filtered_data.write.mode("overwrite").parquet(gcs_output_path)
        # logger.info("Data added to GCS Bucket..")

    except Exception as e:
        logger.error(f"Error occurred due to: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session Stopped!")


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description="Date of the file")
    parser.add_argument("--date",required=True,help="Date of the todays file")

    args=parser.parse_args()
    load(date=args.date)