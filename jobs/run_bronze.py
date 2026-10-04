import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from jobs.bronze_ingestion import BronzeIngestor
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName('Bronze_ETL').master('spark://spark-master:7077').getOrCreate()
    ingestor = BronzeIngestor(spark, sys.argv[1])
    for t in ['orders', 'lineitem', 'customer']:
        ingestor.run(t)
    spark.stop()

if __name__ == "__main__":
    main()
