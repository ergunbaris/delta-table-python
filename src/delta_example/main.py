from pyspark.sql import SparkSession
from delta import *
from delta import configure_spark_with_delta_pip


def create_spark_session():
    builder =  SparkSession.builder \
        .appName("DeltaExample") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "target/spark-warehouse")

    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_delta_example(spark, tableName):
    # Create a simple dataset
    data = [(1, "A"), (2, "B"), (3, "C")]
    df = spark.createDataFrame(data, ["id", "value"])
    df.show()

    # Write the data as a Delta table
    df.write.format("delta").saveAsTable(tableName)

    # Read the Delta table
    deltaTable = DeltaTable.forName(spark, tableName)

    # Perform some operations
    deltaTable.update(condition = "id = 2", set = {"value": "'B_updated'"})

    deltaTable.history().show()

    deltaTable.delete(condition= "id = 3")

    deltaTable.history().show()

    deltaTable.optimize().executeZOrderBy("id")

    deltaTable.optimize().executeCompaction()

    deltaTable.history().show()

    query_delta_table(spark, tableName, 0).show()
    query_delta_table(spark, tableName, 1).show()
    query_delta_table(spark, tableName, 2).show()



def drop_delta_table(spark, tableName):
    spark.sql(f"DROP TABLE {tableName}")

def query_delta_table(spark, tableName, asOfVersion):
    return spark.read.format("delta").option("versionAsOf", asOfVersion ).table(tableName)

def main():
    spark = create_spark_session()
    table_name = "default.delta_table"
    run_delta_example(spark, table_name)
    drop_delta_table(spark, table_name)
    spark.stop()

if __name__ == "__main__":
    main()
