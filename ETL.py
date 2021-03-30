import findspark
findspark.init()

from pyspark.sql import SparkSession


def connect_to_sql(spark, jdbc_hostname, jdbc_port, database, data_table, username, password):
    jdbc_url = "jdbc:sqlserver://{0}:{1};database={2}".format(jdbc_hostname, jdbc_port, database)

    connection_details = {
        "user": username,
        "password": password,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }

    df = spark.read.jdbc(url=jdbc_url, table=data_table, properties=connection_details)
    return df


def main():
    spark = SparkSession.builder.appName("ETL") \
        .config("spark.driver.extraClassPath", "spark-mssql-connector_2.12_3.0-1.0.0-alpha") \
        .getOrCreate()

    df = spark.read.format("Text").option("header", "True").load("\\path\\to\\file")
    df2 = df.select(
        df.value.substr(1, 11).alias('colum1'),
        df.value.substr(12, 4).alias('column2'),
        df.value.substr(16, 9).alias('column3'),
        df.value.substr(25, 15).alias('column4'),
    )

    df2.show()

    df2.write \
        .format("jdbc") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("url", "jdbc:sqlserver://localhost:1433;databaseName=Test") \
        .option("dbtable", "dbo.tblTest") \
        .option("user", "user") \
        .option("password", "password") \
        .mode(saveMode="Append") \
        .save()

    input()
