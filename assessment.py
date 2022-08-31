"""
Author: Chandan Parihar
Date: 28-August-2022
Description: Unit test cases
Date                Name                 Description
28-08-2022           CP                  Demo project
"""

from logger import lLogger
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name


class EventLogger:

    def __init__(self):
        self.spark= SparkSession.builder \
        .master("local") \
        .appName("Code-Assessment-280822") \
        .getOrCreate()
        self.table_name = "event_data"




    def read_json(self, input_path):
        lLogger.info("Reading JSON files from {}".format(input_path))
        # During the initial triage of the data, it is discovered that the underlying data has verying schema and may contains duplicate fields names
        # So in order for the dataframe to accept duplicate fields names, we need to set spark.sql.caseSensitive to true
        # to allow the case sensitive names to be treated differently
        self.spark.conf.set('spark.sql.caseSensitive', True)

        # As highlighted above, since the schema evolves with each underlying JSON file, it's important to not enforce schema explicitly
        # to allow it to discover new fields
        df = self.spark.read \
            .option("multiLine", False).option("mode", "PERMISSIVE") \
            .json(input_path)
        df = df.withColumn("filename", input_file_name())
        lLogger.info("Schema discovered from the input file : ")
        lLogger.info(df._jdf.schema().treeString())
        return df

    def save_data(self, df, save_path, projection_value, partition_key):
        # here the data is partitioned on date with hour and type
        lLogger.info("Writing output data to {}".format(save_path))
        df.createOrReplaceTempView(self.table_name)
        df_new = self.spark.sql("""
                                select
                                    {0}, date_format(cast(replace(timestamp,'T',' ') as timestamp),'yyyy-MM-dd hh:00:00') dt
                                from {1}
                        """.format(projection_value, self.table_name))

        df_new.write.partitionBy(partition_key[0], partition_key[1]).parquet("{}".format(save_path))
