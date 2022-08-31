
from assessment import EventLogger

import sys
import yaml

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Import SparkSession
    # from pyspark.sql import SparkSession
    #
    # from pyspark.sql.functions import input_file_name
    # import yaml
    #
    # with open("config.yml", "r") as ymlfile:
    #     cfg = yaml.load(ymlfile)
    # projection_value = "".join(cfg["BaseQuery"]["select"]) if cfg else ""
    #
    # # Create SparkSession
    # spark = SparkSession.builder \
    #     .master("local[*]") \
    #     .appName("SparkByExamples.com") \
    #     .getOrCreate()
    #
    # # During the initial triage of the data, it is discovered that the underlying data has verying schema and may contains duplicate fields names
    # # So in order for the dataframe to accept duplicate fields names, we need to set spark.sql.caseSensitive to true
    # # to allow the case sensitive names to be treated differently
    # spark.conf.set('spark.sql.caseSensitive', True)
    #
    # # As highlighted above, since the schema evolves with each underlying JSON file, it's important to not enforce schema explicitly
    # # to allow it to discover new fields
    # df = spark.read \
    #         .option("multiLine", False).option("mode", "PERMISSIVE")\
    #         .json("data/events")
    #
    # df.printSchema()
    #
    # df = df.withColumn("filename", input_file_name())
    # df.createOrReplaceTempView("event_data")
    #
    #
    #
    #
    # df_output = spark.sql("""
    #                 select * from
    #                     (
    #                         select
    #                             {}
    #                         from event_data
    #                     )x
    #                     where geoFenceCollection is not null
    #                 """.format(projection_value))
    #
    #
    # df_output.show(10, False)

    if len(sys.argv) != 2:
        sys.stdout.write('Wrong number of args!  Expected 1, but got: ' + str(len(sys.argv) - 1) + '.\n')
        sys.exit(1)
    file = sys.argv[1]

    try:
        with open(file, "r") as ymlfile:
            cfg = yaml.load(ymlfile)

        if cfg:
            projection_value = "".join(cfg["BaseQuery"]["select"])
            input_path = cfg["InputPath"]
            save_path = cfg["SavePath"]
            partition_key = cfg["PartitionKey"]
            if len(partition_key) != 2:
                raise Exception("Expecting a mininum of two partition key columns")
                exit()
    except:
        raise Exception("Error opening the configuration file, check the file is rightly placed with correct mapping.")

    obj = EventLogger()
    df = obj.read_json(input_path)
    obj.save_data(df, save_path, projection_value, partition_key)

    df_output = obj.spark.sql("""
                    select dt, type, count(1)  from
                        (
                            select
                                {}                               
                            from event_data
                        )x
                        group by dt,type
                    """.format(projection_value))

    df_output.show()
    #df1.printSchema()
    #event_time between '2022-01-02 15:00:00' and '2022-01-02 16:00:00'   and sonsor_fuelLevel <10 and