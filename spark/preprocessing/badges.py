# Import statements
import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType

# main method
def main(input_file, output_file): 

    # Schema Definition
    badges_schema = StructType([
        StructField('_Id', IntegerType()),
        StructField('_UserId', IntegerType()),
        StructField('_Name', StringType()),
        StructField('_Date', StringType()),
        StructField('_Class', IntegerType()),
        StructField('_TagBased', BooleanType())
    ])

    # Read the Badges Data from XML
    badges_df = spark.read.format("com.databricks.spark.xml").option("rootTag", "badges")\
                    .option("rowTag","row").load(input_file, schema=badges_schema)

    # Rename Columns
    badges_df = badges_df.withColumnRenamed('_Id','badge_id')\
        .withColumnRenamed('_UserId','user_id')\
            .withColumnRenamed('_Name','badge_name')\
                .withColumnRenamed('_Date','received_date')\
                    .withColumnRenamed('_Class','badge_class')\
                        .withColumnRenamed('_TagBased','tag_based')

    # Write the dataframe to parquet
    badges_df.write.mode('overwrite').parquet(output_file)
    print("Badges Parquet generated.")

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-xml_badges", action="store", dest="xml_badges", type=str)
    parser.add_argument("-output_path", action="store", dest="output_path", type=str)
    args = parser.parse_args() 

    # IO File Path
    input_path = args.xml_badges
    output_path = args.output_path

    spark = SparkSession.builder.appName('Badges Preprocessor').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    # Main Function Call
    main(input_path, output_path)