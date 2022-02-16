import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType

def main(input_file, output_file): 

    postlinks_schema = StructType([
        StructField('_Id', IntegerType()),
        StructField('_CreationDate', StringType()),
        StructField('_PostId', IntegerType()),
        StructField('_RelatedPostId', IntegerType()),
        StructField('_LinkTypeId', IntegerType())
    ])

    # Read the Badges Data from XML
    pl_df = spark.read.format("com.databricks.spark.xml").option("rootTag", "postlinks").option("rowTag","row").load(input_file, schema=postlinks_schema)

    # Rename Columns
    post_links = pl_df.withColumnRenamed('_Id','post_links_id')\
        .withColumnRenamed('_CreationDate','creation_date')\
            .withColumnRenamed('_Name','badge_name')\
                .withColumnRenamed('_PostId','post_id')\
                    .withColumnRenamed('_RelatedPostId','related_post_id')\
                        .withColumnRenamed('_LinkTypeId','link_type_id')

    # Write the dataframe to parquet
    post_links.write.mode('overwrite').parquet(output_file)
    print("Post Links Parquet generated.")

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-xml_postlinks", action="store", dest="xml_postlinks", type=str)
    parser.add_argument("-output_path", action="store", dest="output_path", type=str)
    args = parser.parse_args() 

    # IO File Path
    input_path = args.xml_postlinks
    output_path = args.output_path

    spark = SparkSession.builder.appName('Postlinks Preprocessor').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext

    # Main Function Call
    main(input_path, output_path)