# Import Statements
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import *
import sys
import argparse

# Main method
def main(inputs, output):

    # Schema Definition
    users_schema = types.StructType([types.StructField('_Id', types.IntegerType()),
        types.StructField('_Reputation', types.IntegerType()),
        types.StructField('_CreationDate', types.StringType()),
        types.StructField('_DisplayName', types.StringType()),
        types.StructField('_LastAccessDate', types.StringType()),
        types.StructField('_AboutMe', types.StringType()),
        types.StructField('_Views', types.IntegerType()),
        types.StructField('_UpVotes', types.IntegerType()),
        types.StructField('_DownVotes', types.IntegerType()),
        types.StructField('_WebsiteUrl', types.StringType()),
        types.StructField('_Location', types.StringType()),
        types.StructField('_ProfileImageUrl', types.StringType()),
        types.StructField('_EmailHash', types.StringType()),
        types.StructField('_AccountId', types.IntegerType())])

    df = spark.read.format("com.databricks.spark.xml").option("rootTag", "users").option("rowTag", "row")\
        .schema(users_schema).load(inputs).drop("_AboutMe","_WebsiteUrl","_ProfileImageUrl","_EmailHash","_AccountId")

    df = df.withColumnRenamed('_Id','id').withColumnRenamed('_Reputation','reputation')\
        .withColumnRenamed('_CreationDate','creation_date').withColumnRenamed('_DisplayName','display_name')\
            .withColumnRenamed('_LastAccessDate','last_access_date').withColumnRenamed('_Views','views')\
                .withColumnRenamed('_Upvotes','upvotes').withColumnRenamed('_Downvotes','downvotes')\
                    .withColumnRenamed('_Location','location')
    
    df1 = df.withColumn('creation_year',functions.split(df['creation_date'],"T").getItem(0))
    df1 = df1.drop('creation_date')
    df1 = df1.withColumnRenamed('creation_year','creation_date')

    df2 = df1.withColumn('last_access_year',functions.split(df['last_access_date'],"T").getItem(0))
    df2 = df2.drop('last_access_date')
    df2 = df2.withColumnRenamed('last_access_year','last_access_date')

    users = df2.repartition(120)
    users.write.mode('overwrite').parquet(output)

if __name__ == '__main__':
    # Parsing Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-users_src", action="store", dest="users_src", type=str)
    parser.add_argument("-users_output", action="store", dest="users_output", type=str)
    args = parser.parse_args() 

    inputs = args.users_src
    output = args.users_output
    
    spark = SparkSession.builder.appName('User Preprocessor').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    
    main(inputs, output)