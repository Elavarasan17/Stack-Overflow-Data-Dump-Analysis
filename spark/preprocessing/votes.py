# Import Statements
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
import argparse

# Get Vote Schema Definition
def getVoteSchema():
    votes_schema = types.StructType([
        types.StructField('_Id', types.IntegerType()), # Vote Id - Primary Key 
        types.StructField('_PostId', types.IntegerType()), # Post Id
        types.StructField('_VoteTypeId', types.ByteType()), # Vote type 
        types.StructField('_UserId', types.IntegerType()), # User Id
        types.StructField('_CreationDate', types.StringType()), # Creation Date
        types.StructField('_BountyAmount', types.IntegerType()) # Bounty amount
    ])
    return votes_schema

# Get Vote Types Schema Definition
def getVoteTypeSchema():
    votetypes_schema = types.StructType([
        types.StructField('Id', types.IntegerType()), # Vote Type Id 
        types.StructField('Name', types.StringType()), # Name
    ])
    return votetypes_schema

# Preprocessing Votes data
def preprocess_votes(vote_input, vote_type_input, output):
    # Read the raw data file from the input path
    raw_votes_df = spark.read.format("com.databricks.spark.xml").option("rootTag", "votes").option("rowTag", "row")\
        .load(vote_input, schema=getVoteSchema()).repartition(20)
    votetypes_df = spark.read.csv(vote_type_input, schema=getVoteTypeSchema(), header=True)

    # Renaming Dataframe Columns
    raw_votes_df = raw_votes_df.withColumnRenamed('_Id','vote_id').withColumnRenamed('_PostId','post_id')\
        .withColumnRenamed('_VoteTypeId','type_id').withColumnRenamed('_UserId','user_id')\
            .withColumnRenamed('_CreationDate','creation_date').withColumnRenamed('_BountyAmount','bounty_amount')
    votetypes_df = votetypes_df.withColumnRenamed('Id','vote_type_id').withColumnRenamed('Name','name') 

    # Calculating missing data values for column ie. NULL
    #print("Null Values Percentage for each column:")
    # raw_votes_df.select([(count(when(col(c).isNull(), c)) / count(lit(1))).alias(c) for c in raw_votes_df.columns]).show()

    # Drop the columns with too many NULL values
    # print('Dropping Columns with too many null values...')
    
    correct_votes_df = raw_votes_df.drop('user_id', 'bounty_amount').cache()

    # Transforming the raw data
    accepted_post_df = correct_votes_df.filter(correct_votes_df.type_id == 1).select(['vote_id','post_id'])
    
    joincondition = [correct_votes_df.type_id == votetypes_df.vote_type_id]

    votetype_dist_df = correct_votes_df.alias('a').join(votetypes_df.alias('b'), joincondition, 'left')\
        .groupBy(votetypes_df.vote_type_id, votetypes_df.name).agg(count(correct_votes_df.vote_id).alias('num_of_votes'))\
            .orderBy(votetypes_df.vote_type_id).select(['vote_type_id','name', 'num_of_votes'])

    updownvotes_df = correct_votes_df.where(correct_votes_df.type_id.isin([int('2'),int('3')])).groupBy(correct_votes_df.post_id)\
        .agg(sum(when(correct_votes_df.type_id == 2, int('1')).otherwise(int('0'))).alias('upvotes'),\
             sum(when(correct_votes_df.type_id == 3, int('1')).otherwise(int('0'))).alias('downvotes'))\
                 .orderBy(col('upvotes').desc())

    # Writing the Transformed data
    accepted_post_df.write.mode('overwrite').parquet(output + "acceptedposts")
    updownvotes_df.write.mode('overwrite').parquet(output + "postupdownvotes")
    votetype_dist_df.write.mode('overwrite').parquet(output + "votetypedistribution")                

# Main Method
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-votes_src", action="store", dest="votes_src", type=str)
    parser.add_argument("-vote_types_src", action="store", dest="vote_types_src", type=str)
    parser.add_argument("-votes_dest", action="store", dest="votes_dest", type=str)
    
    args = parser.parse_args() 
    vote_input = args.votes_src
    vote_type_input = args.vote_types_src
    output = args.votes_dest

    spark = SparkSession.builder.appName('Votes Preprocessor').getOrCreate()
    # assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    preprocess_votes(vote_input, vote_type_input, output)