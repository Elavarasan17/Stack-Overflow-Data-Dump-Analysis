# Import Statements
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import *
import argparse

# Get Schema Definition
def getCommentsSchema():
    comments_schema = types.StructType([
        types.StructField('_Id', types.IntegerType()), #Comment Id
        types.StructField('_PostId', types.IntegerType()), #Post Id
        types.StructField('_Score', types.IntegerType()), #Comment Score
        types.StructField('_Text', types.StringType()), #Comment Text
        types.StructField('_CreationDate', types.StringType()), #CreatedDate
        types.StructField('_UserDisplayName', types.StringType()), #User Name
        types.StructField('_UserId', types.IntegerType()), #User Id
        types.StructField('_ContentLincense', types.StringType()) #Content License
    ])
    return comments_schema

# Preprocessing Comments
def preprocess_comments(inputs, output):

    # Read raw data from the input path
    rawcomments_df = spark.read.format("com.databricks.spark.xml").option("rootTag", "coments")\
        .option("rowTag", "row").load(inputs, schema = getCommentsSchema()).repartition(20)
    
    #Dropping unnecessary columns
    rawcomments_df = rawcomments_df.drop('_Text', '_UserDisplayName', '_ContentLincense')

    #Cleaning data
    rawcomments_df = rawcomments_df.withColumnRenamed('_Id','comment_id').withColumnRenamed('_PostId','post_id')\
        .withColumnRenamed('_Score','score').withColumnRenamed('_CreationDate','creation_date')\
            .withColumnRenamed('_UserId','user_id')
    filtered_comments = rawcomments_df.filter(rawcomments_df.user_id.isNotNull()).cache()

    #Transforming Data
    high_score_comments = filtered_comments.orderBy(filtered_comments.score.desc())\
        .select(['comment_id','post_id','score','creation_date','user_id'])

    most_comments_by_user = filtered_comments.groupBy(filtered_comments.user_id).agg(count(filtered_comments.comment_id)\
        .alias('num_of_comments')).orderBy(col('num_of_comments').desc()).select(['user_id', 'num_of_comments'])

    post_withmost_comments = filtered_comments.groupBy(filtered_comments.post_id).agg(count(filtered_comments.user_id)\
        .alias('count_of_users')).orderBy(col('count_of_users').desc()).select(['post_id', 'count_of_users'])

    # Writing the output
    high_score_comments.write.mode('overwrite').parquet(output+ "highscorecomments")
    most_comments_by_user.write.mode('overwrite').parquet(output+ "mostcommentuser")
    post_withmost_comments.write.mode('overwrite').parquet(output+ "postwithmostcomments")

# Main Function
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-comments_src", action="store", dest="comments_src", type=str)
    parser.add_argument("-comments_dest", action="store", dest="comments_dest", type=str)
    
    args = parser.parse_args() 
    inputs = args.comments_src
    output = args.comments_dest

    spark = SparkSession.builder.appName('Comments Preprocessor').getOrCreate()
    #assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    preprocess_comments(inputs, output)