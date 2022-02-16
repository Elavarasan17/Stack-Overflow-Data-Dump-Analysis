import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

# Main Function
def main(input_users, input_posts, input_postlinks, output_dataset):
    
    # Read the Input Data from Parquet
    users = spark.read.parquet(input_users)
    posts = spark.read.parquet(input_posts)
    postlinks = spark.read.parquet(input_postlinks)

    # Get the duplicates using postlinks and posts ||| Post link type id === 3 for posts which are Duplicates
    postlinks_subset = postlinks.drop('creation_date', 'related_post_id')
    duplicate_postlinks = postlinks_subset.filter(postlinks['link_type_id'] == 3).withColumnRenamed('post_id','dup_post_id')
    duplicate_posts = duplicate_postlinks.join(posts, [posts['post_id'] == duplicate_postlinks['dup_post_id']]).drop('dup_post_id')
    #duplicate_posts.cache()
    
    # Most duplicates by Users
    duplicate_posts_users = duplicate_posts.filter(duplicate_posts['owner_user_id'].isNotNull())
    duplicates = duplicate_posts_users.groupBy('owner_user_id').agg(functions.count('post_id').alias('duplicates'))    
    users_duplicating = users.join(duplicates, users['id'] == duplicates['owner_user_id']).select('id','display_name','duplicates')
    #users_duplicating.orderBy(users_duplicating['duplicates'].desc()).show()

    # Most Duplicates for each tag
    posts_tags = duplicate_posts.withColumn('tags', functions.explode(duplicate_posts['tags']))
    duplicate_tags = posts_tags.groupBy('tags').agg(functions.count('post_id').alias('duplicates'))
    #duplicate_tags.orderBy(duplicate_tags['duplicates'].desc()).show()
    #duplicate_tags.filter(duplicate_tags['duplicates'] > 0).orderBy(duplicate_tags['duplicates'].asc()).show()

    # Write the dataframes to bigQuery
    users_duplicating.write.mode('overwrite').format('bigquery').option('table', output_dataset+".duplicate_posts").save()
    duplicate_tags.write.mode('overwrite').format('bigquery').option('table', output_dataset+".duplicate_tags").save()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-input_users", action="store", dest="input_users", type=str)
    parser.add_argument("-input_posts", action="store", dest="input_posts", type=str)
    parser.add_argument("-input_postlinks", action="store", dest="input_postlinks", type=str)
    parser.add_argument("-output_dataset", action="store", dest="output_dataset", type=str)
    parser.add_argument("-bucket_name", action="store", dest="bucket_name", type=str)
    args = parser.parse_args() 

    # IO File Path
    input_users = args.input_users
    input_posts = args.input_posts
    input_postlinks = args.input_postlinks
    output_dataset = args.output_dataset
    bucket_name = args.bucket_name

    spark = SparkSession.builder.appName('Duplicates').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.conf.set("temporaryGcsBucket", bucket_name)

    # Main Function Call
    main(input_users, input_posts,input_postlinks, output_dataset)