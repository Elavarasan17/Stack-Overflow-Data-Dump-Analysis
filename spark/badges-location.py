import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

# Main Function
def main(input_users, input_badges, dataset):
    
    # Read the Data from Parquet
    users_df = spark.read.parquet(input_users)
    badges_df = spark.read.parquet(input_badges)

    # Drop the columns not needed
    badges_df = badges_df.drop('badge_id', 'received_date')
    users_df = users_df.drop('creattion_date', 'last_access_date', 'up_votes', 'down_votes', 'views', 'display_name')

    # Filter the Null/Blank Locations
    filtered_users = users_df.filter((users_df['location'].isNotNull()) & (functions.trim(users_df['location']) != ""))
    
    # Users with badges
    users_badges = badges_df.join(filtered_users, [filtered_users['id'] == badges_df['user_id']]).drop('id').withColumn('badge_class_name', functions.when(badges_df['badge_class'] == 1, "Gold")\
        .when(badges_df['badge_class'] == 2, "Silver").when(badges_df['badge_class'] == 3, "Bronze")).drop('badge_class')     

    # Get Total for each class for each location
    tag_based_badges = users_badges.filter(badges_df['tag_based'] == True).withColumnRenamed('badge_class_name', 'badge_classname')

    total_count = tag_based_badges.groupBy('location').agg(functions.count('badge_name').alias('total_badges'))\
        .withColumn('badge_classname', functions.lit("Total"))
    total_count = total_count.select('location','badge_classname', 'total_badges')
    #total_count.show()

    location_by_class = tag_based_badges.groupBy('location','badge_classname').agg(functions.count('badge_name').alias('total_badges'))
    location_by_class = location_by_class.union(total_count)
    #location_by_class.orderBy(location_by_class['location'].desc()).show(100)

    total_badge_count = users_badges.groupBy('badge_name','location').agg(functions.count('badge_name').alias('badge_count'))\
        .withColumn('badge_class_name', functions.lit("Total"))
    total_badge_count = total_badge_count.select('badge_name','badge_class_name','location','badge_count')
    #total_badge_count.show()

    # Top badge based on class for each location
    class_badge_location = users_badges.groupBy('badge_name','badge_class_name','location').agg(functions.count('user_id').alias('badge_count'))
    class_badge_location = class_badge_location.groupBy('badge_name','badge_class_name','location').agg(functions.max('badge_count').alias('badge_count'))
    class_badge_location = class_badge_location.union(total_badge_count).withColumnRenamed('location', 'badge_location')
    #class_badge_location.orderBy(class_badge_location['badge_location'].asc()).show()

    # Write the result to Big Query
    location_by_class.write.mode('overwrite').format('bigquery').option('table', dataset+".location_for_class").save()
    class_badge_location.write.mode('overwrite').format('bigquery').option('table', dataset+".location_for_badge_by_class").save()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-input_users", action="store", dest="input_users", type=str)
    parser.add_argument("-input_badges", action="store", dest="input_badges", type=str)
    parser.add_argument("-output_dataset", action="store", dest="output_dataset", type=str)
    parser.add_argument("-bucket_name", action="store", dest="bucket_name", type=str)
    args = parser.parse_args() 

    # IO File Path
    input_users = args.input_users
    input_badges = args.input_badges
    output_dataset = args.output_dataset
    bucket_name = args.bucket_name

    spark = SparkSession.builder.appName('Badges Location').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.conf.set("temporaryGcsBucket", bucket_name)

    # Main Function Call
    main(input_users, input_badges, output_dataset)
