import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from pyspark.sql.window import Window

# Main Function
def main(input_users, input_badges, dataset):

    # Read Users 
    users_df = spark.read.parquet(input_users)

    # select Only required fields
    filtered_users = users_df.select('id','display_name')

    # Read the Badges Data from Parquet
    badges_df = spark.read.parquet(input_badges)

    # Just the badge and User
    badges_df = badges_df.drop('badge_id', 'received_date', 'badge_class', 'tag_based')

    # Filter the null values
    badges_df = badges_df.filter(badges_df['user_id'].isNotNull())

    # Cache the dataframe
    badges_df.cache()

    # Badge Earned most by User  ###########################################################################

    badges_by_user = badges_df.groupBy('badge_name','user_id').agg(functions.count('badge_name').alias('badge_count'))
    most_earned_by = badges_by_user.groupBy('badge_name').agg(functions.max('badge_count').alias('most_earned_count'))\
        .withColumnRenamed('badge_name','me_badge_name')
    most_earned_by = most_earned_by.join(badges_by_user, [(badges_by_user['badge_name'] == most_earned_by['me_badge_name']) & \
        (badges_by_user['badge_count'] == most_earned_by['most_earned_count'])]).drop('most_earned_count', 'me_badge_name')\
            .withColumnRenamed('user_id','Most_earned_by_user_id')
    #most_earned_by.orderBy(most_earned_by['badge_count'].desc()).show()

    # Count Total Badges for each user #############################################################
    
    count_max = badges_df.groupBy('user_id').agg(functions.count("badge_name").alias('Total_Count'))

    # Count Distinct Badges for each user ##########################################################
    
    count_distinct = badges_df.groupBy('user_id').agg(functions.countDistinct("badge_name").alias('Total_Distinct'))\
            .withColumnRenamed('user_id','distinct_user_id')

    # Most Earned Badges for each user #############################################################
    
    each_badge = badges_df.groupBy('user_id', 'badge_name').agg(functions.count('badge_name').alias('badge_count'))
    most_earned_badge = each_badge.groupBy('user_id').agg(functions.max('badge_count').alias('most_earned_count'))\
        .withColumnRenamed('user_id','me_user_id')
    most_earned_badge = most_earned_badge.join(each_badge, [(each_badge['user_id'] == most_earned_badge['me_user_id']) & \
        (each_badge['badge_count'] == most_earned_badge['most_earned_count'])]).drop('most_earned_count', 'user_id')\
            .withColumnRenamed('badge_name', 'most_earned_badge')
    most_earned_badge = most_earned_badge.withColumn('most_earned_badge', functions.collect_set('most_earned_badge').over(Window.partitionBy("me_user_id")))
    most_earned_badge = most_earned_badge.withColumn('most_earned_count', functions.avg('badge_count').over(Window.partitionBy("me_user_id"))).dropDuplicates()
    #most_earned_badge.orderBy('me_user_id').show()

    # Badges Feature Set for User likely to answer first ###########################################

    # Count the badges required for first likely answer
    first_likely = badges_df\
        .withColumn("Enlightened", functions.when(badges_df['badge_name'] == "Enlightened", 1).otherwise(0))\
            .withColumn("Explainer", functions.when(badges_df['badge_name'] == "Explainer", 1).otherwise(0))\
                .withColumn("Illuminator", functions.when(badges_df['badge_name'] == "Illuminator", 1).otherwise(0))\
                    .withColumn("Refiner", functions.when(badges_df['badge_name'] == "Refiner", 1).otherwise(0))\
                        .groupBy('user_id').sum().drop('sum(user_id)')

    # Rename the columns
    first_likely = first_likely.withColumnRenamed('sum(Enlightened)', 'Enlightened')\
        .withColumnRenamed('sum(Explainer)', 'Explainer')\
            .withColumnRenamed('sum(Illuminator)', 'Illuminator')\
                .withColumnRenamed('sum(Refiner)', 'Refiner')\
                    .withColumnRenamed('user_id', 'first_user_id')
    
    # Bagdes Feature Set for user's answer likely to be accepted ###################################

    # Count the badges required for answer is likely to be accepted
    accepted_likely = badges_df.withColumn("Generalist", functions.when(badges_df['badge_name'] == "Generalist", 1).otherwise(0))\
        .withColumn("Nice Answer",functions.when(badges_df['badge_name'] == "Nice Answer", 1).otherwise(0))\
            .withColumn("Good Answer",functions.when(badges_df['badge_name'] == "Good Answer", 1).otherwise(0))\
                .withColumn("Great Answer",functions.when(badges_df['badge_name'] == "Great Answer", 1).otherwise(0))\
                    .withColumn("Populist", functions.when(badges_df['badge_name'] == "Populist", 1).otherwise(0))\
                        .withColumn("Guru", functions.when(badges_df['badge_name'] == "Guru", 1).otherwise(0))\
                            .withColumn("Lifejacket", functions.when(badges_df['badge_name'] == "Lifejacket", 1).otherwise(0))\
                                .withColumn("Lifeboat", functions.when(badges_df['badge_name'] == "Lifeboat", 1).otherwise(0))\
                                    .groupBy('user_id').sum().drop('sum(user_id)')\
                                        .withColumnRenamed('user_id','accepted_user_id')

    # Rename the columns
    accepted_likely = accepted_likely.withColumnRenamed('sum(Generalist)', 'Generalist')\
        .withColumnRenamed('sum(Nice Answer)', 'Nice_Answer')\
            .withColumnRenamed('sum(Good Answer)', 'Good_Answer')\
                .withColumnRenamed('sum(Great Answer)', 'Great_Answer')\
                    .withColumnRenamed('sum(Populist)', 'Populist')\
                        .withColumnRenamed('sum(Guru)', 'Guru')\
                            .withColumnRenamed('sum(Lifejacket)', 'Lifejacket')\
                                .withColumnRenamed('sum(Lifeboat)', 'Lifeboat')
    
    # Join all Dataframes into one #################################################################

    # Join All the results into one dataframe
    badges_features = count_max.join(filtered_users, [filtered_users['id'] == count_max['user_id']]).drop('id')
    badges_features = badges_features.join(count_distinct, [count_distinct['distinct_user_id'] == badges_features['user_id']]).drop('distinct_user_id')
    badges_features = badges_features.join(first_likely, [first_likely['first_user_id'] == badges_features['user_id']]).drop('first_user_id')
    badges_features = badges_features.join(accepted_likely, [accepted_likely['accepted_user_id'] == badges_features['user_id']]).drop('accepted_user_id')
    badges_features = badges_features.join(most_earned_badge, [most_earned_badge['me_user_id'] == badges_features['user_id']]).drop('me_user_id')
    #badges_features.orderBy('user_id').show()

    # Writing the output ###########################################################################
    
    # # Save to Big Query
    badges_features.write.mode('overwrite').format('bigquery').option('table', dataset+".badges_features").save()
    most_earned_by.write.mode('overwrite').format('bigquery').option('table', dataset+".badge_earned_by").save()

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

    spark = SparkSession.builder.appName('Badges Features Dataset').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.conf.set("temporaryGcsBucket", bucket_name)

    # Main Function Call
    main(input_users, input_badges, output_dataset)