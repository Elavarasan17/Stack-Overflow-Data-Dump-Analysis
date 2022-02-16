import argparse
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

# Main Function
def main(input_badges, dataset):
    
    # Read the Data from Parquet
    badges_df = spark.read.parquet(input_badges)

    # Drop the columns not needed and compute new ones needed
    badges_yearly_data = badges_df.drop('badge_class', 'user_id')
    badges_yearly_data = badges_yearly_data.withColumn('year', functions.year(badges_yearly_data['received_date']))
    badges_yearly_data = badges_yearly_data.groupBy('badge_name','year','tag_based').agg(functions.count('badge_id').alias('badge_count'))
    badges_yearly_data.cache()

    # Top 10 Tag Based Badges overall ##############################################################

    tag_badge_names = badges_df.filter(badges_df['tag_based'] == True).groupBy('badge_name').agg(functions.count('badge_id').alias('badge_count'))
    tag_badge_names = tag_badge_names.orderBy(tag_badge_names['badge_count'].desc()).limit(10).select('badge_name')
    tag_badge_names = tag_badge_names.rdd.flatMap(list).collect()

    tag_badges_trends = badges_yearly_data.filter(badges_yearly_data['badge_name'].isin(tag_badge_names))
    tag_badges_trends = tag_badges_trends.groupBy(tag_badges_trends['year']).pivot('badge_name').sum('badge_count')
    tag_badges_trends = tag_badges_trends.na.fill(value=0)
    tag_badges_trends = tag_badges_trends.filter(tag_badges_trends['year'] > 2014) 
    #tag_badges_trends.show()

    # Top 10 Non Tag Based Badges overall ##########################################################

    Ntag_badge_names = badges_df.filter(badges_df['tag_based'] == False).groupBy('badge_name').agg(functions.count('badge_id').alias('badge_count'))
    Ntag_badge_names = Ntag_badge_names.orderBy(Ntag_badge_names['badge_count'].desc()).limit(10).select('badge_name')
    Ntag_badge_names = Ntag_badge_names.rdd.flatMap(list).collect()

    Ntag_badges_trends = badges_yearly_data.filter(badges_yearly_data['badge_name'].isin(Ntag_badge_names))
    Ntag_badges_trends = Ntag_badges_trends.withColumn('badge_name',  functions.regexp_replace('badge_name', ' ', '-'))
    Ntag_badges_trends = Ntag_badges_trends.groupBy(Ntag_badges_trends['year']).pivot('badge_name').sum('badge_count')
    Ntag_badges_trends = Ntag_badges_trends.na.fill(value=0)
    Ntag_badges_trends = Ntag_badges_trends.filter(Ntag_badges_trends['year'] > 2014)
    
    #Ntag_badges_trends.show()

    # Writing the output ###########################################################################

    # Write the result to Big Query
    tag_badges_trends.write.mode('overwrite').format('bigquery').option('table', dataset+".trends_badges_tags").save()
    Ntag_badges_trends.write.mode('overwrite').format('bigquery').option('table', dataset+".trends_badges_non_tag").save()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-input_badges", action="store", dest="input_badges", type=str)
    parser.add_argument("-output_dataset", action="store", dest="output_dataset", type=str)
    parser.add_argument("-bucket_name", action="store", dest="bucket_name", type=str)
    args = parser.parse_args() 

    # IO File Path
    input_badges = args.input_badges
    output_dataset = args.output_dataset
    bucket_name = args.bucket_name

    spark = SparkSession.builder.appName('Badges Trends').getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    spark.conf.set("temporaryGcsBucket", bucket_name)

    # Main Function Call
    main(input_badges, output_dataset)
