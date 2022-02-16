from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import date_format
from pyspark.sql.functions import year, month, dayofmonth
import sys
import json
import argparse
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

# add more functions as necessary

    
def main(posts_inputs, users_inputs,temp_bucket_input,dataset_input):
    # main logic starts here

    #Hariish - Users and Tags
    users = spark.read.parquet(users_inputs)
    posts = spark.read.parquet(posts_inputs)
    
    #User and posts join
    posts = posts.withColumn("creation_year",year(posts['creation_date']))
    res = users.join(posts,(posts['owner_user_id'] == users['id'])).select(users.display_name,users.id,posts.post_id,posts.creation_year,users.location,posts.owner_user_id,posts.post_type_id,posts.accepted_answer_id,posts.tags)
    
    #Active users over years:
    res1 = res.groupBy(res['id'],res['display_name'],res['creation_year']).agg(functions.count(res['post_id']).alias('post_count')).orderBy('post_count')
    res2 = res1.groupBy(res1['id'],res['display_name']).pivot('creation_year').sum('post_count')
    res3 = res2.na.fill(value=0)
    res4 = res3.withColumn("overall_posts",res3['2015']+res3['2016']+res3['2017']+res3['2018']+res3['2019']+res3['2020']+res3['2021'])
    active_users = res4.orderBy(res4['overall_posts'].desc()).select('id','display_name','2015','2016','2017','2018','2019','2020','2021','overall_posts')
    act_id = active_users.limit(10).select('id','display_name')
    
    active_users = active_users.withColumnRenamed('2015','y_2015').withColumnRenamed('2016','y_2016').withColumnRenamed('2017','y_2017').withColumnRenamed('2018','y_2018').withColumnRenamed('2019','y_2019').withColumnRenamed('2020','y_2020').withColumnRenamed('2021','y_2021')
    active_users = active_users.limit(10)

    a1 = active_users.selectExpr("display_name", "stack(8, 'y_2015', y_2015, 'y_2016', y_2016, 'y_2017', y_2017,'y_2018',y_2018,'y_2019',y_2019,'y_2020',y_2020,'y_2021',y_2021 ,'overall_posts',overall_posts) as (creation_year, values)").where("values is not null")
    a1 = a1.select('creation_year','display_name','values')
    act_user = a1.groupBy(a1['creation_year']).pivot('display_name').sum('values')
    act_user = act_user.withColumnRenamed('Gordon Linoff',"Gordon_Linoff").withColumnRenamed('Nina Scholz','Nina_Scholz').withColumnRenamed('Ronak Shah','Ronak_Shah').withColumnRenamed('T.J. Crowder','TJ_Crowder').withColumnRenamed('Tim Biegeleisen','Tim_Biegeleisen').withColumnRenamed('Wiktor Stribiżew','Wiktor_Stribiżew')
    #act_user.show()

    #Famous Tags over the year
    p1 = posts.withColumn("new",functions.arrays_zip("tags")).withColumn("new", functions.explode("new")).select('post_id',functions.col("new.tags").alias("tags"),'creation_year')
    p2 = p1.groupBy(p1['tags'],p1['creation_year']).agg(functions.count(p1['tags']).alias('tag_count'))
    p3 = p2.groupBy(p2['tags']).pivot('creation_year').sum('tag_count')
    p3 = p3.na.fill(value=0)
    p4 = p3.withColumn("overall_tag_usage",p3['2015']+p3['2016']+p3['2017']+p3['2018']+p3['2019']+p3['2020']+p3['2021'])
    tag_trends = p4.orderBy(p4['overall_tag_usage'].desc()).select('tags','2015','2016','2017','2018','2019','2020','2021','overall_tag_usage')
    tag_trends = tag_trends.withColumnRenamed('2015','y_2015').withColumnRenamed('2016','y_2016').withColumnRenamed('2017','y_2017').withColumnRenamed('2018','y_2018').withColumnRenamed('2019','y_2019').withColumnRenamed('2020','y_2020').withColumnRenamed('2021','y_2021')
    tag_trends = tag_trends.limit(10)
    t1 = tag_trends.selectExpr("tags", "stack(8, 'y_2015', y_2015, 'y_2016', y_2016, 'y_2017', y_2017,'y_2018',y_2018,'y_2019',y_2019,'y_2020',y_2020,'y_2021',y_2021 ,'overall_tag_usage',overall_tag_usage) as (creation_year, values)").where("values is not null")
    t1 = t1.select('creation_year','tags','values')
    tag_view = t1.groupBy(t1['creation_year']).pivot('tags').sum('values')
    #tag_view.show()

    #writing:
    act_user.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_input+".active_users").save()
    tag_view.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_input+".tag_trends").save()
    act_id.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_input+".top10_user_details").save()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-posts_src", action="store", dest="posts_src", type=str)
    parser.add_argument("-users_src", action="store", dest="users_src", type=str)
    parser.add_argument("-tempbucket_src", action="store", dest="tempbucket_src", type=str)
    parser.add_argument("-dataset_src", action="store", dest="dataset_src", type=str)
    args = parser.parse_args() 

    posts_inputs = args.posts_src
    users_inputs = args.users_src
    temp_bucket_input = args.tempbucket_src
    dataset_input = args.dataset_src

    spark = SparkSession.builder.appName('Explorer DF').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
        
    main(posts_inputs, users_inputs,temp_bucket_input,dataset_input)