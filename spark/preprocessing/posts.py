# Import Statements
from pyspark.sql import SparkSession, types, functions
from pyspark.sql.column import Column, _to_java_column
import argparse

# Data Pre-processing
def process_posts(inputs, outputs):
    posts_schema = get_schema('posts')

    # Removing the first and last line to remove nested xml structure to enable reading xml line by line parallely
    posts_text = sc.textFile(inputs).cache()

    xml_header = posts_text.first()
    posts_without_header = posts_text.filter(lambda text: text != xml_header).cache()

    root_tag = posts_without_header.first()
    posts_xml = posts_without_header.filter(lambda text: text != root_tag).cache()
    total = posts_xml.count()

    posts_without_footer = posts_xml.zipWithIndex() \
        .filter(lambda line:  line[1] < total - 1) \
        .map(lambda line: line[0].strip())

    df = spark.createDataFrame(posts_without_footer, types.StringType()).repartition(60)
    xml_parsed = df.withColumn("parsed_xml", ext_from_xml(df['value'], posts_schema))

    xml = xml_parsed['parsed_xml']
    posts_data = xml_parsed.select(xml['_Id'].alias('post_id'), xml['_AcceptedAnswerId'].alias('accepted_answer_id'),\
         xml['_AnswerCount'].alias('answer_count'), xml['_CreationDate'].alias('creation_date'),\
              xml['_FavoriteCount'].alias('favorite_count'), xml['_OwnerUserId'].alias('owner_user_id'),\
                 xml['_ParentId'].alias('parent_id'), xml['_PostTypeId'].alias('post_type_id'),\
                      xml['_Title'].alias('title'), xml['_ViewCount'].alias('view_count'), xml['_Tags'].alias('_Tags'))

    posts = posts_data.withColumn('tags', functions.expr("regexp_extract_all(_Tags, '\\<(\\\\w+)\\>', 1)")).drop('_Tags')
    posts_df = posts.filter(functions.year(posts['creation_date']) >= 2015)
    posts_df.write.parquet(outputs, mode='overwrite', compression='snappy')

# Main method
def main(args):

    posts_src = args.posts_src
    posts_dest = args.posts_dest
    process_posts(posts_src, posts_dest)

'''
    Converting xml string to dataframe struct column using databricks helper methods
'''
def ext_from_xml(xml_column, schema, options={}):
    java_column = _to_java_column(xml_column.cast('string'))
    java_schema = spark._jsparkSession.parseDataType(schema.json())
    scala_map = spark._jvm.org.apache.spark.api.python.PythonUtils.toScalaMap(options)
    jc = spark._jvm.com.databricks.spark.xml.functions.from_xml(java_column, java_schema, scala_map)
    return Column(jc)

# Get Schema Definition
def get_schema(dataset):
    posts_schema = types.StructType([
        types.StructField('_Id', types.StringType()),
        types.StructField('_AcceptedAnswerId', types.StringType()),
        types.StructField('_AnswerCount', types.StringType()),
        types.StructField('_Body', types.StringType()),
        types.StructField('_ClosedDate', types.TimestampType()),
        types.StructField('_CommentCount', types.DoubleType()),
        types.StructField('_CommunityOwnedDate', types.TimestampType()),
        types.StructField('_ContentLicense', types.StringType()),
        types.StructField('_CreationDate', types.TimestampType()),
        types.StructField('_FavoriteCount', types.StringType()),
        types.StructField('_LastActivityDate', types.TimestampType()),
        types.StructField('_LastEditDate', types.TimestampType()),
        types.StructField('_LastEditorDisplayName', types.StringType()),
        types.StructField('_LastEditorUserId', types.StringType()),
        types.StructField('_OwnerDisplayName', types.StringType()),
        types.StructField('_OwnerUserId', types.StringType()),
        types.StructField('_ParentId', types.StringType()),
        types.StructField('_PostTypeId', types.StringType()),
        types.StructField('_Score', types.StringType()),
        types.StructField('_Tags', types.StringType()),
        types.StructField('_Title', types.StringType()),
        types.StructField('_ViewCount', types.StringType()),
    ])
    schema = {
        'posts': posts_schema
    }
    return schema[dataset]

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-posts_src", action="store", dest="posts_src", type=str)
    parser.add_argument("-posts_dest", action="store", dest="posts_dest", type=str)
    
    args = parser.parse_args() 
    spark = SparkSession.builder.appName('Posts Preprocessor').getOrCreate()

    # Add path to json key file here
    # path_to_json_key_file = … 
    # spark.conf.set("google.cloud.auth.service.account.json.keyfile", path_to_json_key_file)
    # spark.conf.set("google.cloud.auth.service.account.enable", "true")

    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    main(args)