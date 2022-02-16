import argparse

from pyspark.sql import SparkSession, functions
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
from pyspark.sql.functions import array_contains
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName('posts - xml to parquet').getOrCreate()
assert spark.version >= '3.0' # make sure we have Spark 3.0+
sc = spark.sparkContext
sc.setLogLevel('WARN')

def filter_posts(posts):
    filter_posts = array_contains(posts['tags'], 'javascript') | array_contains(posts['tags'], 'python') | array_contains(posts['tags'], 'java') 
    return filter_posts

def main(posts_inputs, users_inputs, answer_data_outputs, users_data_outputs, answer_model_file, user_model_file):

    posts = spark.read.parquet(posts_inputs)
    users = spark.read.parquet(users_inputs)

    questions = posts.filter(posts['post_type_id'] == 1).select(posts['post_id'].alias('question_id'), posts['tags'], posts['accepted_answer_id'], posts['title'], posts['owner_user_id'].alias('asker'), posts['creation_date'].alias('asked_date')) \
        .cache()
    answers = posts.filter(posts['post_type_id'] == 2).select(posts['post_id'].alias('answer_id'), posts['parent_id'].alias('parent_question'), posts['owner_user_id'].alias('answerer'), posts['creation_date'].alias('answered_date')) \
        .cache()
    answers_with_questions = answers.join(questions, [answers['parent_question'] == questions['question_id']])

    answers_with_questions = answers_with_questions.filter(filter_posts(answers_with_questions))

    answered = users.join(answers_with_questions, [answers_with_questions['answerer'] == users['id']]).select(users['id'], users['reputation'], users['display_name'], users['location'], users['last_access_date'],
                answers_with_questions['question_id'],
                answers_with_questions['answer_id'],
                answers_with_questions['title'],
                answers_with_questions['tags'],
                answers_with_questions['asked_date'], answers_with_questions['answered_date'],
                functions.lit(1).alias('answered'),
                (answers_with_questions['answer_id'].eqNullSafe(answers_with_questions['accepted_answer_id'])).alias('accepted').cast('integer')).cache()
    user_ids = answered.select(answered['id'].alias('user_id')).distinct().sample(0.002)
    users_id_list = [row['user_id'] for row in user_ids.collect()]
    users_selected = users.join(user_ids, [user_ids['user_id'] == users['id']])
    users_selected = users_selected.drop('user_id')

    answered_filtered = answered.filter(answered['id'].isin(users_id_list))

    answered_col = answers_with_questions['answerer'].eqNullSafe(users_selected['id']).alias('answered')
    accepted_col = answers_with_questions['answerer'].eqNullSafe(users_selected['id']) & answers_with_questions['accepted_answer_id'].eqNullSafe(answers_with_questions['answer_id'])
    notanswered = users_selected.crossJoin(answers_with_questions.filter(answers_with_questions['answerer'].isin(users_id_list))).select(users_selected['id'], users_selected['reputation'], users_selected['display_name'], users_selected['location'], users_selected['last_access_date'],
                answers_with_questions['question_id'],
                answers_with_questions['answer_id'],
                answers_with_questions['title'],
                answers_with_questions['tags'],
                answers_with_questions['asked_date'], answers_with_questions['answered_date'],
                answered_col.cast('integer'),
                accepted_col.alias('accepted').cast('integer')).cache()

    # Severe undersampling of the not answered labels causes the model to learn that predicting that the user won't answer helps
    # achieve a very large accuracy. Hence a small sample is chosen but the size of the not answered class is kept high enough to
    # simulate real life scenario where most users might not answer for a given post

    notanswered = notanswered.sample(0.003)

    data = notanswered.union(answered_filtered)

    # Creating a bag of words feature set with title and tags as they are textual features and contribute to knowledge about the user
    # and the posts they interact with.

    hashingTFTags = HashingTF(inputCol="tags", outputCol="tagFeatures", numFeatures = 200)
    tokenizer = Tokenizer(inputCol="title", outputCol="tokens")
    hashingTFTitle = HashingTF(inputCol="tokens", outputCol="titleFeatures", numFeatures = 50)

    data.write.parquet(answer_data_outputs, mode='overwrite')

    # ********************************** Answer predictor ********************************** #

    print('# ********************************** Answer predictor ********************************** #')

    train, validation = data.randomSplit([0.80, 0.20])
    train = train.cache()
    validation = validation.cache()


    assembler = VectorAssembler(
        inputCols=['id', "reputation", "tagFeatures", 'titleFeatures'],
        outputCol="features", handleInvalid="skip")
    classifier = DecisionTreeClassifier(maxDepth=5, labelCol="answered")
    pipeline = Pipeline(stages=[hashingTFTags, tokenizer, hashingTFTitle, assembler, classifier])

    print('###############################')
    print('Training model')
    model = pipeline.fit(train)
    print('Done training')
    print('###############################')

    predictions = model.transform(validation)
    predictions = predictions.select('prediction', 'answered')
    evaluator = MulticlassClassificationEvaluator(labelCol = "answered", predictionCol="prediction")
    score = evaluator.evaluate(predictions)

    print(score)

    model.write().overwrite().save(answer_model_file)


    # ********************************** User predictor ********************************** #

    print('# ********************************** User predictor ********************************** #')

    answers_with_questions = answers.join(questions, [answers['parent_question'] == questions['question_id']])
    answered = answered_filtered

    hashingTFTags = HashingTF(inputCol="tags", outputCol="tagFeatures", numFeatures = 200)
    tokenizer = Tokenizer(inputCol="title", outputCol="tokens")
    hashingTFTitle = HashingTF(inputCol="tokens", outputCol="titleFeatures", numFeatures = 120)

    train, validation = answered.randomSplit([0.80, 0.20])
    train = train.cache()
    validation = validation.cache()

    stringIndexer = StringIndexer(inputCol="id", outputCol="user_id",
        stringOrderType="frequencyDesc", handleInvalid="skip")

    assembler = VectorAssembler(
        inputCols=["tagFeatures", 'titleFeatures'],
        outputCol="features", handleInvalid="skip")

    classifier = RandomForestClassifier(maxDepth=15, labelCol="user_id", numTrees=15)
    pipeline = Pipeline(stages=[stringIndexer, hashingTFTags, tokenizer, hashingTFTitle, assembler, classifier])
    print('###############################')
    print('Training model')
    model = pipeline.fit(train)
    print('Done training')
    print('###############################')

    predictions = model.transform(validation)
    predictions = predictions.select('prediction', 'user_id')

    evaluator = MulticlassClassificationEvaluator(labelCol = "user_id", predictionCol="prediction")
    score = evaluator.evaluate(predictions)

    print(score)

    answered.write.parquet(users_data_outputs, mode='overwrite')

    model.write().overwrite().save(user_model_file)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-posts_src", action="store", dest="posts_src", type=str)
    parser.add_argument("-users_src", action="store", dest="users_src", type=str)
    parser.add_argument("-answered_dest", action="store", dest="answered_dest", type=str)
    parser.add_argument("-users_dest", action="store", dest="users_dest", type=str)
    parser.add_argument("-answer_predictor_dest", action="store", dest="answer_predictor_dest", type=str)
    parser.add_argument("-user_predictor_dest", action="store", dest="user_predictor_dest", type=str)
    args = parser.parse_args() 

    posts_inputs = args.posts_src
    users_inputs = args.users_src
    answer_data_outputs = args.answered_dest
    users_data_outputs = args.users_dest
    answer_model_file = args.answer_predictor_dest
    user_model_file = args.user_predictor_dest

    spark = SparkSession.builder.appName('ML Model builder').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    sc = spark.sparkContext
    sc.setLogLevel('WARN')
    main(posts_inputs, users_inputs, answer_data_outputs, users_data_outputs, answer_model_file, user_model_file)