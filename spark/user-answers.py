from pyspark.sql import SparkSession, functions
from pyspark.sql.functions import *
import sys
import argparse
assert sys.version_info >= (3, 5)

def main(posts_inputs, users_inputs, votes_inputs,temp_bucket_input,dataset_output):
    posts = spark.read.parquet(posts_inputs).cache()
    users = spark.read.parquet(users_inputs)
    votes = spark.read.parquet(votes_inputs) 

    questions = posts.filter(posts['post_type_id'] == 1) \
        .select(posts['post_id'].alias('question_id'), posts['tags'], posts['accepted_answer_id'], posts['title'], posts['owner_user_id'].alias('asker'), posts['creation_date'].alias('asked_date'))
    answers = posts.filter(posts['post_type_id'] == 2) \
        .select(posts['post_id'].alias('answer_id'), posts['parent_id'].alias('parent_question'), posts['owner_user_id'].alias('answerer'), posts['creation_date'].alias('answered_date'))
    answers_with_questions = answers.join(questions, [answers['parent_question'] == questions['question_id']])
    answers_by_users = users.join(answers_with_questions, [users['id'] == answers_with_questions['answerer']]) \
        .select(users['id'], users['reputation'], users['display_name'], users['location'], users['last_access_date'],
            answers_with_questions['title'],
            answers_with_questions['tags'],
            answers_with_questions['asked_date'], answers_with_questions['answered_date'],
            answers_with_questions['answer_id'].isNotNull().alias('answered').cast('integer'),
            answers_with_questions['answer_id'].eqNullSafe(answers_with_questions['accepted_answer_id']).alias('accepted').cast('integer'),
            answers_with_questions['answer_id'])

#user expertise on tags
    ans = answers_by_users
    ans1 = ans.drop("last_access_date","title","asked_date","answered_date","reputation")
    ans2 = ans1.join(votes,ans['answer_id'] == votes['post_id']).select(ans['id'],ans['display_name'],ans['tags'],votes['upvotes'],ans['location'],ans['answered'])
    
    ans3 = ans2.withColumn("new",functions.arrays_zip("tags")).withColumn("new", functions.explode("new")).select('id','display_name',functions.col("new.tags").alias("tags"),'upvotes')
    ans4 = ans3.groupBy('id','display_name','tags').agg(functions.max('upvotes').alias('Totupvotes'))
    ans5 = ans3.groupBy('id','display_name').agg(functions.max('upvotes').alias('Totalupvotes'))
    user1 = ans4.join(ans5.alias('mxt'),[(ans4['id'] == ans5['id']) & (ans4['Totupvotes'] == ans5['Totalupvotes'])]).select('mxt.id','mxt.display_name',ans4.tags)
    user_exp = user1.groupBy('id','display_name').agg(functions.collect_list('tags').alias('tags')).cache()
    #user_exp.show()
  
#Expert User in each Tag
    filtered_answers_df = answers_with_questions.drop('parent_question', 'answered_date', 'question_id', 'title', 'asked_date', 'asker')
    upvoted_answers_df = filtered_answers_df.join(votes, filtered_answers_df.answer_id == votes.post_id)\
        .select(filtered_answers_df.answer_id, filtered_answers_df.answerer, explode(filtered_answers_df.tags).alias('tag_name'), votes.upvotes)
    upvote_anscount_df = upvoted_answers_df.groupBy('tag_name', 'answerer').agg(sum('upvotes').alias('total_upvotes'))
    expert_user_tag = upvote_anscount_df.join(users,[upvote_anscount_df['answerer']==users['id']])\
        .select(['id','display_name', 'reputation','tag_name','total_upvotes']).orderBy(upvote_anscount_df.total_upvotes.desc(), upvote_anscount_df.tag_name.asc())

#Top tag's location for answers
    loc_ans = ans2
    loc_ans = loc_ans.drop("id","display_name")
    loc_ans2 = loc_ans.filter((loc_ans['answered'] == 1) & (loc_ans['upvotes'] > 1))
    tag_location = loc_ans2.withColumn("new",functions.arrays_zip("tags")).withColumn("new", functions.explode("new")).select(functions.col("new.tags").alias("tags"),'location')
  
#Average response time for each tag
    discussions = answers.join(questions, [answers['parent_question'] == questions['question_id']])
    first_answer_df = discussions.groupby(discussions['question_id']).agg(functions.min(discussions['answered_date']).alias('first_answered'))
    discussions = discussions.join(first_answer_df, [discussions['question_id'] == first_answer_df['question_id']]).drop(first_answer_df['question_id'])
    average = functions.avg('diff').alias('avg_response_time')
    min_diff = (discussions['first_answered'].cast('long') - discussions['asked_date'].cast('long'))/60
    response_per_question = discussions.select(discussions['question_id'], functions.explode(discussions['tags']).alias('tag'), discussions['asked_date'], discussions['answered_date'], 
    min_diff.alias('diff')).cache()
    response_per_tag = response_per_question.groupby('tag', year(response_per_question['asked_date']).alias('year'), month(response_per_question['asked_date']).alias('month')).agg(average)

#Unanswered Question for each tag
    unanswered_questions = questions.join(answers, [answers['parent_question'] == questions['question_id']], 'leftouter').filter(answers['answer_id'].isNull() & questions['title'].isNotNull()).\
        select(['question_id', 'tags', 'title', 'asked_date'])
    vote_difference = votes.withColumn('difference', abs(votes['upvotes']-votes['downvotes']))
    vote_difference = vote_difference.filter(vote_difference['difference'].isNotNull()).withColumnRenamed('post_id', 'contro_post_id')
    question_no_answer_df = unanswered_questions.join(vote_difference, [vote_difference['contro_post_id'] == unanswered_questions['question_id']]).drop('contro_post_id')
    question_no_answer_df = question_no_answer_df.select(question_no_answer_df['question_id'], question_no_answer_df['title'], question_no_answer_df['tags'] ,to_date(question_no_answer_df['asked_date']).alias('date_posted'), question_no_answer_df['upvotes'], question_no_answer_df['downvotes'], question_no_answer_df['difference'])\
        .orderBy(question_no_answer_df['upvotes'].desc())

#Acceptance rate for each user across all tags
    answers_count = answers_by_users.groupBy(answers_by_users.id).agg(sum(answers_by_users.answered).alias('total_answers'), sum(answers_by_users.accepted).alias('total_accepted_answers'))
    answers_acceptance = answers_count.withColumn('acceptance_percentage', (answers_count['total_accepted_answers']/answers_count['total_answers'])*100.0).withColumnRenamed('id','user_id')
    user_ans_acceptance = answers_by_users.join(answers_acceptance, [answers_acceptance.user_id==answers_by_users.id])\
        .groupBy(answers_by_users.id, answers_by_users.display_name).agg(max(answers_acceptance.acceptance_percentage).alias('total_acceptance_percent'))
   
#write
    user_exp.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_output+".user_tag_expertise").save()
    tag_location.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_output+".tag_answer_locations").save()
    response_per_tag.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table',dataset_output+".tag_response_time").save()
    expert_user_tag.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table', dataset_output+".expert_user_each_tag").save()
    user_ans_acceptance.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table', dataset_output+".acceptance_percentage").save() 
    question_no_answer_df.write.mode('overwrite').format('bigquery').option("temporaryGcsBucket",temp_bucket_input).option('table', dataset_output+".contro_questions").save() 


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-posts_src", action="store", dest="posts_src", type=str)
    parser.add_argument("-users_src", action="store", dest="users_src", type=str)
    parser.add_argument("-votes_src", action="store", dest="votes_src", type=str)
    parser.add_argument("-tempbucket_src", action="store", dest="tempbucket_src", type=str)
    parser.add_argument("-dataset_output", action="store", dest="dataset_output", type=str)
    args = parser.parse_args() 

    posts_inputs = args.posts_src
    users_inputs = args.users_src
    votes_inputs = args.votes_src
    temp_bucket_input = args.tempbucket_src
    dataset_output = args.dataset_output

    spark = SparkSession.builder.appName('User Posts explorer').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    sc = spark.sparkContext
    main(posts_inputs, users_inputs, votes_inputs,temp_bucket_input,dataset_output)