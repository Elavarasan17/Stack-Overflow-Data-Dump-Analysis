import streamlit as st

from pyspark.sql import types, functions
from pyspark.ml.feature import IndexToString
from pyspark.ml.functions import vector_to_array

schema = types.StructType([
    types.StructField('title', types.StringType()),
    types.StructField('tags', types.ArrayType(types.StringType())),
    types.StructField('id', types.StringType()),
])

    # The labels are indexed with a string indexer. So they they have to converted back to string to get the correct user_id

def reverse_indexer(inputCol, outputCol, original_labels):
    return IndexToString(
        inputCol=inputCol, outputCol=outputCol, labels=original_labels)

def form_tags(x):
    tags = []
    print(x.split(','))
    for i in x.split(','):
        print(i)
        print(i.strip())
        if(i != ''):
            tags.append(i.strip())
    return tags

def app(args, spark, model):
    random_user = args
    st.header('Who will answer the question')
    tag = st.text_input("Please enter tags")
    tags = form_tags(tag)
    st.write(tags)
    title = st.text_input('Please enter the question title').strip()
    question = (title, tags, random_user)
    input_data = spark.createDataFrame([question], schema)

    predictions = model.transform(input_data)
    result = predictions.select('prediction', vector_to_array("probability").alias('probs_array')) \
        .withColumn('probs', functions.expr('sort_array(transform(probs_array, (x,i) -> (x as val, i as idx)), False)')) \
        .selectExpr(
            'prediction',
            'probs[0].val as max_1',
            'probs[0].idx as i_1',
            'probs[1].val as max_2',
            'probs[1].idx as i_2',
            'probs[2].val as max_3',
            'probs[2].idx as i_3'
        )

    
    # Getting the original labels from the pipeline stage and creating reverse indexers for the largest probability indexes obtained
    # after prediction. These return the users in the order of probability specified

    original_labels = model.stages[0].labels

    pred_to_label = reverse_indexer(inputCol='prediction', outputCol='original_id', original_labels=original_labels)
    p1 = pred_to_label.transform(result) 
    largest_1_to_label = reverse_indexer(inputCol='i_1', outputCol='pred1', original_labels=original_labels) 
    p2 = largest_1_to_label.transform(p1)
    largest_2_to_label = reverse_indexer(inputCol='i_2', outputCol='pred2', original_labels=original_labels) 
    p3 = largest_2_to_label.transform(p2)
    largest_3_to_label = reverse_indexer(inputCol='i_3', outputCol='pred3', original_labels=original_labels)
    p4 = largest_3_to_label.transform(p3).first()

    if (len(tags) > 0) and (title != ''):
        stackoverflow_url = 'https://stackoverflow.com/users/{}'
        pred1 = stackoverflow_url.format(p4['pred1'])
        pred2 = stackoverflow_url.format(p4['pred2'])
        pred3 = stackoverflow_url.format(p4['pred3'])
        st.write('The suggested user to answer for these domains can be found here: ')
        st.markdown('<a target="_blank" href="{}">User 1</a>'.format(pred1), unsafe_allow_html=True)
        st.markdown('<a target="_blank" href="{}">User 2</a>'.format(pred2), unsafe_allow_html=True)
        st.markdown('<a target="_blank" href="{}">User 3</a>'.format(pred3), unsafe_allow_html=True)
