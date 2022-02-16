import streamlit as st
import plotly.express as px
import math

from datetime import datetime
from pyspark.sql import types

schema = types.StructType([
    types.StructField('id', types.IntegerType()),
    types.StructField('reputation', types.IntegerType()),
    types.StructField('display_name', types.StringType()),
    types.StructField('location', types.StringType()),
    types.StructField('last_access_date', types.TimestampType()),
    types.StructField('title', types.StringType()),
    types.StructField('tags', types.ArrayType(types.StringType())),
    types.StructField('answered', types.IntegerType()),
])

def form_tags(x):
    tags = []
    for i in x.split(','):
        if(i != ''):
            tags.append(i.strip())
    return tags

def app(args, spark, model):
    data = args[0]
    prob = args[1]
    score = args[2]

    st.header('Will the user answer')
    option = st.selectbox(
        'What is the user id?',
        tuple(data['id'].unique()))
    user_id = option.item()
    reputation = st.text_input("Please enter reputation")
    if(reputation != ''):
        reputation = int(reputation)
    else:
        reputation = 0
    tag = st.text_input("Please enter tags")
    tags = form_tags(tag)
    st.write(tags)
    title = st.text_input('Please enter the question title').strip()

    '''

        creating the user tuple to input to the model

    '''
    user = (user_id, reputation, 'test-name', '', datetime.strptime('2021-12-03 22:03:04', '%Y-%m-%d %H:%M:%S'), title, tags, 1)
    input_data = spark.createDataFrame([user], schema)
    predictions = model.transform(input_data).select('prediction').first()['prediction']
    probability_to_answer = prob[(prob['id'] == user_id) & prob['tag'].isin(tags)]['percent'].mean()

    
        
    '''

    Adding a weighted probability based on the model and history of answering activity of the particular user. There is a higher
    weight for the activity of the user as the model tends to overfit. 

    '''

    weighted_prediction = 0.4 * predictions
    weighted_prob = 0
    if(not math.isnan(probability_to_answer)):
        weighted_prob = 0.6 * (probability_to_answer / 100)
    if(title != ''):
        st.write('Prediction : {}'.format(round(weighted_prediction + weighted_prob)))

    with st.expander('Classification confusion Matrix'):
        fig = px.density_heatmap(data, x="answered", y="prediction")#, height=400)
        st.plotly_chart(fig)
        st.write('Accuracy: {}'.format(score))
    with st.expander("View data"):
        st.dataframe(data)
    