## Table of Contents
 - Installation
 - Problem Statement
 - File Structure
 - Outcomes of Analysis
 - Licenses and Acknowledgement

## Installation
Kindly follow the  [running.md](https://csil-git1.cs.surrey.sfu.ca/nra38/cmpt732-dataprocessors-stackoverflow-analysis/-/blob/master/RUNNING.MD) file in the directory which contains step-wise the installation procedures.

##  Problem Statement

Stack overflow is a question-and-answer forum for technical enthusiasts where users are free to ask and answer questions. Although the forum is filled with professional experts the average response time for each question is around **4 days** which is quite a lot of time. Due to this long turnaround time, in some cases, it is hard to tap the full potential of a developer. If this turnaround time is reduced by directing the posts to the right person at the right time, then the questions will be answered quickly, thereby decreasing the time spent on solving the doubtful questions. The main objective of the project is to analyze the stack overflow data dump to derive actionable insights by answering the following question which could help to reduce the response time, thereby increasing the productivity of the tech wizards,
Questions:  
1. Active Users from 2015 based on the number of Q/A posted.  
2. Top 10 expert users in each tag based on upvotes.
3. Average response time per Tag.
4. Trend analysis of Badges and Tags.
5. Topographical visuals of Tags and Badges.

## File Structure
Please find the file structure below,
```
├── README.md
├── app
│   ├── Dockerfile
│   ├── answered_app.py
│   ├── app.py
│   ├── requirements.txt
│   └── users_app.py
├── src
│   ├── badges_features.py
│   ├── badges_trends.py
│   ├── explorer.py
│   ├── location.py
│   ├── ml-model-builder.py
│   ├── preprocessing
│   │   ├── Tags.py
│   │   ├── comments.py
│   │   ├── posts.py
│   │   ├── users.py
│   │   └── votes.py
│   ├── user-answers.py
│   ├── xml_parquet_badges.py
│   └── xml_parquet_postlinks.py
├─── start-up
│   ├── start-up-linux.sh
│   └── start-up-mac.sh
└─── output
````

## Outcome of Analysis

The visualizations of the extensive stack overflow data analysis can be found in output folder.

## Licenses and Acknowledgement
The data set was obtained from [stackexchange](https://archive.org/download/stackexchange). Data processing, storage and visualization are done using Google Cloud Platform.