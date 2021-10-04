## INTRODUCTION

Team-1-project: ETL-AWS

This is a project completed in 4 weeks during the Generation UK&I Data Engineering Bootcamp (London &Birmingham, 2021 Session). The goal of this project is to extract csv from an AWS S3 bucket. The csv files contain orders from different locations across the UK. The extracted files are transformed with Python using Pandas library and uploaded into a redshift database. 

In this project, My team and I built a pipleine to use megabytes of sales data from a retail merchant to develop sales insight that could help the business scale. The results can be used to enhance investment strategies to decide which products sell most at what time of the day and in whcih location. This will enable reduced downtime and enable the business plan ahead.

## Pipeline
The team built a data pipeline that utilizes csv files from AWS S3 bucket.

![Project flow](https://github.com/Richard-code-gig/team-1-project/blob/main/project_flow.png?raw=true)

AWS Lambda to query indexed CSV Files using uploaded dates as keys in the bucket.  
Scan 268 csv files during the project cycle.  
Cleaning, filtering and aggregating Date, Customer, Location, Product, Order and Order-product tables with Pandas in Python.  
Cleaned data is loaded to Redshift.  
Monitoring Lambda invocation on AWS CloudWatch & Grafana and visualising sales trends with Grafana.  

## Requirements
AWS: Setup an AWS account.  
S3: Set up EC2 instance.  
S3: Set up a S3 bucket.  
Postgresql: Set up postgresql connection strings in the docker-compose.yml file or use Redshift.  
Redshift: Set up cluster and database.  
Grafana: Set up Grafana.  
Replace the Redshift configuration in the create_connection.py with your your details.  
Docker: Download Docker Desktop.  
Serverless: Follow instructions to setup Serverless here:  
[https://github.com/infinityworks/data-academy-serverless-example#data-academy-serverless-example](https://github.com/infinityworks/data-academy-serverless-example#data-academy-serverless-example)  
Replace the bucket name and arn strings with yours in the serverless.yml file.  
Grafana: Follow instructions to setup Serverless here:  
[https://medium.com/@miguel.brito/get-grafana-on-aws-ec2-with-redshift-in-just-15-minutes-9947de1ef21](https://medium.com/@miguel.brito/get-grafana-on-aws-ec2-with-redshift-in-just-15-minutes-9947de1ef21)  
CI/CD: To automatically deploy service to AWS when code is pushed to a GitHub branch use the instructions here:  
[https://aws.amazon.com/blogs/compute/using-github-actions-to-deploy-serverless-applications/](https://aws.amazon.com/blogs/compute/using-github-actions-to-deploy-serverless-applications/)  
SQS: Set up AWS SQS.  

## Installation
Git clone https://github.com/Richard-code-gig/team-1-project to install the package folder.  
Initialise a git folder in the project root directory.  
Activate a virtual environment.  
Run pip -r requirements.txt to install python packages.  
cd /.devcontainer && docker build . to build needed images written in the dockerfile.  
run docker-compose up -d to start docker.  

## Third-party libraries
AWS CLI  
AWS SAM CLI  

## Repository Structure and Run Instructions

./.devcontainer/ contains scripts associated with building needed docker images and connections.  
./test/ contains two scripts for testing two of the functions used in this project.  
./src/ contains connection and ETL scripts to launch make connections to specified database (Postgres during local development and Redshift on AWS) and run the etl process.  
Note that this project was developed using Postgres on local machine and Redshift when deployed to AWS cloud.
./src.run_script.py/ contains all python configuration files and scripts for running the project locally and output product info to a local Postgres table.  
./serverless.yml/ contains instruction for AWS S3 and Lambda invocation as well as neccessary functions to include on AWS.  
./main.yml/ contains yml script to initiate AWS/GitHUB CI/CD action.  

# Addon
./src.SQL_queries/ contains some sql queries that can be run on Redshift or Grafana to visualise trends.  

git push -u origin <remote-branch> (This action should automatically download AWS SAM CLI on local machine and deploy to AWS).  

# Development work
My team and I worked together. In the first week we used Trello to communicate the project set up, team structure and GiHub actions. The project flow through setting up Docker and necessary environments, extraction, transformation and loading test csv files to postgresql database in the first 2 weeks. While the final 2 weeks was used for unitesting, AWS service set up, writing yml configuration files, CI/CD, visualization, etc. This programme ingested data from AWS S3, cleaned and filtered with python script and aggregated product information in a cloud Amazon Redshift database.  

## Languages
Python  
Node.js  
Bash  

## Technologies  
Pandas   
Numpy  
AWS EC2, Lambda, Redshift, S3, Cloudwatch, SQS and Grafana  
Docker  
Postgresql  
