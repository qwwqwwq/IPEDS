#!/bin/sh
# These commands, when executed on the EMR cluster, will run the job
hadoop fs -mkdir -p /jq-emr-bucket/data/
hadoop fs -get s3://jq-emr-bucket/data/institution_data.csv
hadoop fs -cp s3://jq-emr-bucket/data/map_data.csv /jq-emr-bucket/data/map_data.csv
hadoop fs -get s3://jq-emr-bucket/jars/pairwise-1.0-SNAPSHOT.jar
spark-submit --files institution_data.csv \
    --master yarn-cluster \
    --class com.pairwise.PairwiseDistance \
    pairwise-1.0-SNAPSHOT.jar \
    institution_data.csv \
    /jq-emr-bucket/data/map_data.csv \
    /jq-emr-bucket/data/output
hadoop fs -cat /jq-emr-bucket/data/output* > result
hadoop fs -put result s3://jq-emr-bucket/data/output_map_data.csv