# Run pairwise institution distance for each geotiff pixel on EMR
from boto import s3, emr
import argparse
import logging

logger = logging.getLogger('run_pairwise_calc')
logger.setLevel(logging.INFO)

SCRIPT_RUNNER='s3://elasticmapreduce/libs/script-runner/script-runner.jar'
COPY='/home/hadoop/lib/emr-s3distcp-1.0.jar'


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jar', name='jar')
    parser.add_argument('--raster-csv', name='raster_csv')
    parser.add_argument('--institution-csv', name='institution_csv')
    parser.add_argument('--output_s3_bucket', name='output_s3_bucket')
    parser.add_argument('--output_s3_prefix', name='output_s3_prefix')


def upload_inputs(jar, raster_csv, institution_csv, bucket, prefix):
    s3_conn = s3.connect_to_region('us-west-2')
    bucket = s3_conn.get_bucket(bucket)
    raster_csv_key = bucket.new_key(prefix + '/raster_data.csv')
    raster_csv_key.set_contents_from_filename(raster_csv)
    institution_csv_key = bucket.new_key(prefix + '/institution_data.csv')
    institution_csv_key.set_contents_from_filename(institution_csv)
    jar_key = bucket.new_key(prefix + '/jar.jar')
    jar_key.set_contents_from_filename(jar)


def run_emr(bucket, prefix):
    steps = [
        emr.JarStep('makedir', jar=SCRIPT_RUNNER, step_args=['hadoop', 'fs', '-mkdir', '-p', '/data/']),
        emr.JarStep('download_to_client', jar=SCRIPT_RUNNER,
                    step_args=['hadoop', 'fs', '-get',
                               's3://' + bucket + '/' + prefix + '/institution_data.csv']),
        emr.JarStep('download_to_hdfs', jar=SCRIPT_RUNNER,
                    step_args=['hadoop', 'fs', '-cp',
                               's3://' + bucket + '/' + prefix + '/raster_data.csv',
                               '/data/raster_data.csv']),
        emr.JarStep('download_jar_to_client', jar=SCRIPT_RUNNER,
                    step_args=['hadoop', 'fs', '-get',
                               's3://' + bucket + '/' + prefix + '/jar.jar']),
        emr.JarStep('run_spark', jar=SCRIPT_RUNNER,
                    step_args=['spark-submit',
                               '--files institution_data.csv',
                               '--master', 'yarn-cluster',
                               '--class', 'com.pairwise.PairwiseDistance',
                               'jar.jar',
                               'institution_data.csv',
                               '/data/raster_data.csv',
                               '/data/output',
                               '20']),
        emr.JarStep('get_output', jar=COPY,
                    step_args=['--src', '/data/output', '--dest',
                               's3://' + bucket + '/' + prefix + '/output'])]

    








def main():
    pass

if __name__ == '__main__':
    main()
