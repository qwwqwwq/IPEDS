# Run pairwise institution distance for each geotiff pixel on EMR
from boto import s3, emr
import argparse
import logging
import time
import sys

logger = logging.getLogger('run_pairwise_calc')
logger.setLevel(logging.INFO)

SCRIPT_RUNNER='s3://elasticmapreduce/libs/script-runner/script-runner.jar'
COPY='/home/hadoop/lib/emr-s3distcp-1.0.jar'


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--jar', name='jar')
    parser.add_argument('--raster_csv', name='raster_csv')
    parser.add_argument('--institution_csv', name='institution_csv')
    parser.add_argument('--output_s3_bucket', name='bucket')
    parser.add_argument('--output_s3_prefix', name='prefix')
    parser.add_argument('--output_file', name='output_file')


def upload_inputs(jar, raster_csv, institution_csv, bucket, prefix):
    s3_conn = s3.connect_to_region('us-west-2')
    bucket = s3_conn.get_bucket(bucket)
    raster_csv_key = bucket.new_key(prefix + '/raster_data.csv')
    raster_csv_key.set_contents_from_filename(raster_csv)
    institution_csv_key = bucket.new_key(prefix + '/institution_data.csv')
    institution_csv_key.set_contents_from_filename(institution_csv)
    jar_key = bucket.new_key(prefix + '/jar.jar')
    jar_key.set_contents_from_filename(jar)


def collect_output(bucket, prefix, output_file):
    s3_conn = s3.connect_to_region('us-west-2')
    bucket = s3_conn.get_bucket(bucket)

    keys = bucket.list(prefix=prefix + '/output')
    with open(output_file, 'w') as of:
        for k in keys:
            k.get_contents_to_file(of)


def run_emr(bucket, prefix):
    conn = emr.connect_to_region('us-west-2')
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
                               '--files', 'institution_data.csv',
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
    bootstrap_actions = [
        emr.BootstrapAction('install-spark',
                            path='file:///usr/share/aws/emr/install-spark/install-spark',
                            bootstrap_action_args=['-x'])]

    jid = conn.run_jobflow(log_uri='s3://' + bucket + '/' + prefix + '/logs',
                     master_instance_type='m3.xlarge',
                     slave_instance_type='m3.xlarge',
                     num_instances=10,
                     enable_debugging=True,
                     ami_version='3.8',
                     visible_to_all_users=True,
                     steps=steps,
                     bootstrap_actions=bootstrap_actions)

    logger.info("Running jobflow: " + jid)
    while True:
        time.sleep(15)
        state = conn.describe_cluster(jid).status.state
        logger.info("Jobflow " + jid + ": " + state)
        if state == 'TERMINATED':
            break
        elif state == 'TERMINATED_WITH_ERRORS':
            sys.exit(1)

def main():
    args = get_args()
    upload_inputs(args.jar, args.raster_csv, args.institution_csv, args.bucket, args.prefix)
    run_emr(args.bucket, args.prefix)
    collect_output(args.bucket, args.prefix, args.output_file)

if __name__ == '__main__':
    main()
