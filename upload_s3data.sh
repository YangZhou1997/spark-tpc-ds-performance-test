# bin/bash
export AWS_ACCESS_KEY_ID=abcd
export AWS_SECRET_ACCESS_KEY=efgh
export AWS_SESSION_TOKEN=ijk
AWS_REGIONS=(us-east-2 us-west-1 ap-southeast-2 us-west-2 ap-northeast-1 ap-south-1 ap-northeast-2 ap-southeast-1 sa-east-1 eu-central-1)
for region in ${AWS_REGIONS[@]}; do 
    hadoop distcp -Dmapreduce.map.memory.mb=3000 -Dmapreduce.reduce.memory.mb=6000 -Dfs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" \
    -Dfs.s3a.access.key="$AWS_ACCESS_KEY_ID" -Dfs.s3a.secret.key="$AWS_SECRET_ACCESS_KEY" -Dfs.s3a.session.token="$AWS_SESSION_TOKEN" -Dfs.s3a.endpoint=s3.${region}.amazonaws.com \
    hdfs://node0:9000/user/hive/warehouse/tpch_flat_parquet_1000.db s3a://speedup-data-$region/ &
done 
wait

