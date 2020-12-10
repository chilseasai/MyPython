import boto3


def read_s3():
    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all():
        print(bucket.name)


if __name__ == '__main__':
    read_s3()
