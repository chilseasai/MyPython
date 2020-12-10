from boto3.dynamodb.conditions import Key, Attr
import boto3
from pprint import pprint


def read_sbi_reward_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('TABLE_SBI_REWARD')
    response = table.get_item(
        Key={
            'hashKey': 'amzn1.sbi.v1.TRYPANEU_FEE_EXEMPTION.8282697835.5',
            'rangeKey': 'B07NGBJ42Q'
        }
    )
    item = response['Item']
    pprint(item, sort_dicts=False)


def read_sbi_document_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('TABLE_SBI_DOCUMENT')
    response = table.get_item(
        Key={
            'hashKey': 'amzn1.sbi.v1.TRYPANEU.35794124712.101611.B00H1OQH3M',
            'rangeKey': '2020-08-20T00:00:00:000Z'
        }
    )
    item = response['Item']
    pprint(item, sort_dicts=False)


def delete_sbi_reward_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('TABLE_SBI_REWARD')
    scan = None
    with table.batch_writer() as batch:
        count = 0
        while scan is None or 'LastEvaluatedKey' in scan:
            if scan is not None and 'LastEvaluatedKey' in scan:
                scan = table.scan(
                    FilterExpression=Attr('rewardType').eq('TRYPANEU_FEE_EXEMPTION'),
                    ExclusiveStartKey=scan['LastEvaluatedKey'],
                )
            else:
                scan = table.scan(
                    FilterExpression=Attr('rewardType').eq('TRYPANEU_FEE_EXEMPTION')
                )
            for item in scan['Items']:
                if count % 5000 == 0:
                    print(count)
                # pprint(item)
                batch.delete_item(Key={'hashKey': item['hashKey'], 'rangeKey': item['rangeKey']})
                count = count + 1


def delete_sbi_document_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('TABLE_SBI_DOCUMENT')
    scan = None
    with table.batch_writer() as batch:
        count = 0
        while scan is None or 'LastEvaluatedKey' in scan:
            if scan is not None and 'LastEvaluatedKey' in scan:
                scan = table.scan(
                    FilterExpression=Attr('programType').eq('TRYPANEU'),
                    ExclusiveStartKey=scan['LastEvaluatedKey'],
                )
            else:
                scan = table.scan(
                    FilterExpression=Attr('programType').eq('TRYPANEU')
                )
            for item in scan['Items']:
                if count % 5000 == 0:
                    print(count)
                pprint(item)
                batch.delete_item(Key={'hashKey': item['hashKey'], 'rangeKey': item['rangeKey']})
                count = count + 1


def count_sbi_reward_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('TABLE_SBI_REWARD')
    scan = None
    count = 0
    removal_count = 0
    creturn_count = 0
    inbound_count = 0
    other_count = 0
    pprint("count_sbi_reward_item")
    while scan is None or 'LastEvaluatedKey' in scan:
        if scan is not None and 'LastEvaluatedKey' in scan:
            scan = table.scan(
                ExclusiveStartKey=scan['LastEvaluatedKey'],
                FilterExpression=Attr('rewardType').eq('REMOVAL_FEE_EXEMPTION')
                                 | Attr('rewardType').eq("CRETURN_FEE_EXEMPTION")
                                 | Attr('rewardType').eq("INBOUND_FEE_EXEMPTION")
            )
        else:
            scan = table.scan(FilterExpression=Attr('rewardType').eq('REMOVAL_FEE_EXEMPTION')
                                               | Attr('rewardType').eq("CRETURN_FEE_EXEMPTION")
                                               | Attr('rewardType').eq("INBOUND_FEE_EXEMPTION")
                              )
        for item in scan['Items']:
            if 'REMOVAL_FEE_EXEMPTION' == item['rewardType']:
                removal_count = removal_count + 1
            elif 'CRETURN_FEE_EXEMPTION' == item['rewardType']:
                creturn_count = creturn_count + 1
            elif 'INBOUND_FEE_EXEMPTION' == item['rewardType']:
                inbound_count = inbound_count + 1
            else:
                other_count = other_count + 1
            count = count + 1
            # if count % 5000 == 0:
            #     print(count)
            print("Count: ", scan['Count'], "ScannedCount: ", scan['ScannedCount'])
    print("count: %s", count)
    print("removal_count: %s", removal_count)
    print("creturn_count: %s", creturn_count)
    print("inbound_count: %s", inbound_count)
    print("other_count: %s", other_count)


def count_discount_pool_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('test-EUAmazon-DiscountPool')
    scan = None
    count = 0
    removal_count = 0
    creturn_count = 0
    inbound_count = 0
    other_count = 0
    pprint("count_discount_pool_item")
    while scan is None or 'LastEvaluatedKey' in scan:
        if scan is not None and 'LastEvaluatedKey' in scan:
            scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
        else:
            scan = table.scan()
        for item in scan['Items']:
            if 'Removal' in item['discountPoolId']:
                removal_count = removal_count + 1
            elif 'CReturn' in item['discountPoolId']:
                creturn_count = creturn_count + 1
            elif 'Inbound' in item['discountPoolId']:
                inbound_count = inbound_count + 1
            else:
                other_count = other_count + 1
            count = count + 1
            # if count % 5000 == 0:
            #     print(count)
    print("count: %s", count)
    print("removal_count: %s", removal_count)
    print("creturn_count: %s", creturn_count)
    print("inbound_count: %s", inbound_count)
    print("other_count: %s", other_count)


def delete_discount_pool_item():
    dynamodb = boto3.resource('dynamodb', region_name='eu-west-1')
    table = dynamodb.Table('test-EUAmazon-DiscountPool')
    scan = None
    with table.batch_writer() as batch:
        count = 0
        while scan is None or 'LastEvaluatedKey' in scan:
            if scan is not None and 'LastEvaluatedKey' in scan:
                scan = table.scan(ExclusiveStartKey=scan['LastEvaluatedKey'])
            else:
                scan = table.scan()
            for item in scan['Items']:
                if count % 5000 == 0:
                    print(count)
                pprint(item)
                batch.delete_item(Key={'hashKey': item['discountPoolId']})
                count = count + 1


if __name__ == '__main__':
    # delete_discount_pool_item()
    # count_discount_pool_item()
    count_sbi_reward_item()
