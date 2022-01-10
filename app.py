import csv
import datetime
import time

import boto3
import pandas as pd

# Static assignments-change as per requirements
athena = boto3.client('athena', region_name='us-east-1')
S3_OUTPUT = 's3://abacus-sandboxes-hzn/dataops/Athena_desk/athena-output/'
db_name = 'hzn_data_view_discovery_domain'
s3 = boto3.resource('s3')
execution_datetime = (str(datetime.datetime.now()).
                      replace(" ", "_").replace(":", "_").
                      replace("-", "_").replace("_", "").
                      split('.')[0])
table_nm = 'dm_member_prod'
ingestionid = ["1774"]
ingestionstring = ','.join(ingestionid)


# Result file to concat all query results
def result_file():
    bucket = s3.Bucket('abacus-sandboxes-hzn')
    prefix_val = "Athena_desk/athena-output/" + execution_datetime
    prof_li = []
    for file in bucket.objects.filter(Prefix=prefix_val):
        if not file.key.endswith('.metadata'):
            prof_li.append(file.key)
    df = pd.concat(map(pd.read_csv, prof_li), ignore_index=False, axis=1)

    df['Non-Null'] = df['TotalCount']-df['NullCount']
    df['Fill'] = df['TotalCount']-df['EmptyCount']
    df['FillPercent'] = round((df['Fill']/df['TotalCount'])*100, 2)
    df['DistinctPercent'] = round(
        (df['distinctcount']/df['TotalCount'])*100, 2)
    df.to_csv(S3_OUTPUT+'output1.csv')


# Storing Queries for profile metrics
    glue = boto3.client('glue', region_name='us-east-1')
    Hmap = {}
    list_1 = []

    glue_table = glue.get_tables(DatabaseName=db_name, MaxResults=1000)
    for table in glue_table['TableList']:
        if (table['Name'] == table_nm):
            for column in table['StorageDescriptor']['Columns']:
                column_name = column['Name']

                sql1 = "(select '" + column_name + "' as attribute,count(distinct " + column_name + ") as distinctcount from " + \
                    db_name+"."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring+"))"
                sql2 = "(select count(*) as NullCount from " + db_name+"."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + ") and " + column_name + " is null)"
                sql3 = "(select count(*) as EmptyCount from " + db_name+"."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + ") and " + column_name + " is null)"
                sql4 = "(select min(length(" + column_name + ")) as MinLength from " + db_name + \
                    "."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + "))"
                sql5 = "(select max(length(" + column_name + ")) as MaxLength from " + db_name+"." + \
                    table_nm + \
                    " where cast(ingestionid as integer ) in ](" + \
                    ingestionstring + "))"
                sql6 = "(select avg(length(" + column_name + ")) as AvgLength from " + db_name + \
                    "."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + "))"
                sql7 = "(select min(" + column_name + ") as StringMinLength from " + db_name+"." + \
                    table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + "))"
                sql8 = "(select max(" + column_name + ") as StringMaxLength from " + db_name+"." + \
                    table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + "))"
                sql9 = "(select count(*) as WhiteSpacePercent from " + db_name+"."+table_nm + \
                    " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + ") and " + column_name + " like '% %')"
                sql10 = "(select count(*) as MaxOneWhiitePercent from " + db_name+"."+table_nm + " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + \
                        ") and length(" + column_name + \
                    ")-length(replace(" + column_name + ",' ','')) > 1)"
                sql11 = "(select '" + column_name + "' as attribute," + column_name + " as value,count(*) as cnt from " + db_name + \
                    "."+table_nm + \
                        " where cast(ingestionid as integer ) in (" + \
                    ingestionstring + ") group by 1,2 limit 100)"
                sql12 = "(select count(*) as TotalCount from " + db_name+"."+table_nm + \
                    " where CAST(ingestionid as integer) in (" + \
                    ingestionstring+"))"

                list_1.append(sql1)
                list_2.append(sql2)
                list_3.append(sql3)
                list_4.append(sql4)
                list_5.append(sql5)
                list_6.append(sql6)
                list_7.append(sql7)
                list_8.append(sql8)
                list_9.append(sql9)
                list_10.append(sql10)
                list_11.append(sql11)
                list_12.append(sql12)

            list_1 = " union all ".join(list_1)
            list_2 = " union all ".join(list_2)
            list_3 = " union all ".join(list_3)
            list_4 = " union all ".join(list_4)
            list_5 = " union all ".join(list_5)
            list_6 = " union all ".join(list_6)
            list_7 = " union all ".join(list_7)
            list_8 = " union all ".join(list_8)
            list_9 = " union all ".join(list_9)
            list_10 = " union all ".join(list_10)
            list_11 = " union all ".join(list_11)
            list_12 = " union all ".join(list_12)

            Hmap['Distinct'] = list_1
            Hmap['NullCount'] = list_2
            Hmap['EmptyCount'] = list_3
            Hmap['MinCount'] = list_4
            Hmap['MaxCount'] = list_5
            Hmap['AvgLength'] = list_6
            Hmap['MinValue'] = list_7
            Hmap['MaxValue'] = list_8
            Hmap['WhiteSpacePercent'] = list_9
            Hmap['MaxOneWhite'] = list_10
            Hmap['Frequency'] = list_11
            Hmap['TotalCount'] = list_12

            for key in Hmap.keys():
                try:
                    athena_execute(Hmap[key])
                except Exception as e:
                    print(e)
    result_file()


# Executing Queries-Sequential execution
def athena_execute(final):
    max_execution = 11
    response = athena.start_query_execution(QueryString=final, QueryExecutionContext={'Database': db_name}, ResultConfiguration={
                                            'OutputLocation': S3_OUTPUT + execution_datetime}, WorkGroup='hzn-dataops')
    execution_id = response['QueryExecutionId']
    # print("QueryExecutionId = " + str(execution_id))
    state = 'QUEUED'
    while (max_execution > 0 and state in ['RUNNING', 'QUEUED']):
        max_execution = max_execution - 1
        print(max_execution)
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in response['QueryExecution']['Status']:
            time.sleep(100)
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                print("Failed")
                return False
            elif state == 'SUCCEEDED':
                print("Success")
                return True


lambda_handler()
