import json
import pdb

from jira.client import JIRA

import credentials as cred
from datetime import datetime as dt
from elasticsearch import Elasticsearch


jira_options = {'server': 'http://ccp.sys.comcast.net'}

# ntid = input("Enter your ntid: ")
# pwd = getpass.getpass("Enter your pwd: ")
ntid = cred.ntid
pwd = cred.pwd

try:
    jira = JIRA(options=jira_options, basic_auth=(ntid, pwd))
except Exception as e:
    print("Error 0901 : Authentication Failed!")
    exit()

# ELK OBJECT CREATED:
es = Elasticsearch('tdk-data-asc-u-001.logging.comcast.net', http_auth=('splash_automatics', '0UeTBkfmot'),
                           verify_certs=False, port=9200, scheme="https")

#Enter valid query: Pass this as input
jira_filter='project ="RDK Broadband" and type=Bug and created>\'2023-01-01 00:00\' and created<\'2023-07-31 23:59\'';



def get_all_issues():
    i = 0;
    chunk_size = 1000;
    total_issues = [];
    while True:
        ticket_details = jira.search_issues(jql_str=jira_filter, startAt=i , maxResults=chunk_size, json_result=True);
        i = i + chunk_size
        total_issues += ticket_details['issues']
        if i >= ticket_details['total']:
            break;
    return total_issues;


issues=get_all_issues()
cleaned_issues = [];
issueTotalCount=len(issues)
print('issueTotalCount: ',issueTotalCount)
issueCount=0
for issue in issues:
    issueCount=issueCount+1
    cleaned_issue = {}
    cleaned_issue['processedTime'] = str(dt.utcnow())
    for issue_field in issue:
        if issue_field == "key":
            uniqueKey = issue[issue_field]
        if issue_field == "fields":
            cleaned_fields = {}
            for field in issue[issue_field]:
                fields = issue[issue_field] #At times comments is huge
                if field == 'comment':
                    continue
                if field == 'description':
                    continue
                elif field == 'issuelinks':
                    linkedIssues = []
                    inwardsIssues = {}
                    outwardsIssues = {}
                    for link in fields[field]:
                        if 'inwardIssue' in link:
                            invalue=link["inwardIssue"]["key"]
                            linkedIssues.append(invalue)
                        elif 'outwardIssue' in link:
                            outvalue = filterOutwardIssue(link)
                            linkedIssues.append(outvalue)
                    cleaned_fields[field]=linkedIssues
                elif field == 'votes':
                    value = fields[field]['votes']
                    cleaned_fields[field] = value
                elif field == 'watches':
                    value = fields[field]['watchCount']
                    cleaned_fields[field] = value
                elif 'customfield_' not in field:
                    fieldvalue=type(fields[field])
                    if fieldvalue is dict:
                        if 'name' in fields[field]:
                            value=fields[field]['name']
                            cleaned_fields[field]=value
                        if 'progress' in fields[field]:
                            value = fields[field]['progress']
                            cleaned_fields[field] = value
                    elif fieldvalue is list:
                        listLen=len(fields[field])
                        value=fields[field]
                        if listLen> 0:
                            concatList=[]
                            for i in range(len(fields[field])):
                                listValues=fields[field][i]
                                listObjTypes=type(listValues)
                                if listObjTypes is dict:
                                    if 'name' in listValues:
                                        value = listValues['name']
                                        cleaned_fields[field] = value
                                elif listObjTypes is str:
                                    concatList.append(listValues)
                            if len(concatList)>0:
                                cleaned_fields[field] = concatList
                        else:
                            cleaned_fields[field] = value
                    else:
                        cleaned_fields[field] = fields[field]
            cleaned_issue["fields"] = cleaned_fields
        else:
            cleaned_issue[issue_field] = issue[issue_field]
    res = es.index(index="jira-fields", doc_type='jira-fields', body=cleaned_issue, id = cleaned_issue["key"],
                                  request_timeout=30);
    cleaned_issues.append(cleaned_issue)