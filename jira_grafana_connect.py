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


issues = get_all_issues()
print(len(get_all_issues()));


cleaned_issues = [];
for issue in issues:
    cleaned_issue = {}
    for issue_field in issue:
        if issue_field == "fields":
            cleaned_fields = {}
            for field in issue[issue_field]:
                fields = issue[issue_field]
                if field == 'comment':
                    continue
                if 'customfield_' not in field:
                    cleaned_fields[field] = fields[field]
            cleaned_issue["fields"] = cleaned_fields
        else:
            cleaned_issue[issue_field] = issue[issue_field]
    res = es.index(index="jira-fields", doc_type='jira-fields', body=cleaned_issue, id = cleaned_issue["key"],
                                  request_timeout=30);
    cleaned_issues.append(cleaned_issue)