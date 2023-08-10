import json
from jira.client import JIRA
import credentials as cred
from elasticsearch import Elasticsearch


jira_options = {'server': 'http://ccp.sys.comcast.net'}

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

# Enter valid query: Pass this as input
# project ="RDK Broadband" and type=Bug and created>= '2023-08-02 00:00' and created<'2023-08-02 23:59'
jira_filter='project ="RDK Broadband" and type=Bug and created>\'2023-08-02 00:00\' and created<\'2023-08-02 23:59\'';

ticket_details = jira.search_issues(jql_str = jira_filter, json_result = True)

# fetch issues from response from JIRA
issues = ticket_details.get('issues')

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
    cleaned_issues.append(cleaned_issue)

print("Cleaned Issues : " + json.dumps(cleaned_issues))