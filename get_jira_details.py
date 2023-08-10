import json
import pdb

from jira.client import JIRA

import credentials as cred

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

#Enter valid query: Pass this as input
jira_filter='project ="RDK Broadband" and type=Bug and created>\'2023-02-13 00:00\' and created<\'2023-02-13 00:59\'';

ticket_details = jira.search_issues(jira_filter);

print(ticket_details)
print("--------------------------------------------------")
for row in ticket_details:
    print(row.fields)
    print(json.dumps(row.raw));
print("--------------------------------------------------")