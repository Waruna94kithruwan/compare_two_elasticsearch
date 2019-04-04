import ast
import urllib3
from elasticsearch import Elasticsearch

# Edit this section
ELASTIC_01 = ""
# Compare with below "Elasticsearch" instance
ELASTIC_02 = ""

MATCHES = ""
MISSEDMATCH = ""
ERRORS = ""

es = Elasticsearch(hosts=ELASTIC_01)
result = es.indices.get_alias("*")
index_list = list(result.keys())

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager()

for index_name in index_list:
    responce_01 = http.urlopen('GET', ELASTIC_01+ "/" + index_name + "/_count")
    responce_02 = http.urlopen('GET', ELASTIC_02+ "/" + index_name + "/_count")

    content_one = responce_01._body.decode('utf-8')
    content_two = responce_02._body.decode("utf-8")
    data_one = ast.literal_eval(str(content_one))
    data_two = ast.literal_eval(str(content_two))

    try:
        count_one = data_one['count']
    except Exception as exception:
        ERRORS += index_name + "\n"
        continue
    try:
        count_two = data_two['count']
    except Exception as exception:
        ERRORS += index_name + "\n"
        continue

    if count_one == count_two:
        MATCHES += index_name + ": Docunrmt count\t" + str(count_one) + " : " + str(count_two) +"\n"

    else:
        MISSEDMATCH += index_name + ": Docunrmt count\t" + str(count_one) + " : " + str(count_two) +"\n"

result_file = open("match_result.txt", "a")
result_file.write("MATCH COUNT \n\n" + MATCHES + "\n\nMISSED MATCH COUNT \n\n" + MISSEDMATCH + "\n\nERRORS \n\n" + ERRORS)
