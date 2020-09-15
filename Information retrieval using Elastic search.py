import jsonlines
import json
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


with open('C:\\Users\\rohin\\Downloads\\signalmedia-1m.jsonl\\sample-1M.jsonl', 'rb') as f:
    k=0
    
    for item in jsonlines.Reader(f):
        if(k>5000):
            continue
        else:
            print("-----------------------------------------------------------")
            print(k)

            print(item)
            print(type(item))
            item = json.dumps(item)
            decoded = json.loads(item)
            print(decoded['id'])
            es.index(index='news_article', doc_type='articles',id = k, body = decoded)
        k+=1
print(k)
print('------------------------------------------')
#print(es.get(index='news_article', doc_type='articles', id=2))
print('---------------------------------------------')
res = es.search(index="news_article", doc_type="articles", body={"query": {"match": {"content": "kuala lumpur"}}})
print(res)
for doc in res['hits']['hits']:
    print(doc['_id'])

''''
import json

result = [json.loads(jline) for jline in sample-1M.split('\n')]
result = [json.loads(jline) for jline in response.read().split('\n')]
'''
from elasticsearch import Elasticsearch

tdocument = 5000
esearch = Elasticsearch([{'host': 'localhost', 'port': 9200}])  # set the connection to connect to Elasticserver


# Exclusive or of two lists to map the time attempt that hit the relevant document
def map(m,p):
    return list(set(m) ^ set(p))

# Calculate the precision and recall at K point and summation of them
def sumation(p,r):
    avg_p = 0.0
    avg_r = 0.0
    cal = 0
    for k in range(tdocument):
        if no_of_attempt[k] == 'relevant':
            p += 1.0
            r += 1.0
            temp_p = p / (k + 1)
            avg_p += temp_p
            temp_r = r / tdocument
            temp_p = round(temp_p, 7)
            temp_r = round(temp_r, 7)
            avg_r += temp_r
            print("@K", k + 1, "| P=", temp_p, "| R=", temp_r, "Document id:", hit_collection[cal])
            cal += 1
        else:
            temp_p = p / (k + 1)
            temp_r = r / tdocument
            avg_p += temp_p
            avg_r += temp_r
            temp_p = round(temp_p, 7)
            temp_r = round(temp_r, 7)
            print("@K", k + 1, "| P=", temp_p, "| R=", temp_r)
    print("Avgerage of precision: =>", avg_p / tdocument)
    print("Avgerage of Recall: =>", avg_r / tdocument)
    


# Print the selection menu to  the screen
print(
    "Searching the documents based on the below question:"
    "\n1.Keyword Search in 'content' - Compare iphone cameras"
    "\n2.Prefix Search for 'Content' for 'media-type' Blog"
    "\n3.Content Search for a date range"
    "\n4.Fuzzy keyword Search"
    "\n5.Search title for a keyword"
    "\n6.Wild card search in 'Source'"
    "\n7.Search content for a keyword with a date range")

    
select = input("\nEnter your question number:")
if select == '1':
    q = input("Please enter the title:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query": {
          "match": {
               "content": {
                 "query": q
                 "operator": "and"
                 }
             }
         }
     }
         , size=tdocument)


if select == '2':
    q = input("Please enter the query:")
    m = input("Please enter the media type to be searched:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase_prefix": {
                            "content": q
                        }
                    },
                    {
                        "match_phrase_prefix": {
                            "media-type": m

                        }
                    },

                ]
            }
        }
    }
                    , size=tdocument)

if select == '3':
    q = input("Please enter the query:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query":{
            "bool":{
                "must":[
                    {
                        "match_phrase_prefix":{
                        "content":q
                    }
                    },
                    {
                    "range":{
                        "published":{
                        "gte":
                        "2010/01/01"
                    ,
                    "lte":
                    "2016/31/12"
                    ,
                    "format":
                    "yyyy/mm/dd||yyyy"
                       }
                     }
                    }

                ]
            }
        }
    }
                    , size=tdocument)


if select == '4':
    q = input("Please enter the query:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query": {
            "fuzzy": {
                "content": {
                    "value" : "mratek",
                    "fuzziness": q
                }

            }
        }
    }
                    , size=tdocument)

if select == '5':
    q = input("Please enter the query:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase_prefix": {
                            "title": q
                        }
                    },

                ]
            }
        }
    }
                    , size=tdocument)

if select == '6':
    q = input("Please enter the query:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query": {
            "wildcard": {
                "source":{
                     "value":"research*",
                     "boost": 1.0,
                     "rewrite": q

                        }
            }

        }
    }
                    , size=tdocument)


if select == '7':
    q = input("Please enter the query:")
    choose = esearch.search(index="news_article", doc_type="articles", body=
    {
        "query":{
            "bool":{
                "must":[
                    {
                        "match_phrase_prefix":{
                        "content":q
                    }
                    },
                    {
                    "range":{
                        "published":{
                        "gte":
                        "2015/01/01"
                    ,
                    "format":
                    "yyyy/mm/dd||yyyy"
                       }
                     }
                    }

                ]
            }
        }
    }
                    , size=tdocument)


number = 1
hits = len(choose['hits']['hits'])  # set number of document in the particular search
print("\nDocuments in database:", tdocument)
print("Documents retrieved :", hits)
# loop to display all the information of the document

for doc in choose['hits']['hits']:
    print("\n==========================================", number, "/", len(choose['hits']['hits']),
          "==================================================")
    number += 1
    print("Document ID:", doc['_id'])
    print("Search score:", doc['_score'])
    print("Media type:", doc['_source']['media-type'])
    print("Title:", doc['_source']['title'])
    print("From source:", doc['_source']['source'])
    print("Published:", doc['_source']['published'])
#    print("Content:", doc['_source']['content'], "\n")

no_of_attempt = list(range(1, tdocument + 1))  # create the numbers of list = to number of document in the database
t_attempt = []
# loop to add the hits attemps
for doc in choose['hits']['hits']:
    t_attempt.append((doc['_id']))
# turn them to integer
t_attempt = [int(x) for x in t_attempt]
t_attempt.sort()  # then sort it

hit_collection = time_doc_found = t_attempt
b_mapping = map(t_attempt, no_of_attempt)  # call the mapping function
print (b_mapping)
print(no_of_attempt)
# convert the hit list to binary list
for i in range(tdocument):
    print(x)
    print(i+1)
    for x in t_attempt:
        if x == i + 1:
            no_of_attempt[i] = 'relevant'
    for y in b_mapping:
        if y == i + 1:
            no_of_attempt[i] = 'none'
# print the ranked retrieval
print(no_of_attempt)

print("\nRETRIEVAL")

sumation(0.00, 0.00)

