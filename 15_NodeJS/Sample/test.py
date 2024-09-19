import requests

headers = {
    # Already added when you pass json= but not when you pass data=
    'Content-Type': 'application/json',
    'api-key': 'HAtcrzIySAOTlxLVsaHVVkASvHHfe1sqbmK3OFtoUv52U7oGtS7sCCX2mX9wWGgl',
}

json_data = {
    'dataSource': 'Cluster0',
    'database': 'curlDB',
    'collection': 'curlCollection',
    'document': {
        'name': 'John Sample',
        'age': 42,
    },
}

response = requests.post('https://ap-south-1.aws.data.mongodb-api.com/app/data-hhvjf/endpoint/data/v1/action/insertOne', headers=headers, json=json_data)
print(response)