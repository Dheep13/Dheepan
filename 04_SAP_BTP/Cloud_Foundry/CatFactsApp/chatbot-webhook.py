from flask import Flask, request, jsonify
import os
import json
import requests

app = Flask(__name__)
cf_port = os.getenv("PORT")

# Only get method by default
@app.route('/')
def hello():
    return '<h1>SAP Conversational AI</h1><body>The animal facts webhook for use in SAP Conversational AI chatbots.<br><img src="static/283370-pictogram-purple.svg" width=260px></body>'

@app.route('/bot', methods=['POST'])
def bot():
  # Get the request body, and determine the dog and memory
  try:
    bot_data = json.loads(request.get_data())
    animal = bot_data['conversation']['memory']['animal']['raw']
    memory = bot_data['conversation']['memory']
  except:
    animal = "dog"
    memory = json.loads("{}")

  # Get the fun fact
  url = "https://cat-fact.herokuapp.com/facts/random?animal_type=" + animal + "&amount=1"
  nodata = {"text" : "No data"}

  # In case the API does not work after 8 seconds, we return "no data"
  try:
    r = requests.get(url, timeout=8)
    fact_data = json.loads(r.content)
  except:
    fact_data = nodata

  # Increment the # of times this has been called
  if 'funfacts' in memory:
     memory['funfacts'] += 1
  else:
     memory['funfacts'] = 1

  # Return message to display (replies) and update memory
  return jsonify(
    status=200,
    replies=[
    {
      'type': 'text',
      'content': fact_data['text']
    }
    ],
    conversation={
      'memory': memory
    }

  )

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)