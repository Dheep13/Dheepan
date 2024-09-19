from flask import Flask, render_template, jsonify
import requests
import os
import json
app = Flask(__name__)
cf_port = os.getenv("PORT")

@app.route('/')
def index():
    url = "https://0509.callidusondemand.com/"
    Username='Deepan'
    Password='Msd183$$'
    # Make the API call
    position='User113'
    # base_url= url+"api/v2/positions?$filter=name eq "+position+" &select=name,title&expand=title"
    base_url='https://0509.callidusondemand.com/api/v2/positions?$filter=(name eq User113)&select=name,title&expand=title&top=1'
    response = requests.get(base_url,auth=(Username,Password))
    data = response.json()
    # for x in data['positions']:
    #     print(x)
    print(data)
    # Render the template with the data
    return render_template('index.html', data=data)


    # return jsonify(
    #     status=200,
    #     replies=[{
    #     'type': 'text',
    #     'content': 'The title name is %s' % (data)
    #     }]
    #     ) 

if __name__ == '__main__':
	if cf_port is None:
		app.run(host='0.0.0.0', port=5000, debug=True)
	else:
		app.run(host='0.0.0.0', port=int(cf_port), debug=True)

# if __name__ == '__main__':
#     app.run(debug=True)

        
        # r = requests.get(base_url , auth=(Username,Password))
        # print(r.text)
        # pos_data = json.loads(r.text)
        # for x in pos_data['positions']:
        #     y = (x['title'])
        #     TitleName=y['displayName']
        #     print(y['displayName'])