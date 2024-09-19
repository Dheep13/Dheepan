from flask import Flask, jsonify
from OpenSSL import SSL

app = Flask(__name__)

@app.route('/')
def home():
    return jsonify({"message": "Welcome to your secure Informatica repository domain"})

@app.route('/status')
def status():
    return jsonify({"status": "OK"})

if __name__ == '__main__':
    context = SSL.Context(SSL.TLSv1_2_METHOD)
    context.use_privatekey_file('key.pem')
    context.use_certificate_file('cert.pem')
    
    app.run(host='0.0.0.0', port=5000, ssl_context=context, debug=True)