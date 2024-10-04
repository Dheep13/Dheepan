# app.py
from flask import Flask, request, jsonify, render_template, send_from_directory
from rag_backend import RAGBackend
import os
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = None  # Remove file size limit

# Ensure upload folder exists
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Initialize the RAG backend
api_key = os.environ.get('OPENAI_API_KEY')
if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable is not set")

rag = RAGBackend(api_key)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    if file and file.filename.endswith('.pdf'):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        try:
            rag.process_pdf(filepath)
            return jsonify({"message": "File processed successfully", "filename": filename}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"error": "Invalid file type"}), 400

@app.route('/ask', methods=['POST'])
def ask_question():
    question = request.json.get('question')
    if not question:
        return jsonify({"error": "No question provided"}), 400
    try:
        answer = rag.ask_question(question)
        return jsonify({"answer": answer}), 200
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=True)