import gradio as gr
from langchain.embeddings.openai import OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain.chains import ConversationalRetrievalChain
from langchain_community.chat_models import ChatOpenAI
from langchain_community.document_loaders import PyPDFLoader
import os
import fitz
from PIL import Image

# Ensure the OpenAI API key is set in the environment
if 'OPENAI_API_KEY' not in os.environ:
    raise EnvironmentError("Did not find OPENAI_API_KEY, please add an environment variable `OPENAI_API_KEY`.")

# Global variables
COUNT, N = 0, 0
chat_history = []
chain = None

# Function to process the PDF file and create a conversation chain
def process_file(file):
    loader = PyPDFLoader(file.name)
    documents = loader.load()

    embeddings = OpenAIEmbeddings()
    pdfsearch = Chroma.from_documents(documents, embeddings)

    chain = ConversationalRetrievalChain.from_llm(ChatOpenAI(temperature=0.3),
                                                  retriever=pdfsearch.as_retriever(search_kwargs={"k": 1}),
                                                  return_source_documents=True)
    return chain

# Function to generate a response based on the chat history and query
def generate_response(history, query, btn):
    global COUNT, N, chat_history, chain

    if COUNT == 0 or chain is None:
        chain = process_file(btn)
        COUNT += 1

    result = chain({"question": query, 'chat_history': chat_history}, return_only_outputs=True)
    chat_history += [(query, result["answer"])]
    N = list(result['source_documents'][0])[1][1]['page']

    updated_history = history + [(query, result["answer"])]
    return updated_history, ''

# Function to render a specific page of a PDF file as an image
def render_file(file):
    global N
    doc = fitz.open(file.name)
    page = doc[N]
    pix = page.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
    image = Image.frombytes('RGB', [pix.width, pix.height], pix.samples)
    return image

# Gradio interface setup
with gr.Blocks() as demo:
    with gr.Column():
        with gr.Row():
            api_key = gr.Textbox(placeholder='Enter OpenAI API key', interactive=True, visible=False)
            change_api_key = gr.Button('Change Key')

        with gr.Row():
            # chatbot = gr.Chatbot(value=[], elem_id='chatbot').style(height=650)
            # show_img = gr.Image(label='Upload PDF', tool='select').style(height=680)
            chatbot = gr.Chatbot(value=[], elem_id='chatbot')
            # show_img = gr.Image(label='Upload PDF', tool='select')

            # For uploading a PDF file
            # btn = gr.FileUploader(label="Upload a PDF", file_types=[".pdf"])
            # # For displaying an image (e.g., a rendered page from the PDF)
            # show_img = gr.Image(label='Rendered PDF Page')
            btn = gr.UploadButton("Click to Upload a File", file_types=["pdf"], file_count="multiple")
            btn.upload(upload_file, upload_button, file_output)

        with gr.Column(scale=0.70):
            txt = gr.Textbox(show_label=False, placeholder="Enter text and press enter").style(container=False)
            submit_btn = gr.Button('Submit')
            btn = gr.FileUploader("📁 Upload a PDF", file_types=[".pdf"])

    submit_btn.click(fn=generate_response, inputs=[chatbot, txt, btn], outputs=[chatbot])

    btn.change(fn=render_file, inputs=[btn], outputs=[show_img])

if __name__ == "__main__":
    demo.launch()

# from langchain_community.document_loaders import WebBaseLoader
# from langchain.text_splitter import CharacterTextSplitter
# from langchain.vectorstores import Chroma
# from langchain.embeddings.openai import OpenAIEmbeddings
# from langchain.llms import OpenAI
# from langchain.chains import RetrievalQA

# # Load the Wikipedia page
# loader = WebBaseLoader("https://en.wikipedia.org/wiki/New_York_City")
# documents = loader.load()

# # Split the text into chunks
# text_splitter = CharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
# texts = text_splitter.split_documents(documents)

# # Create embeddings
# embeddings = OpenAIEmbeddings()

# # Create a vector store
# db = Chroma.from_documents(texts, embeddings, collection_name="wiki-nyc")

# # Create a retriever
# retriever = db.as_retriever()

# # Create a QA chain
# llm = OpenAI(temperature=0)
# qa = RetrievalQA.from_chain_type(llm=llm, chain_type="stuff", retriever=retriever)