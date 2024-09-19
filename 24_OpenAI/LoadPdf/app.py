from langchain.document_loaders import UnstructuredFileLoader
from langchain.text_splitter import CharacterTextSplitter
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
from langchain.chains.question_answering import load_qa_chain
import os
import openai
from langchain import OpenAI, VectorDBQA
# import nltk
from PyPDF2 import PdfReader
from typing_extensions import Concatenate

# pdfreader = PdfReader("CDL.pdf")
# raw_text=''
# for i, page in enumerate(pdfreader.pages):
#     content=page.extract_text()
#     if content:
#         raw_text += content

# #split text to chunks
# text_splitter=CharacterTextSplitter( separator="\n",chunk_size=1200,chunk_overlap=500,length_function= len)
# texts=text_splitter.split_text(raw_text)

# # embeddings-creates vectors for each split and stores in vector store
# embeddings = OpenAIEmbeddings()
# document_search= FAISS.from_texts(texts,embeddings)

#qachain
chain = load_qa_chain(OpenAI(),chain_type="stuff")
query ="what is the filetype code for CS_STAGEPOSITION table?"
docs= document_search.similarity_search(query)
print(chain.run(input_documents=docs, question=query))