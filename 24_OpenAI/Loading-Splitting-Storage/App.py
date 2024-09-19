# Document loader
from langchain.document_loaders import WebBaseLoader
# loader = WebBaseLoader("https://lilianweng.github.io/posts/2023-06-23-agent/")
# loader = WebBaseLoader("https://github.com/Dheep13/Dheep13.github.io/blob/main/THR70.pdf")
loader = WebBaseLoader("https://www.theregister.com/2023/08/08/s4hana_sap_cloud/")
data = loader.load()

# Split
from langchain.text_splitter import RecursiveCharacterTextSplitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size = 500, chunk_overlap = 0)
all_splits = text_splitter.split_documents(data)

# Store 
from langchain.vectorstores import Chroma
from langchain.embeddings import OpenAIEmbeddings
vectorstore = Chroma.from_documents(documents=all_splits,embedding=OpenAIEmbeddings())#text embedding


question = "Summarize this in 100 to 125 words"
docs = vectorstore.similarity_search(question)
print(docs)
# print(len(docs))