


# Document loader
from langchain.document_loaders import WebBaseLoader
# loader = WebBaseLoader("https://lilianweng.github.io/posts/2023-06-23-agent/")
loader = WebBaseLoader("https://help.sap.com/docs/SAP_Commissions/0e4b0e05f53e4f87a21c5ccfca72fea6/726bf3607c231014a804993ce4041860.html?locale=en-US")
data = loader.load()

# Split
from langchain.text_splitter import RecursiveCharacterTextSplitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size = 500, chunk_overlap = 0)
all_splits = text_splitter.split_documents(data)

# Store 
from langchain.vectorstores import Chroma
from langchain.embeddings import OpenAIEmbeddings
vectorstore = Chroma.from_documents(documents=all_splits,embedding=OpenAIEmbeddings())


# question = "What are the approaches to Task Decomposition?"
question = "What is CDL?"
docs = vectorstore.similarity_search(question)

print(docs)
from langchain.chat_models import ChatOpenAI
llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0)
from langchain.chains import RetrievalQA
qa_chain = RetrievalQA.from_chain_type(llm,retriever=vectorstore.as_retriever())
print(qa_chain({"query": question}))