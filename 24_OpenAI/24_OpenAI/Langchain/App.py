from langchain.document_loaders import WebBaseLoader
from langchain.indexes import VectorstoreIndexCreator
# Document loader
loader = WebBaseLoader("https://lilianweng.github.io/posts/2023-06-23-agent/")
# Index that wraps above steps
index = VectorstoreIndexCreator().from_loaders([loader])
# Question-answering
question = "What is Task Decomposition?"
print(index.query(question))