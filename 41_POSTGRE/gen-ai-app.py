from langchain_community.document_loaders import PyPDFLoader
from langchain.vectorstores.pgvector import PGVector
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client
from gen_ai_hub.proxy.langchain.init_models import init_embedding_model

proxy_client = get_proxy_client('gen-ai-hub')

# Load PDF
loaders = [
    PyPDFLoader("GenAIHub.pdf")]
docs = []
for loader in loaders:
    docs.extend(loader.load())

embeddings = init_embedding_model('text-embedding-ada-002')

response = embeddings.embed_query("Just a test for embeddings..")
print(response)
