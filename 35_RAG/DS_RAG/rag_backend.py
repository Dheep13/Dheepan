import PyPDF2
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import FAISS
from langchain.chains import RetrievalQA
from langchain.chat_models import ChatOpenAI

class RAGBackend:
    def __init__(self, api_key):
        self.api_key = api_key
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        self.vector_store = None
        self.qa_chain = None

    def process_pdf(self, pdf_path):
        text = self._extract_text_from_pdf(pdf_path)
        chunks = self._split_text(text)
        self.vector_store = self._create_vector_store(chunks)
        self._setup_qa_chain()

    def _extract_text_from_pdf(self, pdf_path):
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)
            text = ""
            for page in reader.pages:
                text += page.extract_text()
        return text

    def _split_text(self, text):
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len
        )
        return text_splitter.split_text(text)

    def _create_vector_store(self, chunks):
        embeddings = OpenAIEmbeddings(openai_api_key=self.api_key)
        return FAISS.from_texts(chunks, embeddings)

    def _setup_qa_chain(self):
        llm = ChatOpenAI(temperature=0, model_name="gpt-3.5-turbo", openai_api_key=self.api_key)
        self.qa_chain = RetrievalQA.from_chain_type(
            llm=llm,
            chain_type="stuff",
            retriever=self.vector_store.as_retriever()
        )

    def ask_question(self, question):
        if not self.qa_chain:
            raise ValueError("PDF has not been processed yet")
        return self.qa_chain.run(question)