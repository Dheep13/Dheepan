import pandas as pd
from bs4 import BeautifulSoup
import chromadb
from chromadb.utils import embedding_functions
from transformers import pipeline, PretrainedConfig,AutoTokenizer, AutoModelForCausalLM
from IPython.display import display, Markdown
import torch
from huggingface_hub import login
from kaggle_secrets import UserSecretsClient
import os





# loading data
datafile = pd.read_csv("kaggle_winning_solutions_methods.csv")
datafile.info()

datafile.head()

# Data cleaning
datafile = datafile[['place','competition_name','metric','year','writeup','methods']] # omit not required columns
datafile.drop_duplicates(inplace=True) # remove duplicate rows
datafile.info() 


datafile['writeup'] = datafile['writeup'].apply(lambda x: BeautifulSoup(x,'html.parser').get_text())
datafile.reset_index()
datafile.head()


# File structure for indexing in vector database
total_entries = len(datafile) # total number of data points

# Document for writeup
documents = [f"{datafile.iloc[i]['place']} place solution :  \n competition name : {datafile.iloc[i]['competition_name']} \n metric : {datafile.iloc[i]['metric']} \n year : {datafile.iloc[i]['year']} \n writeup : {datafile.iloc[i]['writeup']} \n methods : {datafile.iloc[i]['methods']}" for i in range(total_entries)]

# Metadata to save for each document
metadatas =  [
            {
            'place':str(datafile['place'].iloc[j]),
            'competition_name':str(datafile['competition_name'].iloc[j]),
            'year':str(datafile['year'].iloc[j])
            } for j in range(total_entries)
            ]


# Configure vector database
client = chromadb.Client() # initiate client
ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name = 'all-MiniLM-L6-v2') # Embedding function for documents

# Create a vector database
collection = client.create_collection(
    name = 'kaggle_competition_solutions',
    embedding_function = ef ,
    metadata={"hnsw:space": "l2"}
)


# Ingest documnets into vector database
collection.add(
    documents = documents,
    ids = [str(i) for i in range(total_entries)],
    metadatas = metadatas
)



# Setup the environment
access_token_read = os.getenv('HUGGINGFACE_TOKEN')
login(token = access_token_read)

# Load the model
tokenizer = AutoTokenizer.from_pretrained("google/gemma-2b-it")
model = AutoModelForCausalLM.from_pretrained("google/gemma-2b-it",device_map="cuda")


# def generate_text(prompt):
#     input_ids = tokenizer.encode(prompt,return_tensors="pt").to("cuda")  # Move input to GPU
#     output = model.generate(input_ids, max_length=4056)  # Adjust generation parameters as needed
#     generated_text = tokenizer.decode(output[0], skip_special_tokens=True,clean_up_tokenization_spaces=True)
#     return generated_text

# def query_database(user_prompt):
#     output = collection.query(
#     query_texts=user_prompt,
#     n_results=1
#     )
#     return output['documents'][0][0]


# user_prompt = 'Explain the 12 place solution of Signal Search competition'

# writeup = query_database(user_prompt) # vector database output

# prompt_template = [
#     { "role": "user", 
#      "content": f"You are teacher, explain the given below content based on the instructions. Generate a response in simple to understand words ### instruction {user_prompt}  ### content {writeup}" 
#     },
# ]

# # User prompt and writeup in prompt template format
# prompt = tokenizer.apply_chat_template(prompt_template, tokenize=False, add_generation_prompt=True)

# # Generate respose
# outputs = generate_text(prompt)
# display(Markdown(outputs[len(prompt)-2:]))



# # Model Configuration
# config = PretrainedConfig(
#     do_sample=True,
#     temperature=0.1,
#     top_k=30,
#     top_p=0.7,    
#     torch_dtype=torch.bfloat16, 
#     )

# # Pipeline
# pipe = pipeline(
#     task = 'text-generation',
#     model = model,
#     tokenizer = tokenizer,
#     max_new_tokens = 4056,
#     device_map="auto",
#     config = config
# )



# def Gemma_generate(user_prompt):
#     writeup = query_database(user_prompt)

#     prompt = [
#         { "role": "user", 
#          "content": f"You are teacher, explain the given below content based on the instructions. Generate a response in simple to understand words ### instruction {user_prompt}  ### content {writeup}" 
#         },
#     ]

#     output = pipe(
#         prompt
#         )
#     return output[0]['generated_text'][1]['content']


# # Your prompt goes here 👇
# user_prompt = 'Explain the 12 place solution of Signal Search competition'  
# response = Gemma_generate(user_prompt)
# display(Markdown(response))

# user_prompt = 'Teach me the methods used in Bristol-Myers Squibb competition'  
# response = Gemma_generate(user_prompt)
# display(Markdown(response))


# user_prompt =  '3rd place solution in Detecting Continuous Gravitational Waves'  
# response = Gemma_generate(user_prompt)
# display(Markdown(response))


# user_prompt = 'What preprocessing methods where used in Google Sign Language Recognition competition?'  
# response = Gemma_generate(user_prompt)
# display(Markdown(response))