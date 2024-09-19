from langchain.agents import create_sql_agent
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.sql_database import SQLDatabase
from langchain.llms.openai import OpenAI
from langchain.agents import AgentExecutor
from langchain.agents.agent_types import AgentType
from langchain.chat_models import ChatOpenAI

path = "sqlite:///C:/Users/I520292/OneDrive - SAP SE/Visual Studio Code/24_OpenAI/SQLDatabase/chinook/chinook.db"
db = SQLDatabase.from_uri(path)
toolkit = SQLDatabaseToolkit(db=db, llm=OpenAI(temperature=0))

agent_executor = create_sql_agent(
    llm=ChatOpenAI(temperature=0, model="gpt-3.5-turbo-0613"),
    toolkit=toolkit,
    verbose=True,
    agent_type=AgentType.OPENAI_FUNCTIONS
)


query = input("Enter your query: ")
# print(agent_executor.run("List all tables that are available in the database and also list first 100 records from the Sales Table"))
print(agent_executor.run(query))
