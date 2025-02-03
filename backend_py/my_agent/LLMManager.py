from langchain_core.prompts import ChatPromptTemplate
from langchain_ollama import ChatOllama

class LLMManager:
    def __init__(self):
        # self.llm = ChatOllama(model="qwen2.5:3b-instruct", temperature=0, base_url="http://host.docker.internal:11434")
        self.llm_json_mode = ChatOllama(model="deepseek-r1:1.5b", temperature=0, format="json")
        self.llm = ChatOllama(model="deepseek-r1:1.5b", temperature=0)

    def invoke_json(self, prompt: ChatPromptTemplate, **kwargs) -> str:
        messages = prompt.format_messages(**kwargs)
        response = self.llm_json_mode.invoke(messages)
        return response.content

    def invoke(self, prompt: ChatPromptTemplate, **kwargs) -> str:
        messages = prompt.format_messages(**kwargs)
        response = self.llm.invoke(messages)
        return response.content