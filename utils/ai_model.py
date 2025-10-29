import google.generativeai as genai
import os
from dotenv import load_dotenv
import time

# load API keys from .env
load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

GEMINI_MODEL = "gemini-1.5-flash"

def generate_response(prompt):
    '''
    Send user query to Gemini API and return response
    '''
    max_retries = 3
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel(GEMINI_MODEL)
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(1)  
                continue
            else:
                return f"Error after {max_retries} attempts: {str(e)}"

def summarize_query(query):
    '''
    Summarize query using Gemini for RAG search
    '''
    max_retries = 3
    for attempt in range(max_retries):
        try:
            model = genai.GenerativeModel(GEMINI_MODEL)
            prompt = f"Summarize the following query for a retrieval search: {query}"
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                time.sleep(1)  
                continue
            else:
                return f"Error after {max_retries} attempts: {str(e)}"