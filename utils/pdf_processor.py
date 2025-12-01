import os
import pdfplumber
from typing import List

def load_pdf(file_path):
    '''
    Reads text from PDF document
    '''
    text = ""
    try:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text += page.extract_text() or ""
    except Exception as e:
        print(f"Error loading pdf {file_path}: {e}")
    return text

def process_pdf(file_path):
    '''
    Extracts text from a PDF and splits it into chunks.
    '''
    text = load_pdf(file_path)
    return text