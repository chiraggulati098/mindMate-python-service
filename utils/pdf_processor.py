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

def split_text(text):
    '''
    Splits text into chunks:
    1. stores code in same chunk (no matter the length)
    2. stores text till 1000 chars in 1 chunk, splitting at full stop when exceeded
    '''
    chunks = []
    current_chunk = ""
    in_code_block = False

    for line in text.split('\n'):
        if line.strip().startswith('```'):
            in_code_block = not in_code_block
            current_chunk += line + '\n'
        elif in_code_block:
            current_chunk += line + '\n'
        else:
            if len(current_chunk) + len(line) > 1000:
                last_period = current_chunk.rfind('.')
                if last_period != -1:
                    chunks.append(current_chunk[:last_period + 1].strip())
                    current_chunk = current_chunk[last_period + 1:] + line + '\n'
                else:
                    chunks.append(current_chunk.strip())
                    current_chunk = line + '\n'
            else:
                current_chunk += line + '\n'
    
    if current_chunk:
        chunks.append(current_chunk.strip())
    
    return chunks

def process_pdf(file_path):
    '''
    Extracts text from a PDF and splits it into chunks.
    '''
    text = load_pdf(file_path)
    return split_text(text)