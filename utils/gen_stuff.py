import json
from utils.ai_model import generate_response

def generate_stuff(content):
    json_schema = {
        "summary": "string - concise summary in markdown format like clear, concise notes",
        "flashcards": [
            {
                "front": "string - question or term",
                "back": "string - answer or definition"
            }
        ],
        "mcqs": [
            {
                "question": "string - multiple choice question",
                "options": {
                    "A": "string - option A",
                    "B": "string - option B", 
                    "C": "string - option C",
                    "D": "string - option D"
                },
                "correct_answer": "string - A, B, C, or D"
            }
        ]
    }
    
    # Create the comprehensive prompt
    prompt = f"""
Please analyze the following content and create a comprehensive learning resource in JSON format.

CONTENT TO ANALYZE:
{content}

INSTRUCTIONS:
1. Create a concise summary in markdown format like clear, structured notes covering the main concepts and key points
2. Generate up to 10 flashcards covering important terms, concepts, or facts
3. Create up to 10 multiple choice questions (MCQs) to test understanding
4. Return ONLY valid JSON in the exact format specified below - no additional text or explanations

REQUIRED JSON FORMAT:
{json.dumps(json_schema, indent=2)}

IMPORTANT RULES:
- Return ONLY the JSON object, nothing else
- Ensure all strings are properly escaped
- Use exactly the field names shown above
- For the summary: Use markdown formatting with headers (##, ###), bullet points (-), bold (**text**), and italics (*text*) to create clear, organized notes
- Generate 1-10 flashcards and 1-10 MCQs based on content richness
- Make flashcards and MCQs educational and relevant to the content
- For MCQs, ensure one option is clearly correct and others are plausible distractors
"""
    print("Trying to generate structured learning resource...")
    # Try up to 3 times to get valid JSON response
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            # Get response from Gemini
            response = generate_response(prompt)
            # tackle ```json ... ``` wrapping if there
            if response.startswith("```json"):
                response = response[7:-3]

            # Try to parse the response as JSON
            parsed_json = json.loads(response.strip())
            
            # Validate the JSON structure
            if validate_response_structure(parsed_json):
                return parsed_json
            else:
                print(f"Attempt {attempt + 1}: Invalid JSON structure. Retrying...")
                if attempt == max_attempts - 1:
                    return {"error": "Failed to generate valid JSON after 3 attempts", "raw_response": response}
                
        except json.JSONDecodeError as e:
            print(f"Attempt {attempt + 1}: JSON parsing failed - {e}. Retrying...")
            if attempt == max_attempts - 1:
                return {"error": f"JSON parsing failed after 3 attempts: {str(e)}", "raw_response": response}
        except Exception as e:
            print(f"Attempt {attempt + 1}: Unexpected error - {e}. Retrying...")
            if attempt == max_attempts - 1:
                return {"error": f"Unexpected error after 3 attempts: {str(e)}", "raw_response": response}
    
    return {"error": "Failed to generate response after 3 attempts"}

def validate_response_structure(data):
    """
    Validate that the JSON response has the correct structure
    """
    try:
        # Check if it's a dictionary
        if not isinstance(data, dict):
            return False
            
        # Check required top-level keys
        required_keys = ["summary", "flashcards", "mcqs"]
        if not all(key in data for key in required_keys):
            return False
            
        # Validate summary
        if not isinstance(data["summary"], str) or not data["summary"].strip():
            return False
            
        # Validate flashcards
        if not isinstance(data["flashcards"], list):
            return False
        for flashcard in data["flashcards"]:
            if not isinstance(flashcard, dict):
                return False
            if not all(key in flashcard for key in ["front", "back"]):
                return False
            if not isinstance(flashcard["front"], str) or not isinstance(flashcard["back"], str):
                return False
                
        # Validate MCQs
        if not isinstance(data["mcqs"], list):
            return False
        for mcq in data["mcqs"]:
            if not isinstance(mcq, dict):
                return False
            if not all(key in mcq for key in ["question", "options", "correct_answer"]):
                return False
            if not isinstance(mcq["question"], str):
                return False
            if not isinstance(mcq["options"], dict):
                return False
            required_options = ["A", "B", "C", "D"]
            if not all(opt in mcq["options"] for opt in required_options):
                return False
            if mcq["correct_answer"] not in required_options:
                return False
                
        return True
        
    except Exception as e:
        print(f"Validation error: {e}")
        return False