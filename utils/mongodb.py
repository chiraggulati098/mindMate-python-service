import os
import time
from pymongo import MongoClient
from bson import ObjectId
from typing import Optional, Dict, Any
import dotenv

dotenv.load_dotenv()

class MongoDBConnection:
    """MongoDB connection and operations handler"""
    
    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI")
        if not self.mongo_uri:
            raise ValueError("MONGO_URI not found in environment variables")
        
        self.client = None
        self.database = None
        self.collection = None
        self._connect()
    
    def _connect(self):
        """Establish connection to MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            # Test connection
            self.client.admin.command('ismaster')
            
            # Get database and collection
            self.database = self.client.mindmate
            self.collection = self.database.documentmodels
            
            print("✓ MongoDB connection successful")
        except Exception as e:
            print(f"✗ MongoDB connection failed: {e}")
            raise
    
    def get_document_content(self, document_id: str, user_id: str) -> Optional[str]:
        """
        Fetch document content by documentId and userId
        
        Args:
            document_id (str): The document ID to search for
            user_id (str): The user ID to verify ownership
            
        Returns:
            Optional[str]: The document content if found, None otherwise
        """
        try:
            # Convert string IDs to ObjectId
            try:
                doc_object_id = ObjectId(document_id)
                user_object_id = ObjectId(user_id)
            except Exception as e:
                print(f"Invalid ObjectId format: {e}")
                return None
            
            # Query for document with matching _id and userId
            query = {
                "_id": doc_object_id,
                "userId": user_object_id
            }
            
            document = self.collection.find_one(query)
            
            if document:
                # Extract content field - adjust field name as needed
                content = document.get('content', '')
                if not content:
                    # Try alternative field names
                    content = document.get('text', document.get('body', ''))
                
                print(f"✓ Found document {document_id} for user {user_id}")
                return content
            else:
                print(f"✗ Document {document_id} not found for user {user_id}")
                return None
                
        except Exception as e:
            print(f"Error fetching document: {e}")
            return None
    
    def get_document_file_url(self, document_id: str, user_id: str) -> Optional[str]:
        """
        Fetch document fileUrl by documentId and userId
        
        Args:
            document_id (str): The document ID to search for
            user_id (str): The user ID to verify ownership
            
        Returns:
            Optional[str]: The document fileUrl if found, None otherwise
        """
        try:
            # Convert string IDs to ObjectId
            try:
                doc_object_id = ObjectId(document_id)
                user_object_id = ObjectId(user_id)
            except Exception as e:
                print(f"Invalid ObjectId format: {e}")
                return None
            
            # Query for document with matching _id and userId
            query = {
                "_id": doc_object_id,
                "userId": user_object_id
            }
            
            document = self.collection.find_one(query)
            
            if document:
                # Extract fileUrl field - try multiple possible field names
                file_url = document.get('fileUrl', document.get('file_url', document.get('url', document.get('pdfUrl', ''))))
                
                if file_url:
                    print(f"✓ Found fileUrl for document {document_id}: {file_url}")
                    return file_url
                else:
                    print(f"✗ No fileUrl found for document {document_id}")
                    return None
            else:
                print(f"✗ Document {document_id} not found for user {user_id}")
                return None
                
        except Exception as e:
            print(f"Error fetching document fileUrl: {e}")
            return None
    
    def update_document_generated_content(self, document_id: str, user_id: str, generated_data: Dict[str, Any]) -> bool:
        """
        Update document with generated content (summary, flashcards, MCQs)
        
        Args:
            document_id (str): The document ID to update
            user_id (str): The user ID to verify ownership
            generated_data (Dict): The generated content containing summary, flashcards, mcqs
            
        Returns:
            bool: True if update successful, False otherwise
        """
        try:
            # Convert string IDs to ObjectId
            try:
                doc_object_id = ObjectId(document_id)
                user_object_id = ObjectId(user_id)
            except Exception as e:
                print(f"Invalid ObjectId format: {e}")
                return False
            
            # Query for document with matching _id and userId
            query = {
                "_id": doc_object_id,
                "userId": user_object_id
            }
            
            # Check if generated_data has error
            if "error" in generated_data:
                # Update with failed status
                update = {
                    "$set": {
                        "summary_status": "FAILED",
                        "flashcard_status": "FAILED", 
                        "mcq_status": "FAILED",
                        "processing_error": generated_data.get("error", "Unknown error"),
                        "updated_at": time.time()
                    }
                }
            else:
                # Update with generated content and completed status
                update = {
                    "$set": {
                        "summary": generated_data.get("summary", ""),
                        "flashcards": generated_data.get("flashcards", []),
                        "mcqs": generated_data.get("mcqs", []),
                        "summary_status": "COMPLETED",
                        "flashcard_status": "COMPLETED",
                        "mcq_status": "COMPLETED", 
                        "updated_at": time.time()
                    }
                }
            
            result = self.collection.update_one(query, update)
            
            if result.matched_count > 0:
                print(f"✓ Updated document {document_id} for user {user_id}")
                return True
            else:
                print(f"✗ No document found to update: {document_id} for user {user_id}")
                return False
                
        except Exception as e:
            print(f"Error updating document: {e}")
            return False
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            print("MongoDB connection closed")

# Global connection instance
_mongo_connection = None

def get_mongo_connection() -> MongoDBConnection:
    """Get or create MongoDB connection singleton"""
    global _mongo_connection
    if _mongo_connection is None:
        _mongo_connection = MongoDBConnection()
    return _mongo_connection

def fetch_document_content(document_id: str, user_id: str) -> Optional[str]:
    """
    Convenience function to fetch document content
    
    Args:
        document_id (str): The document ID
        user_id (str): The user ID
        
    Returns:
        Optional[str]: The document content if found
    """
    connection = get_mongo_connection()
    return connection.get_document_content(document_id, user_id)

def update_document_content(document_id: str, user_id: str, generated_data: Dict[str, Any]) -> bool:
    """
    Convenience function to update document with generated content
    
    Args:
        document_id (str): The document ID
        user_id (str): The user ID  
        generated_data (Dict): The generated content
        
    Returns:
        bool: True if update successful
    """
    connection = get_mongo_connection()
    return connection.update_document_generated_content(document_id, user_id, generated_data)

def fetch_document_file_url(document_id: str, user_id: str) -> Optional[str]:
    """
    Convenience function to fetch document fileUrl
    
    Args:
        document_id (str): The document ID
        user_id (str): The user ID
        
    Returns:
        Optional[str]: The document fileUrl if found
    """
    connection = get_mongo_connection()
    return connection.get_document_file_url(document_id, user_id)