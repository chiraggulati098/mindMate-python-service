import os
import time
import uuid
import hashlib
from dotenv import load_dotenv
import google.generativeai as genai
from typing import List, Optional, Dict, Any
from qdrant_client import QdrantClient, models

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)

GEMINI_MODEL = "gemini-2.0-flash"

class VectorStore:
    def __init__(self, collection_name: str = "notes_db", host: str = "localhost", port: int = 6333):
        """
        Initialize Qdrant client and setup collection with payload indexes
        """
        self.client = QdrantClient(host=host, port=port)
        self.collection_name = collection_name
        self.embedding_model = "models/text-embedding-004"
        self.vector_size = 768                              # Gemini text-embedding-004 output dimension

        if not self.client.collection_exists(self.collection_name):
            print(f"Creating collection: {self.collection_name}")
            self.client.create_collection(
                collection_name=self.collection_name,
                vectors_config=models.VectorParams(
                    size=self.vector_size,
                    distance=models.Distance.COSINE
                )
            )
            
            # Indexing 'user_id', 'subject_id' and 'document_id' allows O(1) filtering access
            self._create_payload_index("user_id")
            self._create_payload_index("subject_id")
            self._create_payload_index("document_id")
            print("Payload indexes initialized.")

    def _create_payload_index(self, field_name: str):
        """
        Helper to create a keyword index for a payload field
        """
        self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name=field_name,
            field_schema=models.PayloadSchemaType.KEYWORD
        )

    def chunk_text(self, text: str, max_len: int = 1000) -> List[str]:
        """
        Splits long text into semantic chunks (~1000 characters), breaking only at sentence boundaries ('.')
        """
        sentences = [s.strip() for s in text.split('.') if s.strip()]
        chunks = []
        current = ""

        for s in sentences:
            sentence = s + ". "
            if len(current) + len(sentence) > max_len:
                chunks.append(current.strip())
                current = sentence
            else:
                current += sentence

        if current.strip():
            chunks.append(current.strip())

        return chunks

    def generate_embeddings(self, text: str) -> Optional[List[float]]:
        """
        Generate embedding using Gemini with retry logic
        """
        if not text or not text.strip():
            print('Skipping empty chunk.')
            return None
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = genai.embed_content(model=self.embedding_model, content=text)
                return response['embedding']
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(1)
                else:
                    print(f"Error generating embedding after {max_retries} attempts: {e}")
                    return None

    def _generate_chunk_id(self, document_id: str, index: int, text: str) -> str:
        """
        Creates a deterministic valid UUID for the chunk based on doc_id and content
        """
        text_hash = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]
        unique_string = f"{document_id}_chunk_{index}_{text_hash}"
        
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))

    def add_document(self, text: str, user_id: str, subject_id: str, document_id: str = None) -> str:
        """
        Ingests a document by splitting it into chunks and embedding each chunk separately
        """
        chunks = self.chunk_text(text, max_len=1000)

        for idx, chunk in enumerate(chunks):
            vector = self.generate_embeddings(chunk)
            if not vector:
                print(f"Skipping failed embedding for chunk {idx}")
                continue

            chunk_id = self._generate_chunk_id(document_id, idx, chunk)

            payload = {
                "page_content": chunk,
                "user_id": user_id,
                "subject_id": subject_id,
                "document_id": document_id,
                "chunk_index": idx
            }

            self.client.upsert(
                collection_name=self.collection_name,
                points=[
                    models.PointStruct(
                        id=chunk_id,
                        vector=vector,
                        payload=payload
                    )
                ]
            )
            print(f"Stored chunk {chunk_id} (index {idx}) for User: {user_id}")

    def query(self, query_text: str, user_id: str, subject_id: str, top_k: int = 10) -> List[Dict[str, Any]]:
        """
        Performs a semantic search constrained by user_id and subject_id
        """
        query_vector = self.generate_embeddings(query_text)
        if query_vector is None:
            print("Query embedding failed")
            return []

        # Define STRICT filters
        search_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="user_id",
                    match=models.MatchValue(value=user_id)
                ),
                models.FieldCondition(
                    key="subject_id",
                    match=models.MatchValue(value=subject_id)
                )
            ]
        )

        # Perform Search
        response = self.client.query_points(
            collection_name=self.collection_name,
            query=query_vector,
            limit=top_k,
            query_filter=search_filter
        )

        results = response.points
        return [{'text': hit.payload['page_content'], 'document_id': hit.payload['document_id']} for hit in results]

    def remove_document(self, document_id: str):
        self.client.delete(
            collection_name=self.collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="document_id",
                            match=models.MatchValue(value=document_id)
                        )
                    ]
                )
            )
        )
        print(f"Removed all chunks for document_id: {document_id}")

if __name__ == "__main__":
    # Ensure Docker is running: docker run -p 6333:6333 qdrant/qdrant
    
    store = VectorStore()

    # 1. Add Data
    # Note: For real apps, you'd split long text into multiple chunks loop here
    print("--- Adding Documents ---")
    store.add_document(
        text="The mitochondrion is the powerhouse of the cell.", 
        user_id="user_123", 
        subject_id="biology",
        document_id="doc_bio_001"
    )
    
    store.add_document(
        text="Newton's second law states F=ma.", 
        user_id="user_123", 
        subject_id="physics",
        document_id="doc_phys_001"
    )

    # 2. Query (Biology)
    print("\n--- Querying Biology ---")
    results = store.query("What generates energy in a cell?", user_id="user_123", subject_id="biology")
    for res in results:
        print(f"Found: {res}")

    # 3. Query (Physics - should return nothing because we filter by 'biology')
    print("\n--- Querying Physics (with Biology filter) ---")
    results_wrong = store.query("F=ma", user_id="user_123", subject_id="physicss")
    print(f"Results (Should be empty): {results_wrong}")

    # 4. Remove Document
    print("\n--- Removing Document ---")
    store.remove_document("doc_bio_001")
    store.remove_document("doc_phys_001")