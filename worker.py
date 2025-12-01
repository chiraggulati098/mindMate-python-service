import os
import dotenv
import asyncio
import redis
import json
import time
import random
import uuid
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any

from utils.gen_stuff import generate_stuff
from utils.mongodb import fetch_document_content, update_document_content, fetch_document_file_url
from utils.pdf_processor import process_pdf
from utils.s3_client import download_file_from_url, download_file_from_s3_key, cleanup_temp_file, test_s3_bucket_access
from utils.youtube_lib import get_transcript
from utils.web_scrape import scrape_website

dotenv.load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Worker configuration
WORKER_CONFIG = {
    'max_workers': 4,  # Number of concurrent threads
    'poll_interval': 1,  # Seconds to wait between queue checks
    'task_timeout': 300,  # Maximum time for task processing
    'retry_failed_tasks': True,
}

# Initialize Redis connection
redis_client = redis.from_url(REDIS_URL)
processed_tasks = set()  # Track processed tasks to avoid duplicates
processing_lock = threading.Lock()

def process_pdf_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process PDF task
    """
    task_id = task_data.get('id', f"{uuid.uuid1()}_{hashlib.md5(json.dumps(task_data, sort_keys=True).encode()).hexdigest()[:8]}")
    thread_id = threading.current_thread().ident
    
    print(f"[Thread {thread_id}] Starting text processing task {task_id}")
    print(f"[Thread {thread_id}] Task data: {task_data}")
    
    # Extract documentId and userId from task_data
    document_id = task_data.get('documentId')
    user_id = task_data.get('userId')
    
    if not document_id or not user_id:
        print(f"[Thread {thread_id}] Missing documentId or userId in task data")
        return {
            'task_id': task_id,
            'error': 'Missing documentId or userId',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Fetch fileUrl from MongoDB
    file_url = fetch_document_file_url(document_id, user_id)
    if not file_url:
        print(f"[Thread {thread_id}] Could not fetch fileUrl for document {document_id}")
        return {
            'task_id': task_id,
            'error': 'File URL not found in document',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }

    # Get PDF file from S3/R2
    temp_pdf_path = download_file_from_url(file_url)
    if not temp_pdf_path:
        print(f"[Thread {thread_id}] Could not download PDF from {file_url}")
        print(f"[Thread {thread_id}] Running comprehensive S3 bucket test for debugging...")
        test_s3_bucket_access()
        return {
            'task_id': task_id,
            'error': 'Failed to download PDF file',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }

    try:
        # Process PDF
        content = ''.join(process_pdf(temp_pdf_path))
        print("=============")
        print(content)
    finally:
        # Always cleanup the temporary file
        cleanup_temp_file(temp_pdf_path)

    if not content:
        print(f"[Thread {thread_id}] Could not fetch content for document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Document not found or empty content',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Track processing time
    start_time = time.time()
    result_data = generate_stuff(content)
    processing_time = time.time() - start_time

    # Parse result data and save to MongoDB
    update_success = update_document_content(document_id, user_id, result_data)
    
    if not update_success:
        print(f"[Thread {thread_id}] Failed to update document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Failed to update document in MongoDB',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    result = {
        'task_id': task_id,
        'input': task_data,
        'processing_time': processing_time,
        'status': 'completed',
        'thread_id': thread_id,
        'processed_at': time.time()
    }
    
    print(f"[Thread {thread_id}] Completed text task {task_id} after {processing_time:.2f}s")
    return result

def process_text_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process text task 
    """
    task_id = task_data.get('id', f"{uuid.uuid1()}_{hashlib.md5(json.dumps(task_data, sort_keys=True).encode()).hexdigest()[:8]}")
    thread_id = threading.current_thread().ident
    
    print(f"[Thread {thread_id}] Starting text processing task {task_id}")
    print(f"[Thread {thread_id}] Task data: {task_data}")
    
    # Extract documentId and userId from task_data
    document_id = task_data.get('documentId')
    user_id = task_data.get('userId')
    
    if not document_id or not user_id:
        print(f"[Thread {thread_id}] Missing documentId or userId in task data")
        return {
            'task_id': task_id,
            'error': 'Missing documentId or userId',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Fetch document content from MongoDB
    content = fetch_document_content(document_id, user_id)
    if not content:
        print(f"[Thread {thread_id}] Could not fetch content for document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Document not found or empty content',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Track processing time
    start_time = time.time()
    result_data = generate_stuff(content)
    processing_time = time.time() - start_time

    # Parse result data and save to MongoDB
    update_success = update_document_content(document_id, user_id, result_data)
    
    if not update_success:
        print(f"[Thread {thread_id}] Failed to update document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Failed to update document in MongoDB',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    result = {
        'task_id': task_id,
        'input': task_data,
        'processing_time': processing_time,
        'status': 'completed',
        'thread_id': thread_id,
        'processed_at': time.time()
    }
    
    print(f"[Thread {thread_id}] Completed text task {task_id} after {processing_time:.2f}s")
    return result

def process_youtube_video(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process PDF task
    """

    task_id = task_data.get('id', f"{uuid.uuid1()}_{hashlib.md5(json.dumps(task_data, sort_keys=True).encode()).hexdigest()[:8]}")
    thread_id = threading.current_thread().ident
    
    print(f"[Thread {thread_id}] Starting text processing task {task_id}")
    print(f"[Thread {thread_id}] Task data: {task_data}")
    
    # Extract documentId and userId from task_data
    document_id = task_data.get('documentId')
    user_id = task_data.get('userId')
    
    if not document_id or not user_id:
        print(f"[Thread {thread_id}] Missing documentId or userId in task data")
        return {
            'task_id': task_id,
            'error': 'Missing documentId or userId',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Fetch document content from MongoDB
    content = fetch_document_content(document_id, user_id)
    if not content:
        print(f"[Thread {thread_id}] Could not fetch content for document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Document not found or empty content',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Track processing time
    start_time = time.time()
    transcript = get_transcript(content)
    result_data = generate_stuff(transcript)
    processing_time = time.time() - start_time

    # Parse result data and save to MongoDB
    update_success = update_document_content(document_id, user_id, result_data)
    
    if not update_success:
        print(f"[Thread {thread_id}] Failed to update document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Failed to update document in MongoDB',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    result = {
        'task_id': task_id,
        'input': task_data,
        'processing_time': processing_time,
        'status': 'completed',
        'thread_id': thread_id,
        'processed_at': time.time()
    }
    
    print(f"[Thread {thread_id}] Completed text task {task_id} after {processing_time:.2f}s")
    return result

def process_website_task(task_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process website task 
    """
    task_id = task_data.get('id', f"{uuid.uuid1()}_{hashlib.md5(json.dumps(task_data, sort_keys=True).encode()).hexdigest()[:8]}")
    thread_id = threading.current_thread().ident
    
    print(f"[Thread {thread_id}] Starting website processing task {task_id}")
    print(f"[Thread {thread_id}] Task data: {task_data}")
    
    # Extract documentId and userId from task_data
    document_id = task_data.get('documentId')
    user_id = task_data.get('userId')
    
    if not document_id or not user_id:
        print(f"[Thread {thread_id}] Missing documentId or userId in task data")
        return {
            'task_id': task_id,
            'error': 'Missing documentId or userId',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Fetch document content from MongoDB
    content = fetch_document_content(document_id, user_id)
    if not content:
        print(f"[Thread {thread_id}] Could not fetch content for document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Document not found or empty content',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    # Track processing time
    start_time = time.time()
    scraped_content = asyncio.run(scrape_website("https://www.geeksforgeeks.org/dsa/array-data-structure-guide/"))
    result_data = generate_stuff(scraped_content)
    processing_time = time.time() - start_time

    # Parse result data and save to MongoDB
    update_success = update_document_content(document_id, user_id, result_data)
    
    if not update_success:
        print(f"[Thread {thread_id}] Failed to update document {document_id}")
        return {
            'task_id': task_id,
            'error': 'Failed to update document in MongoDB',
            'status': 'failed',
            'thread_id': thread_id,
            'processed_at': time.time()
        }
    
    result = {
        'task_id': task_id,
        'input': task_data,
        'processing_time': processing_time,
        'status': 'completed',
        'thread_id': thread_id,
        'processed_at': time.time()
    }
    
    print(f"[Thread {thread_id}] Completed website task {task_id} after {processing_time:.2f}s")
    return result

def process_task(queue_name: str, task_data: Dict[str, Any]) -> None:
    """
    Process a single task based on queue name
    """
    try:
        # Create a unique task identifier
        task_hash = hash(json.dumps(task_data, sort_keys=True))
        
        with processing_lock:
            if task_hash in processed_tasks:
                print(f"Task already processed, skipping: {task_hash}")
                return
            processed_tasks.add(task_hash)
        
        # Route task to appropriate processor
        if queue_name == 'process-pdf':
            result = process_pdf_task(task_data)
        elif queue_name == 'process-text':
            result = process_text_task(task_data)
        elif queue_name == 'process-ytvideo':
            result = process_youtube_video(task_data)
        elif queue_name == 'process-website':
            result = process_website_task(task_data)
        else:
            print(f"Unknown queue: {queue_name}, processing as test task")
        
        # what to do with result
        
    except Exception as e:
        print(f"Error processing task from {queue_name}: {e}")
        with processing_lock:
            processed_tasks.discard(task_hash)

def poll_queue(queue_name: str, executor: ThreadPoolExecutor) -> None:
    """
    Continuously poll a Redis queue for new tasks
    """
    print(f"Starting to poll queue: {queue_name}")
    
    while True:
        try:
            # Use blocking pop with timeout
            result = redis_client.brpop(queue_name, timeout=WORKER_CONFIG['poll_interval'])
            
            if result:
                queue, task_json = result
                queue_name = queue.decode('utf-8')
                
                try:
                    task_data = json.loads(task_json.decode('utf-8'))
                    print(f"Received task from {queue_name}: {task_data}")
                    
                    # Submit task to thread pool
                    executor.submit(process_task, queue_name, task_data)
                    
                except json.JSONDecodeError as e:
                    print(f"Invalid JSON in task from {queue_name}: {e}")
                except Exception as e:
                    print(f"Error processing task from {queue_name}: {e}")
                    
        except redis.RedisError as e:
            print(f"Redis error while polling {queue_name}: {e}")
            time.sleep(5) 
        except Exception as e:
            print(f"Unexpected error while polling {queue_name}: {e}")
            time.sleep(1)

def start_worker():
    """
    Start the multi-threaded Redis queue worker
    """
    print('Starting MindMate Redis Worker...')
    print(f'Redis URL: {REDIS_URL}')
    print('Worker configuration:')
    print(f'- Max workers: {WORKER_CONFIG["max_workers"]}')
    print(f'- Poll interval: {WORKER_CONFIG["poll_interval"]}s')
    print(f'- Task timeout: {WORKER_CONFIG["task_timeout"]}s')
    
    # Test Redis connection
    try:
        redis_client.ping()
        print('✓ Redis connection successful')
    except redis.RedisError as e:
        print(f'✗ Redis connection failed: {e}')
        return
    
    # Queues to monitor
    queues = ['process-pdf', 'process-text', 'process-ytvideo', 'process-website']
    
    print(f'Monitoring queues: {queues}')
    print('Worker is ready and waiting for tasks...\n')
    
    # Create thread pool for task processing
    with ThreadPoolExecutor(max_workers=WORKER_CONFIG['max_workers']) as executor:
        # Start polling threads for each queue
        polling_threads = []
        
        for queue_name in queues:
            thread = threading.Thread(target=poll_queue, args=(queue_name, executor))
            thread.daemon = True
            thread.start()
            polling_threads.append(thread)
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(10)
                # Optional: Print worker status
                print(f"Worker status - Processed tasks: {len(processed_tasks)}")
                
        except KeyboardInterrupt:
            print("\nShutting down worker...")
            return

if __name__ == '__main__':
    start_worker()