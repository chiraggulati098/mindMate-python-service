import os
import boto3
import tempfile
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional
import dotenv

dotenv.load_dotenv()

class S3Client:
    """S3/R2 client for file operations"""
    
    def __init__(self):
        self.endpoint_url = os.getenv("R2_ENDPOINT")
        self.access_key_id = os.getenv("R2_ACCESS_KEY_ID")
        self.secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY")
        self.bucket_name = os.getenv("R2_BUCKET_NAME")
        self.public_url = os.getenv("R2_PUBLIC_URL")
        
        if not all([self.endpoint_url, self.access_key_id, self.secret_access_key, self.bucket_name]):
            raise ValueError("Missing R2/S3 credentials in environment variables")
        
        # Store the original endpoint URL for URL parsing
        self.full_endpoint_url = self.endpoint_url
        
        self.client = self._create_client()
    
    def _create_client(self):
        """Create S3 client for Cloudflare R2"""
        try:
            # Extract base endpoint URL (without bucket path)
            # If endpoint includes bucket name, remove it
            base_endpoint = self.endpoint_url
            if f"/{self.bucket_name}" in base_endpoint:
                base_endpoint = base_endpoint.replace(f"/{self.bucket_name}", "")
            
            print(f"Using S3 endpoint: {base_endpoint}")
            print(f"Using bucket: {self.bucket_name}")
            
            client = boto3.client(
                's3',
                endpoint_url=base_endpoint,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key,
                region_name='auto'  # Cloudflare R2 uses 'auto'
            )
            print("✓ S3/R2 client initialized successfully")
            return client
        except Exception as e:
            print(f"✗ Failed to initialize S3/R2 client: {e}")
            raise
    
    def extract_s3_key_from_url(self, file_url: str) -> Optional[str]:
        """
        Extract S3 key from R2/S3 URL
        
        Args:
            file_url (str): The S3/R2 URL
            
        Returns:
            Optional[str]: The S3 key if extracted successfully
        """
        try:
            # Parse the URL to extract the key
            # Expected format: https://endpoint/bucket/key
            # Example: https://ba5fcc7c73a644484da57f186c666593.r2.cloudflarestorage.com/mindmate-storage/documents/...
            
            print(f"Extracting S3 key from URL: {file_url}")
            print(f"Full endpoint URL: {self.full_endpoint_url}")
            
            # Simple approach: if URL starts with full endpoint, extract the remaining path
            if file_url.startswith(self.full_endpoint_url):
                # Remove the full endpoint URL and any leading slashes
                s3_key = file_url[len(self.full_endpoint_url):].lstrip('/')
                
                # The files are actually stored with bucket name as prefix in the key
                # So we need to add the bucket name back as a prefix
                if not s3_key.startswith(f"{self.bucket_name}/"):
                    s3_key = f"{self.bucket_name}/{s3_key}"
                
                print(f"Extracted S3 key: {s3_key}")
                return s3_key
            else:
                print(f"URL does not match expected endpoint: {file_url}")
                return None
                
        except Exception as e:
            print(f"Error extracting S3 key from URL: {e}")
            return None

    def download_file_from_url(self, file_url: str) -> Optional[str]:
        """
        Download file from S3/R2 URL by extracting key and using S3 client
        
        Args:
            file_url (str): The S3/R2 URL of the file
            
        Returns:
            Optional[str]: Path to the downloaded temporary file, None if failed
        """
        try:
            print(f"Processing S3/R2 URL: {file_url}")
            
            # First try to extract S3 key and use S3 client
            s3_key = self.extract_s3_key_from_url(file_url)
            if s3_key:
                print(f"Extracted S3 key: {s3_key}")
                temp_file_path = self.download_file_from_s3(s3_key)
                if temp_file_path:
                    return temp_file_path
                    
            # If S3 download failed, try direct HTTP download as fallback
            print("S3 download failed, falling back to direct HTTP download")
            
            # First try the original URL
            temp_file = self._download_file_via_http(file_url)
            if temp_file:
                return temp_file
                
            # If that fails and we have a public URL configured, try constructing the URL differently
            if self.public_url and s3_key:
                alternative_url = f"{self.public_url.rstrip('/')}/{s3_key}"
                print(f"Trying alternative public URL: {alternative_url}")
                return self._download_file_via_http(alternative_url)
                
            return None
            
        except Exception as e:
            print(f"✗ Unexpected error in download_file_from_url: {e}")
            return None
    
    def _download_file_via_http(self, file_url: str) -> Optional[str]:
        """
        Download file directly via HTTP
        
        Args:
            file_url (str): The HTTP URL of the file
            
        Returns:
            Optional[str]: Path to the downloaded temporary file, None if failed
        """
        try:
            print(f"Attempting direct HTTP download from: {file_url}")
            
            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            temp_file_path = temp_file.name
            temp_file.close()
            
            # Try with different approaches
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Download the file
            response = requests.get(file_url, stream=True, headers=headers, timeout=30)
            response.raise_for_status()
            
            with open(temp_file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            file_size = os.path.getsize(temp_file_path)
            print(f"✓ File downloaded successfully via HTTP to: {temp_file_path} (size: {file_size} bytes)")
            return temp_file_path
            
        except requests.exceptions.RequestException as e:
            print(f"✗ HTTP download error for {file_url}: {e}")
            # Try to get more details about the error
            if hasattr(e, 'response') and e.response is not None:
                print(f"  Status code: {e.response.status_code}")
                print(f"  Response headers: {dict(e.response.headers)}")
                try:
                    error_content = e.response.text[:500]  # First 500 chars
                    print(f"  Response content: {error_content}")
                except:
                    print("  Could not read response content")
            # Clean up failed download
            if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None
        except Exception as e:
            print(f"✗ Unexpected error in HTTP download: {e}")
            # Clean up failed download
            if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            return None
    
    def list_bucket_objects(self, prefix: str = "", max_keys: int = 10) -> None:
        """
        List objects in the bucket for debugging purposes
        
        Args:
            prefix (str): Prefix to filter objects
            max_keys (int): Maximum number of objects to list
        """
        try:
            print(f"Listing bucket objects with prefix '{prefix}'...")
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            if 'Contents' in response:
                print(f"Found {len(response['Contents'])} objects:")
                for obj in response['Contents']:
                    print(f"  - {obj['Key']} (size: {obj['Size']} bytes)")
            else:
                print("No objects found with this prefix")
                if prefix:  # If we searched with a prefix, try listing everything
                    print("Let's see what's actually in the bucket...")
                    self.list_bucket_objects("", 100)  # List more objects without prefix
                
        except Exception as e:
            print(f"Error listing bucket objects: {e}")

    def download_file_from_s3(self, s3_key: str) -> Optional[str]:
        """
        Download file from S3/R2 using boto3 to a temporary file
        
        Args:
            s3_key (str): The S3 key/path of the file
            
        Returns:
            Optional[str]: Path to the downloaded temporary file, None if failed
        """
        try:
            print(f"Downloading file from S3 key: {s3_key}")
            
            # First, let's check if the file exists and list nearby files for debugging
            key_parts = s3_key.split('/')
            if len(key_parts) > 1:
                # List objects with the parent directory as prefix
                parent_prefix = '/'.join(key_parts[:-1]) + '/'
                print(f"Checking if file exists and listing objects with prefix: {parent_prefix}")
                self.list_bucket_objects(parent_prefix, 20)
            
            # Create a temporary file
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
            temp_file_path = temp_file.name
            temp_file.close()
            
            # Download the file from S3/R2
            self.client.download_file(self.bucket_name, s3_key, temp_file_path)
            
            print(f"✓ File downloaded successfully from S3 to: {temp_file_path}")
            return temp_file_path
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchKey':
                print(f"✗ File not found in S3: {s3_key}")
                print("Trying to list all objects in bucket for debugging...")
                self.list_bucket_objects("", 100)
                # Also try to list objects with common prefixes
                common_prefixes = ['uploads/', 'files/', 'pdfs/', 'storage/', '']
                for prefix in common_prefixes:
                    if prefix != "":
                        print(f"\nTrying prefix '{prefix}'...")
                        self.list_bucket_objects(prefix, 20)
            else:
                print(f"✗ S3 client error downloading {s3_key}: {e}")
            return None
        except NoCredentialsError:
            print("✗ S3 credentials not available")
            return None
        except Exception as e:
            print(f"✗ Unexpected error downloading from S3: {e}")
            return None
    
    def cleanup_temp_file(self, file_path: str) -> None:
        """
        Clean up temporary file
        
        Args:
            file_path (str): Path to the temporary file to delete
        """
        try:
            if file_path and os.path.exists(file_path):
                os.unlink(file_path)
                print(f"✓ Cleaned up temporary file: {file_path}")
        except Exception as e:
            print(f"✗ Error cleaning up temporary file {file_path}: {e}")

# Global S3 client instance
_s3_client = None

def get_s3_client() -> S3Client:
    """Get or create S3 client singleton"""
    global _s3_client
    if _s3_client is None:
        _s3_client = S3Client()
    return _s3_client

def download_file_from_url(file_url: str) -> Optional[str]:
    """
    Convenience function to download file from public URL
    
    Args:
        file_url (str): The public URL of the file
        
    Returns:
        Optional[str]: Path to the downloaded temporary file
    """
    client = get_s3_client()
    return client.download_file_from_url(file_url)

def download_file_from_s3_key(s3_key: str) -> Optional[str]:
    """
    Convenience function to download file from S3 key
    
    Args:
        s3_key (str): The S3 key/path of the file
        
    Returns:
        Optional[str]: Path to the downloaded temporary file
    """
    client = get_s3_client()
    return client.download_file_from_s3(s3_key)

def cleanup_temp_file(file_path: str) -> None:
    """
    Convenience function to clean up temporary file
    
    Args:
        file_path (str): Path to the temporary file to delete
    """
    client = get_s3_client()
    client.cleanup_temp_file(file_path)

def test_s3_bucket_access() -> None:
    """
    Convenience function to test bucket access and list contents for debugging
    """
    try:
        client = get_s3_client()
        print(f"\n=== TESTING S3/R2 BUCKET ACCESS ===")
        print(f"Bucket: {client.bucket_name}")
        print(f"Endpoint: {client.endpoint_url}")
        
        # List all bucket contents to see what's actually there
        print(f"\n=== COMPLETE BUCKET LISTING ===")
        client.list_bucket_objects("", 100)
        print(f"=== END BUCKET TEST ===\n")
        
    except Exception as e:
        print(f"Error in bucket access test: {e}")
        import traceback
        traceback.print_exc()