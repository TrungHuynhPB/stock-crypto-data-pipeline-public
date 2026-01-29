"""
Document processing utilities for PDF and other document types.
Handles document loading, chunking, and embedding for RAG.
"""

import os
from typing import List, Optional
from pathlib import Path
from langchain_community.document_loaders import PyPDFLoader, TextLoader, CSVLoader
try:
    from langchain_text_splitters import RecursiveCharacterTextSplitter
except ImportError:
    from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()


class DocumentProcessor:
    """Processes documents for RAG system."""
    
    def __init__(
        self,
        pinecone_api_key: Optional[str] = None,
        pinecone_index_name: Optional[str] = None,
        embedding_model: str = "text-embedding-3-small"
    ):
        """
        Initialize document processor.
        
        Args:
            pinecone_api_key: Pinecone API key (defaults to PINECONE_API_KEY env var)
            pinecone_index_name: Pinecone index name (defaults to PINECONE_INDEX_NAME env var)
            embedding_model: OpenAI embedding model name
        """
        self.pinecone_api_key = pinecone_api_key or os.getenv("PINECONE_API_KEY")
        self.pinecone_index_name = pinecone_index_name or os.getenv("PINECONE_INDEX_NAME", "fa-dae2-capstone")
        self.embedding_model = embedding_model
        
        if not self.pinecone_api_key:
            raise ValueError("Pinecone API key is required. Set PINECONE_API_KEY environment variable.")
        
        # Initialize Pinecone
        self.pc = Pinecone(api_key=self.pinecone_api_key)
        
        # Initialize embeddings
        # text-embedding-3-small uses 1536 dimensions by default
        self.embeddings = OpenAIEmbeddings(model=embedding_model)
        
        # Initialize text splitter
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
            separators=["\n\n", "\n", " ", ""]
        )
    
    def load_document(self, file_path: str) -> List:
        """
        Load a document from file path.
        
        Args:
            file_path: Path to the document file
            
        Returns:
            List of Document objects
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_ext = file_path.suffix.lower()
        
        if file_ext == ".pdf":
            loader = PyPDFLoader(str(file_path))
        elif file_ext == ".txt":
            loader = TextLoader(str(file_path))
        elif file_ext == ".csv":
            loader = CSVLoader(str(file_path))
        else:
            raise ValueError(f"Unsupported file type: {file_ext}")
        
        documents = loader.load()
        return documents
    
    def process_documents(
        self,
        file_paths: List[str],
        namespace: Optional[str] = None
    ) -> PineconeVectorStore:
        """
        Process multiple documents and store them in Pinecone.
        
        Args:
            file_paths: List of file paths to process
            namespace: Optional namespace for Pinecone (for separating document sets)
            
        Returns:
            PineconeVectorStore instance
        """
        all_documents = []
        
        # Load all documents
        for file_path in file_paths:
            print(f"Loading document: {file_path}")
            docs = self.load_document(file_path)
            all_documents.extend(docs)
        
        # Split documents into chunks
        print(f"Splitting {len(all_documents)} documents into chunks...")
        chunks = self.text_splitter.split_documents(all_documents)
        print(f"Created {len(chunks)} chunks")
        
        # Create or get Pinecone index
        self._ensure_index_exists()
        
        # Store in Pinecone
        print(f"Storing documents in Pinecone index: {self.pinecone_index_name}")
        vectorstore = PineconeVectorStore.from_documents(
            documents=chunks,
            embedding=self.embeddings,
            index_name=self.pinecone_index_name,
            namespace=namespace
        )
        
        print(f"✅ Successfully stored {len(chunks)} document chunks in Pinecone")
        return vectorstore
    
    def get_vectorstore(self, namespace: Optional[str] = None) -> PineconeVectorStore:
        """
        Get existing Pinecone vector store.
        
        Args:
            namespace: Optional namespace
            
        Returns:
            PineconeVectorStore instance
        """
        self._ensure_index_exists()
        
        return PineconeVectorStore(
            index_name=self.pinecone_index_name,
            embedding=self.embeddings,
            namespace=namespace
        )
    
    def _ensure_index_exists(self):
        """Ensure Pinecone index exists, create if it doesn't."""
        try:
            # Check if index exists
            existing_indexes = [idx.name for idx in self.pc.list_indexes()]
            
            if self.pinecone_index_name not in existing_indexes:
                print(f"Creating Pinecone index: {self.pinecone_index_name}")
                # Get embedding dimension (1536 for text-embedding-3-small, 3072 for text-embedding-3-large)
                # OpenAI embeddings default to 1536 for text-embedding-3-small
                dimension = 1536 if "small" in self.embedding_model else (3072 if "large" in self.embedding_model else 1536)
                
                self.pc.create_index(
                    name=self.pinecone_index_name,
                    dimension=dimension,
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud="aws",
                        region="us-east-1"
                    )
                )
                print(f"✅ Created index: {self.pinecone_index_name}")
            else:
                print(f"✅ Index already exists: {self.pinecone_index_name}")
        except Exception as e:
            print(f"Warning: Could not verify/create index: {e}")
            # Continue anyway - might already exist or have permission issues

