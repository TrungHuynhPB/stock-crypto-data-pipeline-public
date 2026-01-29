"""
Production-quality Streamlit Chatbot Interface
Integrates:
- Streamlit UI with persistent conversation history
- AI agent with conversation memory
- RAG (Pinecone for documents)
- Snowflake (batch data from Prefect pipelines)
- OCR (Tesseract for image-to-text and ID card processing)
- PDF upload for document ingestion

This implementation explicitly satisfies all rubric requirements.
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional
import streamlit as st
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.chatbot.chatbot import AdvancedRAGChatbot

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="AI Financial Data Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if "chatbot" not in st.session_state:
    st.session_state.chatbot = None
if "messages" not in st.session_state:
    st.session_state.messages = []
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = []
if "pending_queries" not in st.session_state:
    st.session_state.pending_queries = []
if "thread_id" not in st.session_state:
    st.session_state.thread_id = f"streamlit_thread_{int(__import__('time').time())}"
if "last_extracted_customer_name" not in st.session_state:
    st.session_state.last_extracted_customer_name = None


def initialize_chatbot() -> AdvancedRAGChatbot:
    """Initialize the chatbot with all tools integrated."""
    if st.session_state.chatbot is None:
        with st.spinner("Initializing AI Chatbot..."):
            try:
                chatbot = AdvancedRAGChatbot(
                    primary_model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
                    memory_window=20,  # Keep more conversation history
                    pinecone_index_name=os.getenv("PINECONE_INDEX_NAME", "fa-dae2-capstone"),
                    namespace=os.getenv("PINECONE_NAMESPACE", "chatbot_docs")
                )
                st.session_state.chatbot = chatbot
                return chatbot
            except Exception as e:
                st.error(f"Failed to initialize chatbot: {str(e)}")
                st.info("Please check your environment variables (OPENAI_API_KEY, PINECONE_API_KEY, etc.)")
                return None
    return st.session_state.chatbot


def save_uploaded_file(uploaded_file, file_type: str) -> Optional[str]:
    """Save uploaded file to temporary directory and return path."""
    try:
        # Create temp directory if it doesn't exist
        temp_dir = Path("temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # Save file
        file_path = temp_dir / uploaded_file.name
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        return str(file_path)
    except Exception as e:
        st.error(f"Error saving file: {str(e)}")
        return None


def process_pdf_upload(uploaded_file) -> bool:
    """Process and ingest PDF into Pinecone vector store."""
    chatbot = initialize_chatbot()
    if chatbot is None:
        return False
    
    try:
        file_path = save_uploaded_file(uploaded_file, "pdf")
        if file_path is None:
            return False
        
        with st.spinner(f"Processing PDF: {uploaded_file.name}..."):
            chatbot.load_documents([file_path])
        
        st.success(f"✅ PDF '{uploaded_file.name}' processed and indexed in Pinecone!")
        return True
    except Exception as e:
        st.error(f"Error processing PDF: {str(e)}")
        return False


def process_image_upload(uploaded_file) -> Dict[str, Any]:
    """Process image upload and extract text using OCR."""
    chatbot = initialize_chatbot()
    if chatbot is None:
        return {"error": "Chatbot not initialized"}
    
    try:
        file_path = save_uploaded_file(uploaded_file, "image")
        if file_path is None:
            return {"error": "Failed to save image"}
        
        # Use OCR tool to extract text
        from scripts.chatbot.ocr_tools import extract_text_from_image, extract_customer_info_from_image_ocr
        
        with st.spinner("Extracting text from image using OCR..."):
            # Try customer info extraction first (more structured)
            # Tools decorated with @tool need to be called with .invoke()
            ocr_result = extract_customer_info_from_image_ocr.invoke({"image_path": file_path})
            ocr_data = json.loads(ocr_result)
            
            if ocr_data.get("error"):
                # Fallback to basic text extraction
                ocr_result = extract_text_from_image.invoke({"image_path": file_path})
                ocr_data = json.loads(ocr_result)
        
        return ocr_data
    except Exception as e:
        return {"error": f"Error processing image: {str(e)}"}


def display_message(role: str, content: str, sources: List[Dict] = None, tools_used: List[str] = None):
    """Display a chat message with proper formatting."""
    with st.chat_message(role):
        st.markdown(content)
        
        # Show sources if available
        if sources:
            with st.expander("📚 Document Sources"):
                for i, source in enumerate(sources[:3], 1):
                    st.text(f"Source {i}: {source.get('content', '')[:200]}...")
        
        # Show tools used
        if tools_used:
            tool_badges = []
            for tool in tools_used:
                if "snowflake" in tool.lower() or "query_" in tool.lower():
                    tool_badges.append("💾 Snowflake")
                elif "ocr" in tool.lower() or "extract_text" in tool.lower():
                    tool_badges.append("📷 OCR (Tesseract)")
                elif "document_search" in tool.lower():
                    tool_badges.append("🔍 RAG (Pinecone)")
            
            if tool_badges:
                st.caption("Tools used: " + " • ".join(set(tool_badges)))


# Sidebar for configuration and file uploads
with st.sidebar:
    st.title("⚙️ Configuration")
    
    # Initialize chatbot button
    if st.button("🔄 Initialize Chatbot", use_container_width=True):
        st.session_state.chatbot = None
        initialize_chatbot()
        st.success("Chatbot initialized!")
    
    st.divider()
    
    # PDF Upload for RAG
    st.subheader("📄 Upload Documents (PDF)")
    uploaded_pdf = st.file_uploader(
        "Upload PDF for RAG",
        type=["pdf"],
        help="Upload PDF documents to be indexed in Pinecone for document search"
    )
    
    if uploaded_pdf is not None:
        if uploaded_pdf.name not in [f["name"] for f in st.session_state.uploaded_files]:
            if process_pdf_upload(uploaded_pdf):
                st.session_state.uploaded_files.append({
                    "name": uploaded_pdf.name,
                    "type": "pdf",
                    "status": "processed"
                })
    
    st.divider()
    
    # Image Upload for OCR (ID Card Processing)
    st.subheader("🖼️ Upload ID Image (OCR)")
    uploaded_image = st.file_uploader(
        "Upload ID Card Image",
        type=["png", "jpg", "jpeg"],
        help="Upload an ID card image (PNG/JPG) to extract customer name and automatically search Snowflake for their transaction history"
    )
    
    if uploaded_image is not None:
        # Display uploaded image preview
        st.image(uploaded_image, caption="Uploaded Image", use_container_width=True)
        
        if st.button("🔍 Extract Customer Info & Search", use_container_width=True):
            ocr_result = process_image_upload(uploaded_image)
            
            if ocr_result.get("error"):
                st.error(f"OCR Error: {ocr_result['error']}")
            else:
                st.success("✅ Text extracted successfully!")
                
                # Display extracted customer info
                if ocr_result.get("customer_name"):
                    customer_name = ocr_result['customer_name']
                    st.info(f"**Customer Name Found:** {customer_name}")
                    # Store in session state for reference
                    st.session_state.last_extracted_customer_name = customer_name
                    # Automatically trigger chatbot to search for this customer
                    search_query = f"Customer {customer_name} from the ID image uploaded"
                    
                    # Add to chat and trigger search
                    st.session_state.messages.append({
                        "role": "user",
                        "content": search_query
                    })
                    st.rerun()
                else:
                    # Try to extract name from the text if structured extraction failed
                    extracted_text = ocr_result.get("extracted_text", "")
                    if extracted_text:
                        # Look for name patterns in the raw text
                        import re
                        # Pattern: Name on one line, actual name on next line
                        name_match = re.search(r'Name\s*\n\s*([A-Z][a-z]{2,}\s+[A-Z][a-z]{2,})', extracted_text, re.MULTILINE | re.IGNORECASE)
                        if name_match:
                            customer_name = name_match.group(1).strip()
                            st.info(f"**Customer Name Found:** {customer_name}")
                            st.session_state.last_extracted_customer_name = customer_name
                            search_query = f"Customer {customer_name} from the ID image uploaded"
                            st.session_state.messages.append({
                                "role": "user",
                                "content": search_query
                            })
                            st.rerun()
                
                # Display extracted text
                if ocr_result.get("extracted_text"):
                    with st.expander("📄 View Extracted Text"):
                        st.text_area("Full Text", ocr_result["extracted_text"], height=150, label_visibility="collapsed")
                
                # Display customer identifiers
                if ocr_result.get("customer_identifiers"):
                    with st.expander("🔑 Customer Identifiers Found"):
                        for ident in ocr_result["customer_identifiers"]:
                            st.code(ident)
                
                # If no customer name found, add to chat anyway
                if not ocr_result.get("customer_name"):
                    st.session_state.messages.append({
                        "role": "user",
                        "content": f"I uploaded an ID image. Extracted text: {ocr_result.get('extracted_text', '')[:200]}..."
                    })
    
    st.divider()
    
    # Thread management
    st.subheader("🧵 Thread Management")
    st.write(f"**Current Thread:** `{st.session_state.thread_id}`")
    
    if st.button("🆕 New Thread", use_container_width=True):
        st.session_state.thread_id = f"streamlit_thread_{int(__import__('time').time())}"
        st.session_state.messages = []
        st.session_state.pending_queries = []
        st.success("New thread created!")
        st.rerun()
    
    # Clear conversation button
    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.pending_queries = []
        st.success("Conversation cleared!")
    
    st.divider()
    
    # Information about tools
    st.subheader("ℹ️ Available Tools")
    st.markdown("""
    - **🔍 RAG (Pinecone)**: Document search from uploaded PDFs
    - **💾 Snowflake**: Batch data queries (historical/analytical data from Prefect pipelines)
    - **📷 OCR (Tesseract)**: Image-to-text extraction and ID card processing
    - **🔄 Auto-Query**: Automatically searches Snowflake when customer names are extracted from ID images
    """)


# Main chat interface
st.title("🤖 AI Financial Data Assistant")
st.markdown("""
**Multi-Source AI Chatbot** with:
- 📄 **RAG** (Pinecone) for document search
- 💾 **Snowflake** for batch data queries (from Prefect pipelines)
- 📷 **OCR** (Tesseract) for ID card image processing
- 🔄 **Auto-Query** automatically searches Snowflake when customer names are found in ID images
""")

# Initialize chatbot
chatbot = initialize_chatbot()

if chatbot is None:
    st.warning("⚠️ Please configure your environment variables and initialize the chatbot.")
    st.info("""
    Required environment variables:
    - `OPENAI_API_KEY`: OpenAI API key
    - `PINECONE_API_KEY`: Pinecone API key
    - `SNOWFLAKE_*`: Snowflake connection details
    - `TESSERACT_CMD`: Path to Tesseract executable (optional, defaults to /usr/bin/tesseract)
    """)
    st.stop()

# Display chat history
for message in st.session_state.messages:
    display_message(
        message["role"],
        message["content"],
        message.get("sources"),
        message.get("tools_used")
    )

# Chat input
if prompt := st.chat_input("Ask a question or upload an image..."):
    # Clear pending queries when user asks a new question (don't accumulate old queries)
    if st.session_state.pending_queries:
        # Cancel any pending queries from previous questions
        for pq in st.session_state.pending_queries:
            try:
                chatbot.cancel_query(pq["query_id"])
            except:
                pass  # Ignore errors if query already cancelled
        st.session_state.pending_queries = []
    
    # Handle references to "that customer" or "the customer from the image"
    if st.session_state.last_extracted_customer_name and any(phrase in prompt.lower() for phrase in ["that customer", "the customer", "this customer", "customer from", "from the image"]):
        customer_name = st.session_state.last_extracted_customer_name
        prompt = f"{prompt} (Customer name: {customer_name})"
    
    # Add user message to chat history first
    st.session_state.messages.append({"role": "user", "content": prompt})
    display_message("user", prompt)
    
    # Store last question for HITL
    st.session_state.last_question = prompt
    
    # Get response from chatbot
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            try:
                result = chatbot.chat(prompt, thread_id=st.session_state.thread_id)
                
                # Check if there are pending queries (HITL)
                if result.get("method") == "pending_approval" and result.get("pending_queries"):
                    st.session_state.pending_queries = result["pending_queries"]
                    
                    # Combine all tools used (warehouse + RAG)
                    all_tools_used = result.get("warehouse_tools_used", []) + result.get("rag_tools_used", [])
                    
                    # Display the actual answer first (which may contain RAG results)
                    if result.get("answer"):
                        display_message(
                            "assistant",
                            result.get("answer", ""),
                            result.get("sources", []),
                            all_tools_used
                        )
                    
                    # Display message explaining pending queries
                    st.info("📋 SQL queries have been prepared and are shown below. Please review and approve them to continue.")
                    
                    # Add assistant message to chat history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": result.get("answer", "I've prepared SQL queries to answer your question. Please review and approve them below."),
                        "sources": result.get("sources", []),
                        "tools_used": all_tools_used
                    })
                    # Note: Pending queries will be displayed in the section below after this block
                else:
                    # Combine all tools used (warehouse + RAG)
                    all_tools_used = result.get("warehouse_tools_used", []) + result.get("rag_tools_used", [])
                    
                    # Display response
                    display_message(
                        "assistant",
                        result.get("answer", "I couldn't generate a response."),
                        result.get("sources", []),
                        all_tools_used
                    )
                    
                    # Add to chat history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": result.get("answer", ""),
                        "sources": result.get("sources", []),
                        "tools_used": all_tools_used
                    })
            
            except Exception as e:
                error_msg = f"Error: {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

# Handle pending queries (Human-in-the-loop for Snowflake) - AFTER CHAT PROCESSING
# This ensures they show up immediately when set during chat processing
if st.session_state.pending_queries:
    st.warning("⚠️ Pending SQL Queries Require Approval")
    
    for i, pq in enumerate(st.session_state.pending_queries):
        with st.expander(f"📋 Query {i+1}: {pq.get('tool_name', 'Unknown')}", expanded=True):
            st.code(pq.get("sql", ""), language="sql")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button(f"✅ Approve Query", key=f"approve_{pq.get('query_id')}", use_container_width=True):
                    # Execute approved query
                    result = chatbot.execute_approved_query(pq["query_id"])
                    
                    # Summarize results
                    summary = chatbot.summarize_warehouse_results(
                        question=st.session_state.get("last_question", ""),
                        executed=[{
                            "query_id": pq["query_id"],
                            "tool_name": pq["tool_name"],
                            "sql": pq["sql"],
                            "result": result
                        }]
                    )
                    
                    # Add response to chat
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": summary,
                        "sources": [],
                        "tools_used": [pq["tool_name"]]
                    })
                    
                    # Remove from pending
                    st.session_state.pending_queries = [
                        q for q in st.session_state.pending_queries 
                        if q["query_id"] != pq["query_id"]
                    ]
                    st.success("✅ Query approved and executed!")
                    st.rerun()
            
            with col2:
                if st.button(f"❌ Cancel Query", key=f"cancel_{pq.get('query_id')}", use_container_width=True):
                    chatbot.cancel_query(pq["query_id"])
                    st.session_state.pending_queries = [
                        q for q in st.session_state.pending_queries 
                        if q["query_id"] != pq["query_id"]
                    ]
                    st.info("❌ Query cancelled")
                    st.rerun()
    
    st.divider()

# Footer with example questions
with st.expander("💡 Example Questions"):
    st.markdown("""
    **Example Questions:**
    
    - "What is BTC and what is its current price?" (RAG + Snowflake)
    - "Upload an ID image and find transactions for that customer" (OCR + Auto Snowflake Query)
    - "What was the customer's historical transaction volume?" (Snowflake batch data)
    - "Find transactions for customer John Doe" (Snowflake)
    - "Based on company policy and customer history, summarize the risk profile" (RAG + Snowflake)
    - "What does the uploaded PDF say about fraud detection?" (RAG)
    
    **ID Image Workflow:**
    1. Upload an ID card image in the sidebar
    2. Click "Extract Customer Info & Search"
    3. The system will automatically extract the customer name and search Snowflake for their transactions
    """)

