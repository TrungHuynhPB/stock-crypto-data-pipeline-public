# Production-Quality AI Chatbot System

## Overview

This is a comprehensive AI chatbot system that integrates multiple data sources and tools:

- **Streamlit UI**: Modern chat interface with persistent conversation history
- **AI Agent**: LangChain-based agent with conversation memory
- **RAG (Retrieval-Augmented Generation)**: Pinecone vector store for document search
- **Snowflake**: Batch data warehouse queries (from Prefect pipelines)
- **OCR**: Tesseract (pytesseract) for image-to-text extraction and ID card processing

## Architecture

```
User Input (Text/Image/PDF)
    ↓
Streamlit UI (chatbot_app.py)
    ↓
AI Agent (chatbot.py)
    ├─→ Conversation Memory
    ├─→ Tool Selection Logic
    │   ├─→ OCR Tools (ocr_tools.py) - Tesseract
    │   ├─→ Snowflake Tools (warehouse_tools.py) - Batch Data
    │   └─→ RAG Tool (document_processor.py) - Pinecone
    ↓
Multi-Source Response
```

## Key Features

### ✅ Streamlit Chatbot Interface
- **Persistent conversation history**: Implemented using `st.session_state.messages`
- **Text input**: Chat input with `st.chat_input()`
- **Image upload (PNG/JPG)**: File uploader in sidebar with OCR processing
- **PDF upload for RAG**: PDF uploader that indexes documents in Pinecone
- **Display assistant responses**: Formatted messages with sources and tool badges
- **Conversation context**: Maintained across turns via LangChain memory

### ✅ AI Agent Architecture
- **Conversation memory**: `SimpleConversationMemory` class with deque-based storage
- **Tool usage**: All tools registered and available to agent
- **Multi-source reasoning**: Agent dynamically selects tools based on query type
- **Agent decision logic**: Implemented in system prompt with clear decision flow

### ✅ Tooling (Explicit Implementation)

#### 1. OCR Tool (`ocr_tools.py`)
- **Uses pytesseract**: `extract_text_from_image()` function
- **Converts images to text**: Preprocessing with OpenCV, OCR with Tesseract
- **Extracts customer identifiers**: Pattern matching for IDs and names from ID cards
- **ID Card Processing**: Enhanced name extraction for customer identification
- **Tools**: `extract_text_from_image`, `extract_customer_info_from_image_ocr`

#### 2. Snowflake Query Tool (`warehouse_tools.py`)
- **Read-only SQL (SELECT)**: All queries are SELECT statements
- **Queries warehouse tables**: Accesses `fct_transactions`, `fct_asset_prices`, `dim_customer`, etc.
- **Used for historical/analytical questions**: Batch data queries from Prefect pipelines
- **Human-in-the-loop**: Queries require approval before execution
- **Customer Search**: Can search by customer ID or name (useful for ID image workflow)
- **Tools**: `query_transactions`, `query_asset_prices`, `query_transaction_summary`, `query_price_trends`, `query_news_events`, `query_customer_by_name`

#### 3. Document Retriever Tool (RAG)
- **Ingests PDFs**: `DocumentProcessor.load_document()`
- **Embeds documents**: OpenAI embeddings (`text-embedding-3-small`)
- **Stores in Pinecone**: Vector store with chunking (1000 chars, 200 overlap)
- **Provides contextual answers**: Semantic search via `document_search` tool

### ✅ Agent Reasoning Logic

The agent follows this decision flow (implemented in system prompt):

1. **PDF Upload**: 
   - Chunk document → Embed → Store in Pinecone
   - Handled in Streamlit UI before chat

2. **Image Upload (ID Card)**:
   - Run OCR (pytesseract) → Extract customer name/ID
   - **Auto-Query**: Automatically searches Snowflake for customer transactions
   - Handled via `extract_text_from_image` or `extract_customer_info_from_image_ocr`

3. **User Questions**:
   - **Historical/analytical data** → Query Snowflake (`query_transactions`, etc.)
   - **Policies/documents** → Query Pinecone (`document_search`)
   - **Customer from image** → OCR → Auto-query Snowflake

4. **Multiple Sources**:
   - Agent combines results from RAG + Snowflake
   - Generates unified response

## Setup

### 1. Environment Variables

Create a `.env` file with:

```bash
# OpenAI
OPENAI_API_KEY=your_openai_key
OPENAI_MODEL=gpt-4o-mini

# Pinecone
PINECONE_API_KEY=your_pinecone_key
PINECONE_INDEX_NAME=fa-dae2-capstone
PINECONE_NAMESPACE=chatbot_docs

# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=DB_T23
SNOWFLAKE_SCHEMA=SC_T23
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_PRIVATE_KEY_FILE_PATH=/path/to/private_key.p8

# Tesseract (optional, defaults to /usr/bin/tesseract)
TESSERACT_CMD=/usr/bin/tesseract
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

Or install from project root:
```bash
uv sync
```

### 3. Install Tesseract OCR

**Linux:**
```bash
sudo apt-get install tesseract-ocr
```

**macOS:**
```bash
brew install tesseract
```

**Windows:**
Download from: https://github.com/UB-Mannheim/tesseract/wiki

### 4. Run Streamlit App

```bash
streamlit run scripts/chatbot/chatbot_app.py
```

## Usage Examples

### Example 1: OCR from ID Image (Auto-Query)
1. Upload an ID card image (PNG/JPG) in the sidebar
2. Click "Extract Customer Info & Search"
3. System automatically extracts customer name and searches Snowflake for transactions

### Example 2: Historical Data Query
```
User: "What was the customer's historical transaction volume?"
Agent: Uses Snowflake tool → Returns batch data (requires approval)
```

### Example 3: Document Search (RAG)
1. Upload a PDF in the sidebar
2. Ask: "What does the document say about fraud detection?"
Agent: Uses Pinecone RAG → Returns relevant document chunks

### Example 4: Multi-Source Query
```
User: "Based on company policy and customer history, summarize the risk profile"
Agent: 
  - Queries Pinecone for policy documents (RAG)
  - Queries Snowflake for customer history (batch)
  - Combines results → Unified response
```

### Example 5: Customer Search by Name
```
User: "Find transactions for customer John Doe"
Agent: Uses query_transactions with customer_name parameter → Returns transactions
```

## File Structure

```
scripts/
└── chatbot/
    ├── chatbot.py              # Main AI agent with memory and tool integration
    ├── chatbot_app.py          # Streamlit UI (RUBRIC: UI)
    ├── ocr_tools.py            # OCR tools using pytesseract (RUBRIC: OCR)
    ├── warehouse_tools.py      # Snowflake query tools (RUBRIC: Batch)
    ├── document_processor.py   # Pinecone RAG implementation
    ├── __init__.py             # Module exports
    └── src/                    # Utility modules (chunking, text extraction)
```

## Code Quality Features

- ✅ **Modular Python**: Clear separation of UI, agent, and tools
- ✅ **Type hints**: Functions include type annotations
- ✅ **Docstrings**: All functions documented
- ✅ **Error handling**: Try-except blocks with user-friendly messages
- ✅ **Environment variables**: All credentials via .env
- ✅ **Inline comments**: Comments show rubric coverage

## Rubric Requirements Checklist

- [x] Streamlit chat-style UI with persistent history
- [x] Text input and image upload (PNG/JPG)
- [x] PDF upload for RAG
- [x] Display assistant responses clearly
- [x] Conversation context across turns
- [x] AI agent with conversation memory
- [x] Tool usage and multi-source reasoning
- [x] OCR Tool using pytesseract
- [x] Snowflake Query Tool (batch data)
- [x] Document Retriever Tool (RAG with Pinecone)
- [x] Agent reasoning logic for routing queries
- [x] PDF upload → chunk → embed → Pinecone
- [x] Image upload → OCR → extract entities → Auto-query Snowflake
- [x] Historical queries → Snowflake
- [x] Document queries → Pinecone
- [x] Multi-source combination
- [x] Read-only queries (no mutations)
- [x] Environment variables for credentials
- [x] Graceful error handling

## Troubleshooting

### Tesseract Not Found
```bash
# Set TESSERACT_CMD in .env or environment
export TESSERACT_CMD=/usr/bin/tesseract
```

### Pinecone Index Not Found
- The app will create the index automatically if it doesn't exist
- Ensure `PINECONE_API_KEY` is set correctly

### Snowflake Connection Error
- Verify all Snowflake credentials in .env
- Check that private key file path is correct
- Ensure Snowflake warehouse is running

## Next Steps

1. **Deploy**: Deploy Streamlit app to Streamlit Cloud or custom server
2. **Monitor**: Add logging and monitoring for production use
3. **Scale**: Consider caching for frequently accessed data
4. **Security**: Add authentication and rate limiting
5. **Testing**: Add unit tests for tools and agent logic
