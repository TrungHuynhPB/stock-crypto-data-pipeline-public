# Quick Start Guide

## Prerequisites

1. **Python 3.8+**
2. **Tesseract OCR** installed on your system
3. **Environment variables** configured (see `.env` setup below)
4. **Infrastructure running**: Snowflake, Pinecone

## Step 1: Install Tesseract OCR

### Linux (Ubuntu/Debian)
```bash
sudo apt-get update
sudo apt-get install tesseract-ocr
```

### macOS
```bash
brew install tesseract
```

### Windows
Download installer from: https://github.com/UB-Mannheim/tesseract/wiki

Verify installation:
```bash
tesseract --version
```

## Step 2: Set Up Environment Variables

Create a `.env` file in the project root:

```bash
# OpenAI (Required)
OPENAI_API_KEY=sk-...
OPENAI_MODEL=gpt-4o-mini

# Pinecone (Required)
PINECONE_API_KEY=your-pinecone-key
PINECONE_INDEX_NAME=fa-dae2-capstone
PINECONE_NAMESPACE=chatbot_docs

# Snowflake (Required for batch queries)
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-user
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=DB_T23
SNOWFLAKE_SCHEMA=SC_T23
SNOWFLAKE_ROLE=your-role
SNOWFLAKE_PRIVATE_KEY_FILE_PATH=/path/to/private_key.p8

# Tesseract (Optional - defaults to /usr/bin/tesseract)
TESSERACT_CMD=/usr/bin/tesseract
```

## Step 3: Install Python Dependencies

```bash
uv sync
```

Or using pip:
```bash
pip install -r requirements.txt
```

## Step 4: Verify Infrastructure

### Verify Snowflake Connection
```bash
# Test connection (if you have a test script)
python scripts/s00_test_snowflake_connection.py
```

### Verify Pinecone Access
- The app will auto-create the Pinecone index if it doesn't exist
- Ensure `PINECONE_API_KEY` is set correctly

## Step 5: Run Streamlit App

```bash
streamlit run scripts/chatbot/chatbot_app.py
```

The app will open in your browser at `http://localhost:8501`

## Step 6: Test the System

### Test 1: OCR from ID Image (Auto-Query)
1. In the sidebar, upload an ID card image (PNG/JPG)
2. Click "Extract Customer Info & Search"
3. System will extract customer name and automatically search Snowflake for transactions
4. Review and approve any SQL queries if prompted

### Test 2: PDF Upload for RAG
1. In the sidebar, upload a PDF document
2. Wait for "✅ PDF processed" message
3. Ask: "What does the document say about [topic]?"
4. Agent will use Pinecone RAG to find relevant document chunks

### Test 3: Batch Data Query
```
Ask: "What was the historical transaction volume for customer John Doe?"
Expected: Agent prepares Snowflake query → Requires approval → Returns batch data
```

### Test 4: Customer Search by Name
```
Ask: "Find transactions for customer John Doe"
Expected: Agent uses query_transactions with customer_name parameter
```

### Test 5: Multi-Source Query
```
Ask: "Based on company policy and customer history, summarize the risk profile"
Expected: Agent queries both Pinecone (documents) and Snowflake (history)
```

## Troubleshooting

### Issue: Tesseract not found
```
Error: TesseractNotFoundError
Solution: Install Tesseract and set TESSERACT_CMD in .env
```

### Issue: Pinecone index not found
```
Error: IndexNotFoundError
Solution: The app will auto-create the index. Ensure PINECONE_API_KEY is correct.
```

### Issue: Snowflake connection failed
```
Error: snowflake.connector.errors.DatabaseError
Solution:
1. Verify all SNOWFLAKE_* variables in .env
2. Check private key file path is correct
3. Ensure Snowflake warehouse is running
```

### Issue: OpenAI API error
```
Error: openai.error.AuthenticationError
Solution: Verify OPENAI_API_KEY is set correctly
```

## Example Usage Flow

### Complete Workflow: Customer Transaction Analysis from ID Image

1. **Upload Customer ID Image**
   - Sidebar → Upload ID card image → Click "Extract Customer Info & Search"
   - OCR extracts customer name: "John Doe"

2. **Auto-Query Snowflake**
   - System automatically searches Snowflake for "John Doe"
   - SQL query prepared → User approves → Returns transaction history

3. **Query Historical Data**
   - Chat: "What is the historical transaction volume for John Doe?"
   - Agent prepares Snowflake query → User approves → Returns batch data

4. **Combine with Policy Documents**
   - Upload policy PDF in sidebar
   - Chat: "Based on company policy and John Doe's history, assess risk"
   - Agent queries Pinecone (policy) + Snowflake (history) → Unified response

## Next Steps

- Read `README_CHATBOT.md` for detailed architecture
- Read `RUBRIC_COVERAGE.md` for rubric requirement details
- Customize system prompt in `scripts/chatbot/chatbot.py` (Line 267)
- Add custom tools in respective tool files

## Support

For issues or questions:
1. Check `RUBRIC_COVERAGE.md` for implementation details
2. Review error messages in Streamlit UI
3. Check logs in terminal where Streamlit is running
