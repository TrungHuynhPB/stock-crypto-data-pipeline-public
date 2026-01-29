"""
Advanced AI Chatbot with RAG capabilities using Pinecone and LangGraph.
Features:
- Core RAG with PDF/document processing
- PostgreSQL persistence with LangGraph
- Warehouse data integration
- Hybrid search (semantic + keyword)
- Multi-model orchestration
- Custom prompt engineering
- Advanced context handling
"""

import os
import sys
import json
from typing import List, Optional, Dict, Any, Literal, Annotated
from pathlib import Path
from dotenv import load_dotenv

# Langchain imports
try:
    from langchain_openai import ChatOpenAI, OpenAIEmbeddings
    from langchain_community.chat_models import ChatAnthropic
    from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
    from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, ToolMessage
    from langchain_core.callbacks import StreamingStdOutCallbackHandler
except ImportError as e:
    print("=" * 60)
    print("ERROR: LangChain packages not found!")
    print("=" * 60)
    print("Please install dependencies using one of:")
    print("  uv sync")
    print("  pip install -r requirements.txt")
    print("=" * 60)
    raise

# LangGraph imports
try:
    from langgraph.graph import StateGraph, END, START, MessagesState
    from langgraph.graph.message import add_messages
    from langgraph.checkpoint.postgres import PostgresSaver
    import psycopg
    from psycopg.rows import dict_row
except ImportError as e:
    print("=" * 60)
    print("ERROR: LangGraph packages not found!")
    print("=" * 60)
    print("Please install: uv sync")
    print("Or: pip install langgraph langgraph-checkpoint-postgres psycopg")
    print("=" * 60)
    raise

# Compression retriever is optional - make it gracefully unavailable if not found
ContextualCompressionRetriever = None
LLMChainExtractor = None
try:
    from langchain.retrievers import ContextualCompressionRetriever
    from langchain.retrievers.document_compressors import LLMChainExtractor
except ImportError:
    try:
        from langchain_community.retrievers import ContextualCompressionRetriever
        from langchain_community.retrievers.document_compressors import LLMChainExtractor
    except ImportError:
        # Compression not available - will disable compression feature
        pass

# Agent creation utilities will be imported when needed in _create_agent()
# This avoids import errors at module load time

try:
    from langchain.tools.retriever import create_retriever_tool
except ImportError:
    from langchain_core.tools.retriever import create_retriever_tool

# Local imports
from scripts.chatbot.document_processor import DocumentProcessor
from scripts.chatbot import warehouse_tools
from scripts.chatbot.warehouse_tools import WAREHOUSE_TOOLS
from scripts.chatbot.ocr_tools import OCR_TOOLS

load_dotenv()


def create_postgres_connection():
    """Create PostgreSQL connection for LangGraph checkpointing."""
    try:
        host = os.getenv("LANGGRAPH_POSTGRES_HOST")
        port = os.getenv("LANGGRAPH_POSTGRES_PORT")
        database = os.getenv("LANGGRAPH_POSTGRES_DB")
        user = os.getenv("LANGGRAPH_POSTGRES_USER")
        password = os.getenv("LANGGRAPH_POSTGRES_PASSWORD")

        db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"

        # Create connection with autocommit and dict_row factory
        connection = psycopg.connect(db_uri, autocommit=True, row_factory=dict_row)
        return connection
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        print("💡 Make sure PostgreSQL is running and credentials are correct in .env")
        return None


class AdvancedRAGChatbot:
    """Advanced RAG chatbot with hybrid search and multi-model support."""
    
    def __init__(
        self,
        primary_model: str = "gpt-4o-mini",
        secondary_model: Optional[str] = None,
        use_hybrid_search: bool = True,
        use_compression: bool = True,
        memory_window: int = 10,
        use_summary_memory: bool = False,
        pinecone_index_name: Optional[str] = None,
        namespace: Optional[str] = None
    ):
        """
        Initialize the chatbot.
        
        Args:
            primary_model: Primary LLM model (default: gpt-4o-mini)
            secondary_model: Secondary LLM for specific tasks (optional)
            use_hybrid_search: Enable hybrid search (semantic + keyword)
            use_compression: Enable document compression for better context
            memory_window: Number of messages to keep in memory
            use_summary_memory: Use summary-based memory instead of window
            pinecone_index_name: Pinecone index name
            namespace: Pinecone namespace
        """
        self.primary_model = primary_model
        self.secondary_model = secondary_model
        self.use_hybrid_search = use_hybrid_search
        self.use_compression = use_compression
        self.namespace = namespace
        
        # Initialize document processor
        self.doc_processor = DocumentProcessor(pinecone_index_name=pinecone_index_name)
        
        # Initialize vectorstore
        self.vectorstore = self.doc_processor.get_vectorstore(namespace=namespace)
        
        # Initialize LLMs
        self.primary_llm = self._create_llm(primary_model)
        self.secondary_llm = self._create_llm(secondary_model) if secondary_model else None
        
        # Initialize retriever
        self.retriever = self._create_retriever()
        
        # Initialize tools (RAG + warehouse tools)
        self.tools = self._create_tools()
        self.tools_by_name = {tool.name: tool for tool in self.tools}
        
        # Store thread_id for persistence (will be set per conversation)
        self.default_thread_id = "default"
        
        # Initialize agent graph with PostgreSQL persistence
        self.graph = self._create_agent_graph()
    
    def _create_llm(self, model_name: Optional[str]) -> Optional[Any]:
        """Create LLM instance."""
        if not model_name:
            return None
        
        if "gpt" in model_name.lower() or "openai" in model_name.lower():
            return ChatOpenAI(
                model=model_name,
                temperature=0.7,
                streaming=True,
                callbacks=[StreamingStdOutCallbackHandler()]
            )
        elif "claude" in model_name.lower() or "anthropic" in model_name.lower():
            return ChatAnthropic(
                model=model_name,
                temperature=0.7
            )
        else:
            # Default to OpenAI
            return ChatOpenAI(
                model=model_name,
                temperature=0.7
            )
    
    def _create_retriever(self):
        """Create retriever with optional hybrid search and compression."""
        # Base semantic retriever
        semantic_retriever = self.vectorstore.as_retriever(
            search_type="similarity",
            search_kwargs={"k": 5}
        )
        
        if self.use_hybrid_search:
            # Create BM25 retriever for keyword search
            # Note: This requires documents to be stored separately for BM25
            # For now, we'll use a combination approach
            retriever = semantic_retriever
            
            # Enhanced retriever with metadata filtering
            retriever = self.vectorstore.as_retriever(
                search_type="similarity_score_threshold",
                search_kwargs={
                    "k": 5,
                    "score_threshold": 0.3
                }
            )
        else:
            retriever = semantic_retriever
        
        # Apply compression if enabled and available
        if self.use_compression and self.primary_llm and ContextualCompressionRetriever and LLMChainExtractor:
            try:
                compressor = LLMChainExtractor.from_llm(self.primary_llm)
                compression_retriever = ContextualCompressionRetriever(
                    base_compressor=compressor,
                    base_retriever=retriever
                )
                return compression_retriever
            except Exception as e:
                print(f"Warning: Compression retriever not available: {e}. Using standard retriever.")
        
        return retriever
    
    def _create_tools(self):
        """Create tools for the agent (RAG + warehouse tools + OCR tools)."""
        tools = []
        
        # Create RAG retriever tool (Pinecone document search)
        try:
            rag_tool = create_retriever_tool(
                self.retriever,
                "document_search",
                "Search through uploaded documents (PDFs, text files) stored in Pinecone to find relevant information. Use this for general knowledge, concepts, policies, and context from documents."
            )
            tools.append(rag_tool)
        except Exception as e:
            print(f"Warning: Could not create retriever tool: {e}")
        
        # Add OCR tools (pytesseract for image-to-text and ID card processing)
        tools.extend(OCR_TOOLS)
        
        # Add warehouse tools (Snowflake batch data)
        tools.extend(WAREHOUSE_TOOLS)
        
        return tools
    
    def _create_agent_graph(self):
        """Create LangGraph agent with PostgreSQL persistence."""
        
        # Custom system prompt
        system_prompt = """You are an advanced AI assistant for a financial data analytics platform. 
You have access to multiple data sources and tools:

1. DOCUMENT SEARCH (RAG - Pinecone): Use document_search tool to search through uploaded PDFs and documents
   - Best for: Definitions, explanations, conceptual questions, general knowledge
   - Best for: Understanding what something is, company information, policies, document context
   - When users ask "What is X?" or "Tell me about X", consider whether they're asking for a definition or data
   - For definition questions, document_search is typically more appropriate than querying the database
   - Example: "What is Foundry AI?" → likely asking for definition/explanation, try document_search first
   - Example: "What is BTC?" → if asking what BTC is (definition), use document_search; if asking for price data, use Snowflake
   
2. OCR TOOLS (Tesseract): Use extract_text_from_image and extract_customer_info_from_image_ocr
   - Use for: Extracting text from uploaded images (PNG, JPG)
   - Use for: Extracting customer identifiers (names, IDs) from ID card images
   - When a customer name is extracted from an ID image, consider using Snowflake tools to search for that customer
   
3. SNOWFLAKE TOOLS: Use query_transactions, query_asset_prices, etc.
   - Best for: Specific data queries, historical records, analytical queries
   - Best for: Transaction data, price data, customer records, aggregated statistics
   - These tools query the Snowflake data warehouse (batch-loaded data from Prefect pipelines)
   - Use when the question explicitly requests data, records, or numerical information

Tool Selection Guidelines:
- For questions asking "What is X?" or "Define X": Consider whether the user wants a definition/explanation (use document_search) or specific data (use Snowflake)
- For questions asking "Show me", "Get", "Find", "Query": Typically these are data requests, use Snowflake tools
- For image uploads: Use OCR tools first, then consider Snowflake if customer information is extracted
- For combined questions: Use document_search for definitions, Snowflake for data

Available tools:
- RAG: document_search
- OCR: extract_text_from_image, extract_customer_info_from_image_ocr
- Snowflake: query_transactions, query_asset_prices, query_transaction_summary, query_price_trends, query_news_events, query_customer_by_name

ID Image Processing Workflow:
1. When user uploads an ID image, use extract_customer_info_from_image_ocr to extract customer name and/or ID
2. If customer information is found, consider using query_transactions or query_customer_by_name
3. Present the extracted customer information and their transaction data together

Human-in-the-loop for Snowflake queries:
- Snowflake tools do NOT execute queries immediately
- When you call a Snowflake tool, it will return: {"status": "PENDING_APPROVAL", "query": {"query_id": "...", "tool_name": "...", "sql": "..."}}
- When you receive a PENDING_APPROVAL payload, inform the user that the query needs approval
- Do not fabricate results. Wait until the user approves and the system provides query results.

Think step by step about what the user is asking for, and select the most appropriate tool(s) to answer their question."""
        
        # Define chatbot node
        def chatbot_node(state: MessagesState):
            """Chatbot node that processes user messages and may call tools."""
            messages = state["messages"]
            
            # Prepare messages with system prompt
            full_messages = [SystemMessage(content=system_prompt)] + messages
            
            # Bind tools to LLM
            llm_with_tools = self.primary_llm.bind_tools(self.tools)
            
            # Get LLM response
            response = llm_with_tools.invoke(full_messages)
            
            return {"messages": [response]}
        
        # Define tools node
        def tools_node(state: MessagesState):
            """Tools node that executes tool calls."""
            if not state.get("messages"):
                return {"messages": []}
            
            last_message = state["messages"][-1]
            outputs = []
            
            if not (hasattr(last_message, "tool_calls") and last_message.tool_calls):
                return {"messages": outputs}
            
            for tool_call in last_message.tool_calls:
                tool_name = tool_call["name"]
                tool_args = tool_call["args"]
                
                print(f"🚀 Executing {tool_name}...")
                try:
                    tool = self.tools_by_name.get(tool_name)
                    if tool:
                        tool_result = tool.invoke(tool_args)
                        outputs.append(
                            ToolMessage(
                                content=str(tool_result) if not isinstance(tool_result, str) else tool_result,
                                name=tool_name,
                                tool_call_id=tool_call["id"],
                            )
                        )
                        print(f"✅ Tool {tool_name} executed successfully")
                    else:
                        outputs.append(
                            ToolMessage(
                                content=f"Tool {tool_name} not found",
                                name=tool_name,
                                tool_call_id=tool_call["id"],
                            )
                        )
                except Exception as e:
                    print(f"❌ Tool {tool_name} execution failed: {e}")
                    outputs.append(
                        ToolMessage(
                            content=f"Error: {str(e)}",
                            name=tool_name,
                            tool_call_id=tool_call["id"],
                        )
                    )
            
            return {"messages": outputs}
        
        # Define routing function
        def route_query(state: MessagesState) -> Literal["tools", "end"]:
            """Route based on whether tools need to be called."""
            if not state.get("messages"):
                return "end"
            
            last_message = state["messages"][-1]
            
            # Check if the last message has tool calls
            if hasattr(last_message, "tool_calls") and last_message.tool_calls:
                return "tools"
            
            return "end"
        
        # Build the graph
        graph_builder = StateGraph(MessagesState)
        
        # Add nodes
        graph_builder.add_node("chatbot", chatbot_node)
        graph_builder.add_node("tools", tools_node)
        
        # Add edges
        graph_builder.add_edge(START, "chatbot")
        
        # Add conditional edges from chatbot
        graph_builder.add_conditional_edges(
            "chatbot",
            route_query,
            {
                "tools": "tools",
                "end": END,
            },
        )
        
        # After tools, go back to chatbot
        graph_builder.add_edge("tools", "chatbot")
        
        # Create PostgreSQL checkpointer
        connection = create_postgres_connection()
        if connection is None:
            print("⚠️ Falling back to in-memory storage")
            from langgraph.checkpoint.memory import MemorySaver
            checkpointer = MemorySaver()
        else:
            checkpointer = PostgresSaver(connection)
            checkpointer.setup()  # Create necessary tables
            print("✅ Connected to PostgreSQL for persistent storage")
        
        # Compile the graph with checkpointer
        graph = graph_builder.compile(checkpointer=checkpointer)
        
        return graph
    
    def chat(self, question: str, thread_id: str = "default", config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Process a chat question using the LangGraph agent.
        
        Args:
            question: User's question
            thread_id: Thread ID for conversation persistence (default: "default")
            config: Optional configuration dict
            
        Returns:
            Dictionary with answer, sources, and metadata
        """
        try:
            # Prepare config for thread management
            if config is None:
                config = {"configurable": {"thread_id": thread_id}}
            
            # Get current state to know how many messages existed before this turn
            try:
                current_state = self.graph.get_state(config)
                messages_before = len(current_state.values.get("messages", [])) if current_state.values else 0
            except:
                messages_before = 0
            
            # Invoke the graph with the user message
            result = self.graph.invoke(
                {"messages": [HumanMessage(content=question)]},
                config=config
            )
            
            # Extract the final response
            final_message = result["messages"][-1]
            answer = final_message.content if hasattr(final_message, "content") else str(final_message)
            
            # Extract sources and tool usage from messages
            sources = []
            warehouse_tools_used = []
            rag_tools_used = []
            pending_queries: List[Dict[str, Any]] = []
            
            # Only process messages from THIS turn (messages added after messages_before)
            messages_this_turn = result["messages"][messages_before:] if messages_before > 0 else result["messages"]
            
            # Process messages from this turn to extract tool usage and sources
            for msg in messages_this_turn:
                # Check for tool calls
                if hasattr(msg, "tool_calls") and msg.tool_calls:
                    for tool_call in msg.tool_calls:
                        tool_name = tool_call.get("name", "")
                        # Check if it's a warehouse tool
                        for tool in WAREHOUSE_TOOLS:
                            if tool.name == tool_name:
                                warehouse_tools_used.append(tool_name)
                                break
                        # Check if it's a RAG tool
                        if tool_name == "document_search":
                            rag_tools_used.append(tool_name)
                
                # Check for tool messages (results)
                if isinstance(msg, ToolMessage):
                    tool_name = msg.name
                    content = msg.content
                    
                    # Check for document search results
                    if tool_name == "document_search":
                        # Track RAG tool usage
                        if "document_search" not in rag_tools_used:
                            rag_tools_used.append("document_search")
                        try:
                            # Try to parse as JSON array of documents
                            docs = json.loads(content) if isinstance(content, str) else content
                            if isinstance(docs, list):
                                for doc in docs[:3]:  # Top 3 sources
                                    if isinstance(doc, dict):
                                        sources.append({
                                            "content": doc.get("page_content", str(doc))[:200] + "...",
                                            "metadata": doc.get("metadata", {})
                                        })
                        except:
                            pass
                    
                    # Detect HITL pending-approval payloads
                    if isinstance(content, str):
                        pq = self._try_parse_pending_query_payload(content)
                        if pq is not None:
                            pending_queries.append(pq)
            
            # If the agent attempted a warehouse query, we require approval before continuing.
            if pending_queries:
                return {
                    "answer": answer or "I can run a warehouse query to answer that. Please review and approve the SQL.",
                    "sources": sources,
                    "warehouse_tools_used": warehouse_tools_used,
                    "rag_tools_used": rag_tools_used,
                    "pending_queries": pending_queries,
                    "method": "pending_approval"
                }

            return {
                "answer": answer or "I couldn't generate a response.",
                "sources": sources,
                "warehouse_tools_used": warehouse_tools_used,
                "rag_tools_used": rag_tools_used,
                "method": "agent_with_tools"
            }
            
        except Exception as e:
            return {
                "answer": f"I encountered an error: {str(e)}. Please try rephrasing your question.",
                "sources": [],
                "warehouse_tools_used": [],
                "rag_tools_used": [],
                "method": "error"
            }
    
    def load_documents(self, file_paths: List[str]):
        """Load and process documents into the vector store."""
        print("Processing documents...")
        self.doc_processor.process_documents(file_paths, namespace=self.namespace)
        # Refresh vectorstore
        self.vectorstore = self.doc_processor.get_vectorstore(namespace=self.namespace)
        self.retriever = self._create_retriever()
        self.tools = self._create_tools()
        self.tools_by_name = {tool.name: tool for tool in self.tools}
        self.graph = self._create_agent_graph()
        print("✅ Documents loaded and indexed")

    @staticmethod
    def _try_parse_pending_query_payload(payload: str) -> Optional[Dict[str, Any]]:
        """Parse the warehouse tool HITL payload, returning the `query` dict if present."""

        try:
            data = json.loads(payload)
        except Exception:
            return None

        if isinstance(data, dict) and data.get("status") == "PENDING_APPROVAL" and isinstance(data.get("query"), dict):
            return data["query"]

        return None

    def execute_approved_query(self, query_id: str) -> str:
        """Execute a previously prepared query after the user approves."""

        return warehouse_tools.execute_pending_query(query_id)

    def cancel_query(self, query_id: str) -> bool:
        """Cancel a pending query after the user rejects it."""

        return warehouse_tools.cancel_pending_query(query_id)

    def summarize_warehouse_results(self, *, question: str, executed: List[Dict[str, Any]]) -> str:
        """Turn executed SQL + JSON results into a user-friendly answer.

        This intentionally bypasses the tool-using agent to avoid triggering more queries.
        """

        summary_prompt = ChatPromptTemplate.from_messages(
            [
                (
                    "system",
                    """You are a financial data assistant. You are given the user's question, plus executed warehouse SQL and their JSON results.

Write a clear answer based ONLY on the provided results.
- If results are empty / a message string, say so.
- If results are JSON arrays, summarize key rows and compute simple aggregates when helpful.
- Mention that the data came from the warehouse query tools.
- Do not claim you executed anything beyond what is provided.
- When being asked about definition, what is ..., prioritize to use RAG Tool to search for documents first
""",
                ),
                (
                    "human",
                    "User question:\n{question}\n\nExecuted queries + results (JSON):\n{executed}",
                ),
            ]
        )

        executed_str = json.dumps(executed, indent=2, ensure_ascii=False)
        msgs = summary_prompt.format_messages(question=question, executed=executed_str)

        # primary_llm may stream to stdout (consistent with the rest of the CLI experience)
        response = self.primary_llm.invoke(msgs)
        return getattr(response, "content", str(response))


def main():
    """Main CLI interface for the chatbot."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Advanced RAG Chatbot")
    parser.add_argument("--model", default="gpt-3.5-turbo", help="Primary LLM model")
    parser.add_argument("--secondary-model", help="Secondary LLM model")
    parser.add_argument("--index", help="Pinecone index name")
    parser.add_argument("--namespace", help="Pinecone namespace")
    parser.add_argument("--load-docs", nargs="+", help="Load documents (PDFs, TXT files)")
    parser.add_argument("--no-hybrid", action="store_true", help="Disable hybrid search")
    parser.add_argument("--no-compression", action="store_true", help="Disable document compression")
    
    args = parser.parse_args()
    
    # Initialize chatbot
    print("🤖 Initializing Advanced RAG Chatbot...")
    chatbot = AdvancedRAGChatbot(
        primary_model=args.model,
        secondary_model=args.secondary_model,
        use_hybrid_search=not args.no_hybrid,
        use_compression=not args.no_compression,
        pinecone_index_name=args.index,
        namespace=args.namespace
    )
    
    # Load documents if provided
    if args.load_docs:
        chatbot.load_documents(args.load_docs)
    
    # Interactive chat loop
    print("\n" + "="*60)
    print("Chatbot Ready! Type 'quit' or 'exit' to end the conversation.")
    print("Type 'load <file_path>' to load additional documents.")
    print("="*60 + "\n")
    
    while True:
        try:
            question = input("\nYou: ").strip()
            
            if not question:
                continue
            
            if question.lower() in ["quit", "exit", "q"]:
                print("Goodbye!")
                break
            
            if question.lower().startswith("load "):
                file_path = question[5:].strip()
                try:
                    chatbot.load_documents([file_path])
                    print(f"✅ Loaded document: {file_path}")
                except Exception as e:
                    print(f"❌ Error loading document: {e}")
                continue
            
            print("\nBot: ", end="", flush=True)
            result = chatbot.chat(question)

            # Human-in-the-loop approval for warehouse SQL
            if result.get("method") == "pending_approval" and result.get("pending_queries"):
                pending_queries = result["pending_queries"]
                print()  # ensure newline after any streaming

                executed_payloads: List[Dict[str, Any]] = []
                for pq in pending_queries:
                    query_id = pq.get("query_id")
                    tool_name = pq.get("tool_name")
                    sql = pq.get("sql")
                    if not query_id or not sql:
                        continue

                    print("\n" + "=" * 60)
                    print(f"🧾 Proposed warehouse query ({tool_name})")
                    print("-" * 60)
                    print(sql.strip())
                    print("=" * 60)

                    approve = input("Execute this query? [y/N]: ").strip().lower() in {"y", "yes"}
                    if not approve:
                        chatbot.cancel_query(query_id)
                        print("❌ Cancelled. Query was not executed.")
                        continue

                    print("✅ Approved. Executing query...", flush=True)
                    query_result = chatbot.execute_approved_query(query_id)
                    executed_payloads.append(
                        {
                            "query_id": query_id,
                            "tool_name": tool_name,
                            "sql": sql,
                            "result": query_result,
                        }
                    )

                # If we executed at least one query, summarize results into a final answer
                if executed_payloads:
                    print("\nBot (final): ", end="", flush=True)
                    final_answer = chatbot.summarize_warehouse_results(question=question, executed=executed_payloads)
                    print(final_answer)

                print("\n" + "-" * 60)
                print("-" * 60)
                continue
            
            print()  # New line after streaming
            print("\n" + "-"*60)
            if result.get("sources"):
                print(f"\n📚 Document Sources ({len(result['sources'])}):")
                for i, source in enumerate(result["sources"][:3], 1):
                    print(f"  {i}. {source['content']}")
            if result.get("warehouse_tools_used"):
                print(f"\n💾 Warehouse Tools Used:")
                for tool_name in result["warehouse_tools_used"]:
                    print(f"  - {tool_name}")
            print("-"*60)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")


if __name__ == "__main__":
    main()
