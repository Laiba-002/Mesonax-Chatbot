"""
Manufacturing Knowledge Graph - Interactive Chatbot Interface
Streamlit Application
"""

from config import NEO4J_CONFIG, OPENAI_CONFIG
from schema import SCHEMA
from migrator import DataMigrator
from query_engine import AIQueryEngine
from jwt_auth import (
    initialize_auth_session,
    authenticate_from_url,
    get_user_plantCode,
    get_user_code,
    get_user_name,
    logout_user
)
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import html
import re
from typing import Dict, List, Any, Optional
from neo4j import GraphDatabase
import openai
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import JWT authentication module (local to KG folder)

# Import existing modules


# Page Configuration
st.set_page_config(
    page_title="MESONEX",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize authentication session
initialize_auth_session()

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        padding: 1rem 0;
        border-bottom: 3px solid #1f77b4;
        margin-bottom: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    
    .chat-message {
        padding: 1.2rem;
        border-radius: 12px;
        margin: 0.8rem 0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        animation: slideIn 0.3s ease-out;
    }
    
    @keyframes slideIn {
        from {
            opacity: 0;
            transform: translateY(10px);
        }
        to {
            opacity: 1;
            transform: translateY(0);
        }
    }
    
    .user-message {
        background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%);
        border-left: 4px solid #1976d2;
        margin-left: auto;
        margin-right: 0;
        max-width: 85%;
    }
    
    .assistant-message {
        background: linear-gradient(135deg, #f5f5f5 0%, #eeeeee 100%);
        border-left: 4px solid #388e3c;
        margin-left: 0;
        margin-right: auto;
        max-width: 85%;
    }
    
    .query-box {
        background-color: #263238;
        color: #aed581;
        padding: 1rem;
        border-radius: 5px;
        font-family: 'Courier New', monospace;
        font-size: 0.9rem;
        overflow-x: auto;
        margin: 1rem 0;
    }
    
    .stButton>button {
        width: 100%;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem;
        font-weight: bold;
        border-radius: 5px;
    }
    
    .stButton>button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    .mesonex-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 1.5rem;
        padding-bottom: 1rem;
        border-bottom: 2px solid rgba(102, 126, 234, 0.3);
    }
    
    .mesonex-icon {
        font-size: 2rem;
        animation: iconBounce 2s ease-in-out infinite;
    }
    
    .mesonex-text {
        display: flex;
        flex-direction: column;
    }
    
    .mesonex-brand {
        font-size: 1.3rem;
        font-weight: 900;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: 2px;
        margin: 0;
        line-height: 1.2;
        animation: textGlow 2s ease-in-out infinite;
    }
    
    @keyframes textGlow {
        0%, 100% {
            opacity: 1;
        }
        50% {
            opacity: 0.85;
        }
    }
    
    .mesonex-tagline {
        font-size: 0.65rem;
        color: #667eea;
        letter-spacing: 1px;
        margin: 2px 0 0 0;
        font-weight: 600;
        text-transform: uppercase;
        opacity: 0.8;
    }
    
    @keyframes iconBounce {
        0%, 100% {
            transform: translateY(0);
        }
        50% {
            transform: translateY(-6px);
        }
    }
</style>
""", unsafe_allow_html=True)


class ManufacturingChatbot:
    """Main chatbot class integrating Neo4j and OpenAI"""

    def __init__(self):
        self.driver = None
        self.query_engine = AIQueryEngine()
        self.connected = False

    def connect(self) -> bool:
        """Connect to Neo4j"""
        try:
            self.driver = GraphDatabase.driver(
                NEO4J_CONFIG['uri'],
                auth=(NEO4J_CONFIG['username'], NEO4J_CONFIG['password'])
            )
            with self.driver.session() as session:
                session.run("RETURN 1")
            self.connected = True
            return True
        except Exception as e:
            st.error(f"❌ Neo4j Connection Error: {e}")
            return False

    def execute_query(self, cypher: str, parameters: Dict = None) -> Dict[str, Any]:
        """Execute Cypher query with plant-specific filtering"""
        try:
            # Add plant code filtering to parameters
            if parameters is None:
                parameters = {}

            # Get authenticated user's plant code
            plant_code = get_user_plantCode()
            if plant_code:
                parameters['plant_code'] = plant_code
                logger.info(f"🔒 Filtering results for plant: {plant_code}")

            with self.driver.session() as session:
                result = session.run(cypher, parameters)
                records = [dict(record) for record in result]
                return {
                    'success': True,
                    'data': records,
                    'error': None
                }
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Query Error: {error_msg}")

            # Categorize the error for better user messaging
            if "property" in error_msg.lower() and "does not exist" in error_msg.lower():
                user_friendly_msg = "The generated query contains invalid property names. The AI may have misunderstood your question."
            elif "variable" in error_msg.lower() and "not defined" in error_msg.lower():
                user_friendly_msg = "The generated query has a syntax error with variable references."
            elif "cannot be parsed" in error_msg.lower():
                user_friendly_msg = "The generated query has incorrect date/time handling."
            elif "syntax error" in error_msg.lower():
                user_friendly_msg = "The generated query has a syntax error."
            else:
                user_friendly_msg = "The generated query could not be executed due to an error."

            return {
                'success': False,
                'data': [],
                'error': error_msg,
                'user_friendly_error': user_friendly_msg
            }

    def ask(self, question: str) -> Dict[str, Any]:
        """Process natural language question with error-handling retry loop"""
        from config import MAX_RETRY_ATTEMPTS

        # Get user's plant code for filtering
        user_plant_code = get_user_plantCode()

        # Initialize retry variables
        attempt = 1
        query_result = None
        execution_result = None
        retry_log = []  # Track all attempts for debugging

        # Generate initial Cypher query using AI with plant-specific filtering
        query_result = self.query_engine.generate_cypher(
            question, plant_code=user_plant_code)

        if not query_result['success']:
            return {
                'success': False,
                'error': query_result.get('error', 'Failed to generate query'),
                'question': question
            }

        # Error-handling retry loop
        while attempt <= MAX_RETRY_ATTEMPTS:
            logger.info(
                f"Query execution attempt {attempt}/{MAX_RETRY_ATTEMPTS}")

            # Execute query
            execution_result = self.execute_query(query_result['cypher'])

            # Log this attempt
            retry_log.append({
                'attempt': attempt,
                'cypher': query_result['cypher'],
                'success': execution_result['success'],
                'error': execution_result.get('error') if not execution_result['success'] else None
            })

            # Check if query execution succeeded
            if execution_result['success']:
                logger.info(f"✅ Query succeeded on attempt {attempt}")

                results = execution_result['data']

                # Generate natural language summary only if query succeeded
                summary = self.query_engine.explain_results(
                    query_result['cypher'],
                    results
                )

                response = {
                    'success': True,
                    'question': question,
                    'cypher': query_result['cypher'],
                    'explanation': query_result['explanation'],
                    'results': results,
                    'count': len(results),
                    'summary': summary,
                    'timestamp': datetime.now().isoformat(),
                    'plant_code': user_plant_code
                }

                # Add retry info if it took multiple attempts
                if attempt > 1:
                    response['retry_info'] = {
                        'total_attempts': attempt,
                        'error_corrected': True,
                        'previous_errors': [log['error'] for log in retry_log[:-1]]
                    }
                    logger.info(
                        f"Query succeeded after {attempt} attempts with error correction")

                return response

            # Query execution failed
            logger.warning(
                f"❌ Query failed on attempt {attempt}: {execution_result['error']}")

            # Check if we've reached max retries
            if attempt >= MAX_RETRY_ATTEMPTS:
                logger.error(
                    f"Max retry attempts ({MAX_RETRY_ATTEMPTS}) reached. Query failed.")
                return {
                    'success': False,
                    'question': question,
                    'cypher': query_result['cypher'],
                    'error': execution_result['error'],
                    'user_friendly_error': execution_result.get('user_friendly_error', 'Query execution failed'),
                    'summary': f'I attempted to answer your question {MAX_RETRY_ATTEMPTS} times but encountered technical difficulties. The system tried to learn from the errors but could not generate a working query. Could you please rephrase your question or try asking about something else? You can also check the example questions in the sidebar for guidance.',
                    'timestamp': datetime.now().isoformat(),
                    'query_error': True,
                    'retry_info': {
                        'total_attempts': attempt,
                        'max_attempts_reached': True,
                        'all_errors': [log['error'] for log in retry_log]
                    }
                }

            # Feed error back to AI for query correction
            logger.info(
                f"🔄 Feeding error back to AI for correction (attempt {attempt + 1})...")

            previous_cypher = query_result['cypher']

            query_result = self.query_engine.generate_cypher_with_error_correction(
                natural_query=question,
                failed_cypher=previous_cypher,
                error_message=execution_result['error'],
                attempt_number=attempt + 1,
                plant_code=user_plant_code
            )

            if not query_result['success']:
                # AI failed to generate corrected query
                logger.error(
                    f"AI failed to generate corrected query on attempt {attempt + 1}")
                return {
                    'success': False,
                    'question': question,
                    'error': query_result.get('error', 'Failed to generate corrected query'),
                    'summary': 'I had trouble generating a corrected query. Could you please rephrase your question?',
                    'timestamp': datetime.now().isoformat(),
                    'retry_info': {
                        'total_attempts': attempt,
                        'correction_failed': True
                    }
                }

            # Check if AI generated the same query (no actual correction)
            new_cypher = query_result['cypher'].strip()
            if new_cypher == previous_cypher.strip():
                logger.warning(
                    f"⚠️ AI generated the same query on attempt {attempt + 1}. Providing stronger feedback...")

                # Provide even stronger feedback
                stronger_correction = self.query_engine.generate_cypher_with_error_correction(
                    natural_query=question,
                    failed_cypher=previous_cypher,
                    error_message=f"CRITICAL: You generated the EXACT SAME query that failed. The error was: {execution_result['error']}. You MUST generate a DIFFERENT query that fixes this error. The failed query was: {previous_cypher}",
                    attempt_number=attempt + 1,
                    plant_code=user_plant_code
                )

                if stronger_correction['success']:
                    query_result = stronger_correction
                    logger.info(
                        "✅ Generated different query after stronger feedback")

            # Increment attempt counter for next iteration
            attempt += 1

        # Should not reach here, but just in case
        return {
            'success': False,
            'question': question,
            'error': 'Unexpected error in retry loop',
            'summary': 'An unexpected error occurred. Please try again.',
            'timestamp': datetime.now().isoformat()
        }

    def close(self):
        """Close connections"""
        if self.driver:
            self.driver.close()


class DataVisualizer:
    """Generate intelligent visualizations based on query results"""

    @staticmethod
    def extract_metrics(results: List[Dict]) -> Dict[str, Any]:
        """Extract key metrics from results"""
        if not results:
            return {}

        metrics = {
            'total_records': len(results),
            'unique_keys': {},
            'numeric_aggregates': {}
        }

        # Analyze first result structure
        sample = results[0]

        for key, value in sample.items():
            # Count unique values
            unique_values = set(str(r.get(key))
                                for r in results if r.get(key) is not None)
            metrics['unique_keys'][key] = len(unique_values)

            # Aggregate numeric values
            if isinstance(value, (int, float)):
                values = [r.get(key) for r in results if isinstance(
                    r.get(key), (int, float))]
                if values:
                    metrics['numeric_aggregates'][key] = {
                        'sum': sum(values),
                        'avg': sum(values) / len(values),
                        'min': min(values),
                        'max': max(values)
                    }

        return metrics

    @staticmethod
    def create_charts(df: pd.DataFrame) -> List[go.Figure]:
        """Create appropriate charts based on data"""
        charts = []

        if df.empty:
            return charts

        # Detect chart types based on columns
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        text_cols = df.select_dtypes(include=['object']).columns.tolist()

        # Filter out columns with unhashable types (lists, dicts, etc.)
        hashable_text_cols = []
        for col in text_cols:
            try:
                # Try to check if column contains hashable values
                sample = df[col].dropna(
                ).iloc[0] if not df[col].dropna().empty else None
                if sample is None or not isinstance(sample, (list, dict, set)):
                    # Verify by attempting a value_counts operation
                    _ = df[col].value_counts()
                    hashable_text_cols.append(col)
            except (TypeError, AttributeError):
                # Skip columns with unhashable types
                continue

        text_cols = hashable_text_cols

        # 1. Bar chart for categorical counts
        if text_cols:
            for col in text_cols[:2]:  # First 2 text columns
                try:
                    value_counts = df[col].value_counts().head(10)
                    if len(value_counts) > 1:
                        fig = px.bar(
                            x=value_counts.index,
                            y=value_counts.values,
                            title=f"Distribution of {col}",
                            labels={'x': col, 'y': 'Count'},
                            color=value_counts.values,
                            color_continuous_scale='viridis'
                        )
                        fig.update_layout(showlegend=False, height=400)
                        charts.append(fig)
                except (TypeError, ValueError):
                    # Skip if column has unhashable types
                    continue

        # 2. Line/Bar charts for numeric data
        if numeric_cols and text_cols:
            for num_col in numeric_cols[:2]:
                for text_col in text_cols[:1]:
                    try:
                        # Aggregate by text column
                        grouped = df.groupby(text_col)[num_col].sum(
                        ).sort_values(ascending=False).head(10)
                        if len(grouped) > 0:
                            fig = px.bar(
                                x=grouped.index,
                                y=grouped.values,
                                title=f"{num_col} by {text_col}",
                                labels={'x': text_col, 'y': num_col},
                                color=grouped.values,
                                color_continuous_scale='blues'
                            )
                            fig.update_layout(showlegend=False, height=400)
                            charts.append(fig)
                    except (TypeError, ValueError):
                        # Skip if column has unhashable types
                        continue

        # 3. Pie chart for proportions
        if text_cols and len(df) > 1:
            col = text_cols[0]
            try:
                value_counts = df[col].value_counts().head(8)
                if len(value_counts) > 1:
                    fig = px.pie(
                        values=value_counts.values,
                        names=value_counts.index,
                        title=f"Proportion of {col}",
                        hole=0.4
                    )
                    fig.update_layout(height=400)
                    charts.append(fig)
            except (TypeError, ValueError):
                # Skip if column has unhashable types
                pass

        # 4. Time series if datetime columns exist
        date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
        if date_cols and numeric_cols:
            date_col = date_cols[0]
            num_col = numeric_cols[0]
            df_sorted = df.sort_values(date_col)
            fig = px.line(
                df_sorted,
                x=date_col,
                y=num_col,
                title=f"{num_col} Over Time",
                markers=True
            )
            fig.update_layout(height=400)
            charts.append(fig)

        return charts[:4]  # Return max 4 charts


def initialize_session_state():
    """Initialize Streamlit session state and auto-connect to database"""
    if 'chatbot' not in st.session_state:
        st.session_state.chatbot = ManufacturingChatbot()
        # Auto-connect to database on first initialization
        with st.spinner("🔌 Connecting to Neo4j database..."):
            if st.session_state.chatbot.connect():
                st.session_state.connected = True
            else:
                st.session_state.connected = False

    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []

    if 'connected' not in st.session_state:
        st.session_state.connected = False


def render_sidebar():
    """Render sidebar with configuration and examples"""
    with st.sidebar:
        # Header logo image
        st.image(
            "logo.jpeg",
            caption=None,
            use_column_width=True
        )
        st.markdown("<div style='margin-top: -20px;'></div>",
                    unsafe_allow_html=True)

        # st.markdown("---")

        # # User Authentication Info
        # st.markdown("### 👤 User Profile")
        # st.markdown(f"**👤 Name:** {get_user_name() or 'N/A'}")
        # st.markdown(f"**🏭 Plant:** {get_user_plantCode() or 'N/A'}")
        # st.markdown(f"**🔑 Code:** {get_user_code() or 'N/A'}")

        # if st.button("🚪 Logout", use_container_width=True):
        #     logout_user()
        #     st.rerun()

        # st.markdown("---")

        # Connection Status
        if st.session_state.connected:
            pass  # Connected silently
        else:
            st.error("❌ Database Connection Failed")
            if st.button("🔄 Retry Connection", use_container_width=True):
                with st.spinner("Reconnecting..."):
                    if st.session_state.chatbot.connect():
                        st.session_state.connected = True
                        st.rerun()
                    else:
                        st.error("Failed to connect. Check Neo4j is running.")

        # st.markdown("---")
        st.markdown(" ")
        # Example Questions
        st.markdown("### 💡 Example Questions")
        examples = [
            "Show me all plants in the system",
            "List machines on line L001",
            "What are the active production plans?",
            "Show production output for machine M001",
            "How many operations are there?",
            "Show me the plant hierarchy",
            "List all products being manufactured",
            "Show material consumption for operations"
        ]

        for example in examples:
            if st.button(example, key=f"ex_{example[:20]}"):
                st.session_state.pending_question = example

        st.markdown("---")

        # Clear History
        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.session_state.chatbot.query_engine.clear_history()
            st.success("✅ Chat history cleared")
            st.rerun()

        # # Conversation Context Status
        # st.markdown("---")
        # st.markdown("### 💬 Conversation Context")
        # conv_count = st.session_state.chatbot.query_engine.get_conversation_count()
        # if conv_count > 0:
        #     st.info(f"📝 **{conv_count}** conversation{'s' if conv_count != 1 else ''} in memory\n\n✨ You can ask follow-up questions!")
        #     st.caption(f"Last {min(conv_count, 6)} conversations available for context")
        # else:
        #     st.caption("No conversation history yet. Start chatting!")

        # Migration Tool
        st.markdown("---")
        st.markdown("### 🔄 Data Migration")
        if st.button("Migrate Data from SQL"):
            st.session_state.show_migration = True


def render_metrics(metrics: Dict):
    """Render key metrics in cards"""
    if not metrics:
        return

    st.markdown("### 📊 Key Metrics")

    cols = st.columns(4)
    # Total Records
    with cols[0]:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Records</div>
            <div class="metric-value">{metrics.get('total_records', 0)}</div>
        </div>
        """, unsafe_allow_html=True)

    # Display unique counts
    unique_keys = metrics.get('unique_keys', {})
    for idx, (key, count) in enumerate(list(unique_keys.items())[:3], 1):
        with cols[idx]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">Unique {key}</div>
                <div class="metric-value">{count}</div>
            </div>
            """, unsafe_allow_html=True)


def render_chat_message(role: str, content: str):
    """Render a chat message with professional styling"""
    css_class = "user-message" if role == "user" else "assistant-message"
    icon = "👤 You" if role == "user" else "🤖 Assistant"

    # Get user name for display if available
    user_display = get_user_name() if role == "user" else "MESONEX AI"

    # Strip HTML tags to prevent them from being rendered as visible text
    cleaned_content = re.sub(r'<[^>]+>', '', str(content))

    st.markdown(f"""
    <div class="chat-message {css_class}">
        <div style="display: flex; align-items: center; margin-bottom: 8px; gap: 8px;">
            <span style="font-size: 1.2em;">{icon.split()[0]}</span>
            <strong style="color: {'#1976d2' if role == 'user' else '#388e3c'}; font-size: 0.95em;">
                {user_display}
            </strong>
        </div>
        <div style="margin-left: 32px; line-height: 1.6; font-size: 0.95em; color: #333;">
            {cleaned_content}
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_query_result(response: Dict, message_index: int = 0):
    """Render comprehensive query results"""
    if not response['success']:
        # Check if this is a query execution error vs AI generation error
        if response.get('query_error', False):
            # Show technical details in a minimal expander for advanced users
            with st.expander("🔧 View Technical Details", expanded=False):
                st.caption("Generated Query:")
                st.code(response.get('cypher', 'No query generated'),
                        language='cypher')
                st.caption("Error Details:")
                st.text(response.get('error', 'Unknown error'))
        else:
            # General error - minimal display
            st.warning(
                f"⚠️ {response.get('error', 'An error occurred. Please try again.')}")
        return

    results = response['results']

    # Natural Language Summary
    # st.markdown("### 💬 Summary")
    # st.info(response.get('summary', 'Query executed successfully'))

    # # Show Cypher Query
    with st.expander("🔍 View Generated Query", expanded=False):
        # st.markdown('<div class="query-box">', unsafe_allow_html=True)
        st.code(response['cypher'], language='cypher')
        st.markdown('</div>', unsafe_allow_html=True)
    # if response.get('explanation'):
    # st.caption(response['explanation'])

    # if not results:
    #     st.warning("📭 No results found")
    #     return

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # # Extract and display metrics
    # metrics = DataVisualizer.extract_metrics(results)
    # render_metrics(metrics)

    # Display data table
    # st.markdown("### 📋 Data Table")
    st.markdown(" ")

    # Check if dataframe is empty
    if df.empty:
        st.info("📭 No data found for the given criteria.")
    else:
        st.dataframe(df, use_container_width=True, height=400)

    # Download option (only show if data exists)
    if not df.empty:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download Results as CSV",
            data=csv,
            file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            key=f"download_csv_{message_index}"
        )
    # Generate and display charts
    # st.markdown("### 📈 Visualizations")
    charts = DataVisualizer.create_charts(df)

    if charts:
        # Display charts in columns
        if len(charts) == 1:
            st.plotly_chart(charts[0], use_container_width=True)
        elif len(charts) == 2:
            col1, col2 = st.columns(2)
            with col1:
                st.plotly_chart(charts[0], use_container_width=True)
            with col2:
                st.plotly_chart(charts[1], use_container_width=True)
        else:
            col1, col2 = st.columns(2)
            for idx, chart in enumerate(charts):
                with col1 if idx % 2 == 0 else col2:
                    st.plotly_chart(chart, use_container_width=True)
    else:
        st.info("No visualizations available for this data")

    # Display data table
    # st.markdown("### 📋 Data Table")
    # st.dataframe(df, use_container_width=True, height=400)

    # # Download option
    # csv = df.to_csv(index=False)
    # st.download_button(
    #     label="📥 Download Results as CSV",
    #     data=csv,
    #     file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
    #     mime="text/csv",
    #     key=f"download_csv_{message_index}"
    # )


def render_migration_dialog():
    """Render data migration interface"""
    st.markdown("## 🔄 Data Migration Tool")
    st.info("This will migrate data from SQL Server to Neo4j Knowledge Graph")

    col1, col2 = st.columns([1, 1])

    with col1:
        clear_existing = st.checkbox("Clear existing Neo4j data", value=True)

    with col2:
        if st.button("Start Migration", type="primary"):
            with st.spinner("Migrating data... This may take a few minutes"):
                migrator = DataMigrator()

                # Create progress placeholders
                progress_bar = st.progress(0)
                status_text = st.empty()

                try:
                    # Connect
                    status_text.text("Connecting to databases...")
                    progress_bar.progress(10)

                    if not migrator.connect_sql():
                        st.error("Failed to connect to SQL Server")
                        return

                    if not migrator.connect_neo4j():
                        st.error("Failed to connect to Neo4j")
                        return

                    progress_bar.progress(20)

                    # Clear if requested
                    if clear_existing:
                        status_text.text("Clearing existing data...")
                        migrator.clear_database()
                        progress_bar.progress(30)

                    # Create constraints
                    status_text.text("Creating constraints...")
                    migrator.create_constraints()
                    progress_bar.progress(40)

                    # Create indexes
                    status_text.text("Creating indexes...")
                    migrator.create_indexes()
                    progress_bar.progress(50)

                    # Migrate nodes
                    status_text.text("Migrating nodes...")
                    migrator.migrate_nodes()
                    progress_bar.progress(75)

                    # Create relationships
                    status_text.text("Creating relationships...")
                    migrator.create_relationships()
                    progress_bar.progress(90)

                    # Verify
                    status_text.text("Verifying migration...")
                    migrator.verify_migration()
                    progress_bar.progress(100)

                    st.success("✅ Migration completed successfully!")

                    migrator.close()

                except Exception as e:
                    st.error(f"Migration failed: {e}")
                    migrator.close()

    if st.button("Close"):
        st.session_state.show_migration = False
        st.rerun()


def main():
    """Main application"""
    initialize_session_state()

    # ============================================
    # JWT AUTHENTICATION CHECK
    # ============================================

    if not st.session_state.get("authenticated", False):
        authenticate_from_url(verify_signature=False)

        if not st.session_state.get("authenticated", False):
            st.error("🔒 **Authentication Required**")
            st.info(
                "Please provide a valid JWT token in the URL: ?token=your_jwt_token_here")
            st.stop()

    # Display authenticated user info in sidebar
    # with st.sidebar:
        # st.markdown("---")
        # st.markdown("### 👤 User Information")
        # st.markdown(f"**User:** {get_user_name()}")
        # st.markdown(f"**Plant:** {get_user_plantCode()}")
        # st.markdown(f"**Code:** {get_user_code()}")

        # if st.button("🚪 Logout"):
        #     logout_user()
        #     st.rerun()
        # st.markdown("---")

    # Header
    # st.markdown('<div class="main-header"> MESONEX</div>',
    #             unsafe_allow_html=True)

    # Display authenticated user's plant info
    # col1, col2, col3 = st.columns(3)
    # with col1:
    #     st.metric("🏭 Plant Code", get_user_plantCode() or "N/A")
    # with col2:
    #     st.metric("👤 User", get_user_name() or "N/A")
    # with col3:
    #     st.metric("🔑 User Code", get_user_code() or "N/A")

    # st.markdown("---")

    # Sidebar
    render_sidebar()

    # Check for migration dialog
    if hasattr(st.session_state, 'show_migration') and st.session_state.show_migration:
        render_migration_dialog()
        return

    # Main chat interface
    if not st.session_state.connected:
        st.error("⚠️ Database connection failed. Please check:")
        st.info("""
        1. Neo4j database is running
        2. Connection details in config.py are correct
        3. Use 'Retry Connection' button in sidebar to reconnect
        """)
        return

    # Display chat history
    for idx, message in enumerate(st.session_state.chat_history):
        if message['role'] == 'user':
            render_chat_message('user', message['content'])
        else:
            render_chat_message('assistant', message.get('summary', ''))
            if message.get('show_details', False):
                render_query_result(message, message_index=idx)

    # Handle pending question from sidebar
    if hasattr(st.session_state, 'pending_question'):
        question = st.session_state.pending_question
        delattr(st.session_state, 'pending_question')

        # Add to history
        st.session_state.chat_history.append({
            'role': 'user',
            'content': question
        })

        # Process question
        with st.spinner(" Thinking..."):
            response = st.session_state.chatbot.ask(question)
            response['show_details'] = True
            st.session_state.chat_history.append({
                'role': 'assistant',
                **response
            })

        st.rerun()

    # Chat input
    question = st.chat_input(
        "Ask me anything about your manufacturing data...")

    if question:
        # Add user message to history
        st.session_state.chat_history.append({
            'role': 'user',
            'content': question
        })

        # Process question
        with st.spinner("🤔 Analyzing your question..."):
            response = st.session_state.chatbot.ask(question)
            response['show_details'] = True
            st.session_state.chat_history.append({
                'role': 'assistant',
                **response
            })

        st.rerun()


if __name__ == "__main__":
    main()
