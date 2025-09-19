import base64
import json
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from snowflake.cortex import complete
from snowflake.snowpark.context import get_active_session

# Page configuration
st.set_page_config(
    page_title="MusicFlow - Schema Registry",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS for better styling
st.markdown(
    """
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(90deg, #FF6B6B, #4ECDC4, #45B7D1);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 2rem;
    }
    .schema-card {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 5px solid #4ECDC4;
        margin-bottom: 1rem;
    }
    .source-schema {
        background-color: #fff3cd;
        border-left: 5px solid #ffc107;
    }
    .target-schema {
        background-color: #d1ecf1;
        border-left: 5px solid #17a2b8;
    }
    .field-mapping {
        background-color: #d4edda;
        padding: 0.5rem;
        border-radius: 5px;
        border-left: 3px solid #28a745;
        margin: 0.25rem 0;
    }
    .confidence-high { color: #28a745; font-weight: bold; }
    .confidence-medium { color: #ffc107; font-weight: bold; }
    .confidence-low { color: #dc3545; font-weight: bold; }
    .status-ready { color: #28a745; font-weight: bold; }
    .status-processing { color: #ffc107; font-weight: bold; }
    .status-error { color: #dc3545; font-weight: bold; }
    .connection-success {
        background-color: #d1ecf1;
        padding: 1rem;
        border-radius: 5px;
        border-left: 5px solid #17a2b8;
        margin-bottom: 1rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


# Initialize Snowflake connection
session = get_active_session()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_schema_registry_data():
    """Load schema registry data from Snowflake"""
    query = """
    SELECT 
        TABLE_NAME,
        TABLE_NAMESPACE,
        AVRO_SCHEMA,
        SCHEMA_ANALYSIS,
        SCHEMA_VERSION,
        IS_READY,
        STATUS,
        BASELINE_SOURCE,
        LAST_ANALYSIS_SOURCE,
        CREATED_AT,
        UPDATED_AT
    FROM SCHEMA_REGISTRY
    ORDER BY CREATED_AT DESC
    """

    __df = session.sql(query).to_pandas()

    return __df


@st.spinner("Loading Schema Analysis")
def get_schema_analysis(__schema_analysis):
    __out = complete(
        "claude-4-sonnet",
        f"""Convert this schema analysis JSON into clean markdown format. Requirements:
- Do NOT include any main header or title
- Start directly with key metrics using bold labels
- Use emojis for visual appeal: ✅ for direct mappings, 🔄 for semantic mappings, ⚠️ for evolution requirements, 📊 for statistics
- Format field mappings as numbered list under "### 🔗 Field Mapping Analysis" subheader
- Put all field names in backticks (code format): `field_name`
- Highlight important information like evolution requirements in bold and as quote
- Use proper markdown formatting for Streamlit display
- Keep it concise and readable

Example structure:
**Match Status:** ✅ Match Found
**Evolution Required:** ⚠️ Yes

### 🔗 Field Mapping Analysis
**1.** ✅ Direct mapping description
**2.** 🔄 Semantic mapping description
**3.** ⚠️ **Evolution requirement description**{__schema_analysis}
""",
    )
    return __out


def decode_base64_field(field_value):
    """Decode base64 field and return readable string"""
    try:
        if pd.isna(field_value) or field_value == "":
            return None

        decoded_bytes = base64.b64decode(field_value)
        return decoded_bytes.decode("utf-8")
    except:
        # Return original value if decoding fails
        return field_value


def parse_schema_json(schema_str):
    """Parse JSON schema string safely, handling base64 encoding"""
    try:
        if pd.isna(schema_str) or schema_str == "":
            return {}

        # Try to decode from base64 first
        try:
            decoded_bytes = base64.b64decode(schema_str)
            decoded_str = decoded_bytes.decode("utf-8")
            return json.loads(decoded_str)
        except:
            # If base64 decoding fails, try direct JSON parsing
            return json.loads(schema_str)

    except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
        return {}


def get_confidence_style(confidence):
    """Return CSS class based on confidence level"""
    if confidence >= 95:
        return "confidence-high"
    elif confidence >= 85:
        return "confidence-medium"
    else:
        return "confidence-low"


def get_status_style(status):
    """Return CSS class based on status"""
    if status == "READY":
        return "status-ready"
    elif status in ["DRAFT"]:
        return "status-processing"
    else:
        return "status-error"


def create_schema_flow_diagram(schema_df):
    """Create a flow diagram showing schema transformation"""
    if schema_df.empty:
        return go.Figure()

    fig = go.Figure()

    # Get unique tables
    tables = schema_df["TABLE_NAME"].unique()
    y_positions = list(range(len(tables), 0, -1))

    # Add source schema nodes
    for i, table_name in enumerate(tables):
        table_data = schema_df[schema_df["TABLE_NAME"] == table_name].iloc[0]
        status_color = "#28a745" if table_data["STATUS"] == "READY" else "#ffc107"

        fig.add_shape(
            type="rect",
            x0=0,
            y0=y_positions[i] - 0.3,
            x1=2,
            y1=y_positions[i] + 0.3,
            fillcolor=status_color,
            opacity=0.3,
            line=dict(color=status_color, width=2),
        )

        fig.add_annotation(
            x=1,
            y=y_positions[i],
            text=f"<b>{table_name}</b><br>Status: {table_data['STATUS']}<br>Version: {table_data['SCHEMA_VERSION']}",
            showarrow=False,
            font=dict(size=10),
            align="center",
        )

        # Add arrows to unified schema
        fig.add_annotation(
            x=3,
            y=y_positions[i],
            ax=2.1,
            ay=y_positions[i],
            axref="x",
            ayref="y",
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#666666",
        )

    # Add unified schema (right side)
    center_y = (max(y_positions) + min(y_positions)) / 2
    fig.add_shape(
        type="rect",
        x0=4,
        y0=center_y - 0.5,
        x1=6,
        y1=center_y + 0.5,
        fillcolor="#17a2b8",
        opacity=0.3,
        line=dict(color="#17a2b8", width=2),
    )

    fig.add_annotation(
        x=5,
        y=center_y,
        text="<b>Unified Events Schema</b><br>Analytics-Ready Format<br>Standardized Fields",
        showarrow=False,
        font=dict(size=12, color="#17a2b8"),
        align="center",
    )

    # Add arrows to unified schema
    for i in range(len(tables)):
        fig.add_annotation(
            x=3.9,
            y=center_y,
            ax=3.1,
            ay=y_positions[i],
            axref="x",
            ayref="y",
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#666666",
        )

    # Add LLM processing indicator
    fig.add_annotation(
        x=3,
        y=0.5,
        text="🧠 LLM Intelligence<br>Schema Detection & Mapping",
        showarrow=False,
        font=dict(size=11, color="#6f42c1"),
        bgcolor="rgba(111, 66, 193, 0.1)",
        bordercolor="#6f42c1",
        borderwidth=2,
    )

    fig.update_layout(
        title="MusicFlow Schema Transformation Flow",
        xaxis=dict(range=[-0.5, 6.5], showgrid=False, showticklabels=False),
        yaxis=dict(
            range=[0, max(y_positions) + 1], showgrid=False, showticklabels=False
        ),
        height=400,
        showlegend=False,
        plot_bgcolor="white",
    )

    return fig


# Main header
st.markdown(
    '<h1 class="main-header">🎵  MusicFlow Schema Registry</h1>', unsafe_allow_html=True
)

# Connection success indicator
st.markdown(
    """
<div class="connection-success">
    ✅ <strong>Connected to Snowflake</strong> | Schema Registry loaded successfully
</div>
""",
    unsafe_allow_html=True,
)

# Sidebar for navigation
st.sidebar.title("Schema Registry Navigation")
view_mode = st.sidebar.selectbox(
    "View Mode:",
    ["Overview", "Schema Details", "Analysis Results"],
)

# Add refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Load data from Snowflake
try:
    schema_df = load_schema_registry_data()

    if schema_df.empty:
        st.warning("No data found in schema registry. Please ensure data is loaded.")
        st.stop()

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.stop()

if view_mode == "Overview":
    st.header("📋 Schema Registry Overview")

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Schemas", len(schema_df))

    with col2:
        ready_count = len(schema_df[schema_df["STATUS"] == "READY"])
        st.metric("Ready Schemas", ready_count)

    with col3:
        processing_count = len(schema_df[schema_df["STATUS"] == "DRAFT"])
        st.metric("Draft", processing_count)

    with col4:
        latest_version = int(schema_df["SCHEMA_VERSION"].max())
        st.metric("Latest Version", f"{latest_version}")

    st.markdown("---")

    # Schema registry table
    st.subheader("📊 Registered Schemas")

    # Format the dataframe for display
    display_df = schema_df.copy()
    display_df["CREATED_AT"] = pd.to_datetime(display_df["CREATED_AT"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    display_df["UPDATED_AT"] = pd.to_datetime(display_df["UPDATED_AT"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )

    st.dataframe(
        display_df[
            [
                "TABLE_NAME",
                "TABLE_NAMESPACE",
                "SCHEMA_VERSION",
                "STATUS",
                "IS_READY",
                "CREATED_AT",
                "UPDATED_AT",
            ]
        ],
        column_config={
            "TABLE_NAME": "Table Name",
            "TABLE_NAMESPACE": "Namespace",
            "SCHEMA_VERSION": "Version",
            "STATUS": st.column_config.TextColumn("Status"),
            "IS_READY": st.column_config.CheckboxColumn("Ready"),
            "CREATED_AT": "Created",
            "UPDATED_AT": "Updated",
        },
        hide_index=True,
        use_container_width=True,
    )

    # Schema transformation flow
    st.subheader("🔄 Schema Transformation Flow")
    flow_fig = create_schema_flow_diagram(schema_df)
    st.plotly_chart(flow_fig, use_container_width=True)

elif view_mode == "Schema Details":
    st.header("🔍 Schema Details")

    # Schema selector
    table_names = schema_df["TABLE_NAME"].unique()
    selected_table = st.selectbox("Select Schema:", table_names)
    selected_row = schema_df[schema_df["TABLE_NAME"] == selected_table].iloc[0]

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("📝 Schema Information")
        status_class = get_status_style(selected_row["STATUS"])

        st.markdown(
            f"""
        <div class="schema-card source-schema">
            <strong>Table Name:</strong> {selected_row["TABLE_NAME"]}<br>
            <strong>Namespace:</strong> {selected_row["TABLE_NAMESPACE"]}<br>
            <strong>Version:</strong> v{selected_row["SCHEMA_VERSION"]}<br>
            <strong>Status:</strong> <span class="{status_class}">{selected_row["STATUS"]}</span><br>
            <strong>Is Ready:</strong> {"✅ Yes" if selected_row["IS_READY"] else "❌ No"}<br>
            <strong>Created:</strong> {selected_row["CREATED_AT"]}<br>
            <strong>Updated:</strong> {selected_row["UPDATED_AT"]}
        </div>
        """,
            unsafe_allow_html=True,
        )

        # Show AVRO schema if available
        if pd.notna(selected_row["AVRO_SCHEMA"]) and selected_row["AVRO_SCHEMA"]:
            st.subheader("📋 AVRO Schema")
            avro_schema = parse_schema_json(selected_row["AVRO_SCHEMA"])
            if avro_schema:
                st.json(avro_schema, expanded=False)
            else:
                # Try to decode as base64 and display as text
                decoded_schema = decode_base64_field(selected_row["AVRO_SCHEMA"])
                if decoded_schema and decoded_schema != selected_row["AVRO_SCHEMA"]:
                    st.code(decoded_schema, language="json")
                else:
                    st.code(selected_row["AVRO_SCHEMA"], language="json")

    with col2:
        st.spinner()
        st.subheader("🔍 Schema Analysis")
        if (
            pd.notna(selected_row["SCHEMA_ANALYSIS"])
            and selected_row["SCHEMA_ANALYSIS"]
        ):
            schema_analysis = parse_schema_json(selected_row["SCHEMA_ANALYSIS"])
            if schema_analysis:
                out = get_schema_analysis(schema_analysis)
                st.markdown(out)
            else:
                st.text_area(
                    "Analysis Results", selected_row["SCHEMA_ANALYSIS"], height=400
                )
        else:
            st.info("No schema analysis available for this table.")

        # Source information
        st.subheader("📄 Source Information")
        st.markdown(f"""
        **Baseline Source:** {selected_row["BASELINE_SOURCE"] or "N/A"}  
        **Last Analysis Source:** {selected_row["LAST_ANALYSIS_SOURCE"] or "N/A"}
        """)

elif view_mode == "Analysis Results":
    st.header("🧠 Schema Analysis Results")

    # Filter for schemas with analysis
    analyzed_schemas = schema_df[
        schema_df["SCHEMA_ANALYSIS"].notna() & (schema_df["SCHEMA_ANALYSIS"] != "")
    ]

    if analyzed_schemas.empty:
        st.warning("No schema analysis results found.")
        st.stop()

    # Schema selector
    analyzed_tables = analyzed_schemas["TABLE_NAME"].unique()
    selected_table = st.selectbox("Select Schema for Analysis:", analyzed_tables)
    selected_analysis = analyzed_schemas[
        analyzed_schemas["TABLE_NAME"] == selected_table
    ].iloc[0]

    # Parse and display analysis
    analysis_data = parse_schema_json(selected_analysis["SCHEMA_ANALYSIS"])

    if analysis_data:
        out = get_schema_analysis(analysis_data)
        st.markdown(out)

    else:
        st.subheader("📄 Raw Analysis")
        st.text_area(
            "Analysis Content", selected_analysis["SCHEMA_ANALYSIS"], height=400
        )
# Footer
st.markdown("---")
st.markdown(
    f"""
    <div style='text-align: center; color: #666666;'>
        <p>🔄 <strong>MusicFlow Schema Registry</strong> | Connected to Snowflake</p>
        <p>Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | Total Schemas: {len(schema_df)}</p>
    </div>
    """,
    unsafe_allow_html=True,
)
