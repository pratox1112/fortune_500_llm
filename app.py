import streamlit as st
import requests
import json

# API endpoints
START_API = "https://f9uq2y80jj.execute-api.us-east-1.amazonaws.com/test"
STATUS_API = "https://ee7385n43f.execute-api.us-east-1.amazonaws.com/test"
QUERY_API = "https://kzlmwc1f8k.execute-api.us-east-1.amazonaws.com/dev/query"

st.set_page_config(page_title="Fortune 500 Chatbot", page_icon="📊", layout="wide")
st.title("📊 Fortune 500 Company Chatbot")

# ---------------------------
# 1. Inputs for company
# ---------------------------
ticker = st.text_input("Stock Ticker (Yahoo)", value="TSLA")
company = st.text_input("Company Name (SEC)", value="Tesla")

# ---------------------------
# 2. Run pipeline (trigger once, no infinite loop)
# ---------------------------
if st.button("🚀 Run Pipeline"):
    with st.spinner(f"Starting pipeline for {company}..."):
        try:
            start_resp = requests.post(START_API, json={"ticker": ticker, "company": company})
            data = start_resp.json()
            exec_arn = data.get("executionArn")
            if exec_arn:
                st.success(f"Pipeline triggered for {company}. Execution ARN: {exec_arn}")

                # Check status once (not looping forever)
                status_resp = requests.post(STATUS_API, json={"executionArn": exec_arn})
                status_data = status_resp.json()
                st.info(f"Pipeline current status: {status_data.get('status')}")
            else:
                st.error("Pipeline did not return an execution ARN.")
        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")

# ---------------------------
# 3. Chatbot
# ---------------------------
st.subheader(f"💬 Chat with {company}'s Financial Data")

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# Display history
for role, text in st.session_state.chat_history:
    with st.chat_message(role):
        st.write(text)

# Chat input
if question := st.chat_input("Ask a question..."):
    # Add user msg
    st.session_state.chat_history.append(("user", question))
    with st.chat_message("user"):
        st.write(question)

    try:
        resp = requests.post(QUERY_API, json={"question": question, "company": company}, timeout=60)
        data = resp.json()
        if "body" in data and isinstance(data["body"], str):
            data = json.loads(data["body"])

        answer = data.get("answer", "No answer found.")
        citations = data.get("citations", [])

        response_text = answer
        if citations:
            response_text += "\n\nSources:\n" + "\n".join([str(c) for c in citations])

    except Exception as e:
        response_text = f"Error: {e}"

    # Add assistant msg
    st.session_state.chat_history.append(("assistant", response_text))
    with st.chat_message("assistant"):
        st.write(response_text)
