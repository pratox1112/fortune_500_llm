import streamlit as st
import requests
import time
import json
import config 

# Replace with your API Gateway endpoints

st.set_page_config(page_title="Financial Data Pipeline", page_icon="📊", layout="centered")
st.title("📊 Financial Data Pipeline Runner")

# Inputs
ticker = st.text_input("Stock Ticker (Yahoo)", value="TSLA")
company = st.text_input("Company Name (SEC)", value="Tesla")

if st.button("🚀 Run Pipeline"):
    with st.spinner("Starting pipeline..."):
        # Start the pipeline
        try:
            start_resp = requests.post(config.START_API, json={"ticker": ticker, "company": company})
            start_resp.raise_for_status()
            data = start_resp.json()
        except Exception as e:
            st.error(f"Failed to start pipeline: {e}")
            st.stop()

        exec_arn = data.get("executionArn")
        if not exec_arn:
            st.error("No execution ARN returned. Check Lambda/API config.")
            st.stop()

        st.info(f"Pipeline started. Execution ARN:\n\n`{exec_arn}`")

        # Poll status
        while True:
            try:
                status_resp = requests.post(config.STATUS_API, json={"executionArn": exec_arn})
                status_resp.raise_for_status()
                status_data = status_resp.json()
            except Exception as e:
                st.error(f"Failed to fetch status: {e}")
                st.stop()

            status = status_data.get("status")

            # Show live status
            with st.empty():
                st.write(f"🔄 Current Status: **{status}**")

            if status in ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"]:
                break

            time.sleep(5)

        # ✅ Final output handling
        if status == "SUCCEEDED":
            st.success("✅ Pipeline finished successfully!")

            output = status_data.get("output", {})

            # Just display the answer text
            if isinstance(output, dict) and "answer" in output:
                st.subheader("Summary")
                st.write(output["answer"])
            else:
                st.warning("No answer found in output. Showing raw output:")
                st.json(output)

        else:
            st.error(f"❌ Pipeline ended with status: {status}")
