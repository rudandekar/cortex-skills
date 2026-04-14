# app.py
import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

st.set_page_config(page_title="<App Title>", layout="wide")
session = get_active_session()

# Sidebar filters
st.sidebar.header("Filters")
risk_filter = st.sidebar.multiselect("Risk Tier", ["HIGH", "MEDIUM", "LOW"], default=["HIGH", "MEDIUM"])
status_filter = st.sidebar.selectbox("Status", ["PENDING", "ALL"])

# Main content
st.title("<App Title>")
tab1, tab2 = st.tabs(["Action Queue", "Analytics"])

with tab1:
    st.header("Items Pending Review")
    
    # Fetch data
    risk_list = ",".join([f"'{r}'" for r in risk_filter])
    query = f"""
        SELECT * FROM <schema>.V_ACTION_QUEUE
        WHERE <risk_tier_col> IN ({risk_list})
        AND status = 'PENDING'
        LIMIT 50
    """
    df = session.sql(query).to_pandas()
    
    if df.empty:
        st.success("No items pending review!")
    else:
        for idx, row in df.iterrows():
            with st.container():
                col1, col2, col3 = st.columns([3, 1, 2])
                
                with col1:
                    st.subheader(f"{row['<display_col>']}")
                    st.write(f"Risk: {row['<risk_score_col>']:.2f} ({row['<risk_tier_col>']})")
                
                with col2:
                    st.metric("Recommendation", row['<recommendation_col>'])
                
                with col3:
                    c1, c2, c3 = st.columns(3)
                    entity_id = row['<entity_id_col>']
                    
                    if c1.button("✓", key=f"approve_{entity_id}", help="Approve"):
                        session.sql(f"""
                            INSERT INTO <schema>.ACTION_LOG 
                            (entity_id, entity_type, action_type, original_score, actioned_by)
                            VALUES ('{entity_id}', '<entity_type>', 'APPROVE', 
                                    {row['<risk_score_col>']}, CURRENT_USER())
                        """).collect()
                        st.rerun()
                    
                    if c2.button("✎", key=f"override_{entity_id}", help="Override"):
                        st.session_state[f"show_override_{entity_id}"] = True
                    
                    if c3.button("⏸", key=f"defer_{entity_id}", help="Defer"):
                        st.session_state[f"show_defer_{entity_id}"] = True
                
                # Override form
                if st.session_state.get(f"show_override_{entity_id}"):
                    with st.form(f"override_form_{entity_id}"):
                        reason = st.selectbox("Reason", [<override_reasons>])
                        notes = st.text_area("Notes (optional)")
                        if st.form_submit_button("Submit"):
                            session.sql(f"""
                                INSERT INTO <schema>.ACTION_LOG 
                                (entity_id, entity_type, action_type, action_reason, 
                                 action_notes, original_score, actioned_by)
                                VALUES ('{entity_id}', '<entity_type>', 'OVERRIDE',
                                        '{reason}', '{notes}', {row['<risk_score_col>']}, CURRENT_USER())
                            """).collect()
                            st.session_state[f"show_override_{entity_id}"] = False
                            st.rerun()
                
                st.divider()

with tab2:
    st.header("Action Analytics")
    summary = session.sql("SELECT * FROM <schema>.V_ACTION_SUMMARY WHERE action_date >= DATEADD('day', -30, CURRENT_DATE())").to_pandas()
    st.bar_chart(summary.pivot(index='ACTION_DATE', columns='ACTION_TYPE', values='ACTION_COUNT'))
    
    st.subheader("Override Reasons")
    reasons = session.sql("SELECT * FROM <schema>.V_OVERRIDE_REASONS").to_pandas()
    st.dataframe(reasons)
