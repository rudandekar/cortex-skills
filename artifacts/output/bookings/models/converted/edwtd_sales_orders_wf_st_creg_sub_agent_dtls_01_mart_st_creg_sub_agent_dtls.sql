{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_creg_sub_agent_dtls', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CREG_SUB_AGENT_DTLS',
        'target_table': 'ST_CREG_SUB_AGENT_DTLS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.677181+00:00'
    }
) }}

WITH 

source_ff_creg_sub_agent_dtls1 AS (
    SELECT
        updated_ts,
        updated_by,
        sub_agent_id,
        sub_agent_company,
        phone,
        last_name,
        first_name,
        email,
        cisco_sub_agent_id,
        be_geo_id,
        added_ts,
        added_by,
        active_flag
    FROM {{ source('raw', 'ff_creg_sub_agent_dtls1') }}
),

final AS (
    SELECT
        updated_ts,
        updated_by,
        sub_agent_id,
        sub_agent_company,
        phone,
        last_name,
        first_name,
        email,
        cisco_sub_agent_id,
        be_geo_id,
        added_ts,
        added_by,
        active_flag
    FROM source_ff_creg_sub_agent_dtls1
)

SELECT * FROM final