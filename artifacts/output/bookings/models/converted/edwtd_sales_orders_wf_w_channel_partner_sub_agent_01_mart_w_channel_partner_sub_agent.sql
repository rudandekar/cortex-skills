{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_channel_partner_sub_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CHANNEL_PARTNER_SUB_AGENT',
        'target_table': 'W_CHANNEL_PARTNER_SUB_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.985866+00:00'
    }
) }}

WITH 

source_w_channel_partner_sub_agent AS (
    SELECT
        master_agent_be_geo_id_int,
        cisco_sub_agent_id,
        external_sub_agent_id,
        dd_master_agent_partner_name,
        sub_agent_company_name,
        sub_agent_first_name,
        sub_agent_last_name,
        sub_agent_phone_number,
        sub_agent_email_address,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_channel_partner_sub_agent') }}
),

final AS (
    SELECT
        master_agent_be_geo_id_int,
        cisco_sub_agent_id,
        external_sub_agent_id,
        dd_master_agent_partner_name,
        sub_agent_company_name,
        sub_agent_first_name,
        sub_agent_last_name,
        sub_agent_phone_number,
        sub_agent_email_address,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_channel_partner_sub_agent
)

SELECT * FROM final