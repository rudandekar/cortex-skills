{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_webex_cisco_company_map', 'batch', 'edwtd_ea'],
    meta={
        'source_workflow': 'wf_m_ST_WEBEX_CISCO_COMPANY_MAP',
        'target_table': 'ST_WEBEX_CISCO_COMPANY_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.490724+00:00'
    }
) }}

WITH 

source_ff_webex_cisco_company_map AS (
    SELECT
        batch_id,
        webex_company,
        cisco_company,
        company_desc,
        financial_company_key,
        theater,
        sl6_description,
        sales_territory_key,
        sales_mkt_segment_name,
        sub_sales_mkt_segment_name,
        percentage_split,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_webex_cisco_company_map') }}
),

final AS (
    SELECT
        batch_id,
        webex_company,
        cisco_company,
        company_desc,
        financial_company_key,
        theater,
        sl6_description,
        sales_territory_key,
        sales_mkt_segment_name,
        sub_sales_mkt_segment_name,
        percentage_split,
        create_datetime,
        action_code
    FROM source_ff_webex_cisco_company_map
)

SELECT * FROM final