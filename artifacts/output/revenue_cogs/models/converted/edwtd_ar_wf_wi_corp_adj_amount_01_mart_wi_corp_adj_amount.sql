{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_corp_adj_amount', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_CORP_ADJ_AMOUNT',
        'target_table': 'WI_CORP_ADJ_AMOUNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.974124+00:00'
    }
) }}

WITH 

source_wi_corp_adj_amonut AS (
    SELECT
        ae_header_id,
        ae_line_num,
        application_id,
        global_name,
        adjustment_revenue,
        adjustment_cogs
    FROM {{ source('raw', 'wi_corp_adj_amonut') }}
),

final AS (
    SELECT
        transaction_dist_id,
        ae_header_id,
        ae_line_num,
        application_id,
        global_name,
        adjustment_revenue,
        adjustment_cogs
    FROM source_wi_corp_adj_amonut
)

SELECT * FROM final