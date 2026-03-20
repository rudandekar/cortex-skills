{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_quote_concession_source', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_QUOTE_CONCESSION_SOURCE',
        'target_table': 'N_DEAL_QUOTE_CONCESSION_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.908382+00:00'
    }
) }}

WITH 

source_w_deal_quote_concession_source AS (
    SELECT
        bk_quote_num,
        bk_deal_concession_source_num,
        concession_source_type,
        ru_tmip_quote_num,
        bk_contract_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_quote_concession_source') }}
),

final AS (
    SELECT
        bk_quote_num,
        bk_deal_concession_source_num,
        concession_source_type,
        ru_tmip_quote_num,
        bk_contract_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_deal_quote_concession_source
)

SELECT * FROM final