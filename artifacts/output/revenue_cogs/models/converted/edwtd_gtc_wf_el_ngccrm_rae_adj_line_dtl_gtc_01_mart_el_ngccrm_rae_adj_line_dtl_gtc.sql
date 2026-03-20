{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ngccrm_rae_adj_line_dtl_gtc', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_EL_NGCCRM_RAE_ADJ_LINE_DTL_GTC',
        'target_table': 'EL_NGCCRM_RAE_ADJ_LINE_DTL_GTC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.268209+00:00'
    }
) }}

WITH 

source_el_ngccrm_rae_adj_line_dtl_gtc AS (
    SELECT
        adjustment_line_id,
        amount,
        code_combination_id,
        currency_code,
        exchange_rate,
        gl_date,
        gl_posted_date,
        gl_posted_flag,
        global_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_ngccrm_rae_adj_line_dtl_gtc') }}
),

final AS (
    SELECT
        adjustment_line_id,
        amount,
        code_combination_id,
        currency_code,
        exchange_rate,
        gl_date,
        gl_posted_date,
        gl_posted_flag,
        global_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_el_ngccrm_rae_adj_line_dtl_gtc
)

SELECT * FROM final