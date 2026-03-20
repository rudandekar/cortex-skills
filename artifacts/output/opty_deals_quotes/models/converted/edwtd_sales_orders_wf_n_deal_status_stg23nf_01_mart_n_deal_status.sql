{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_deal_status_stg23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_DEAL_STATUS_STG23NF',
        'target_table': 'N_DEAL_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.974138+00:00'
    }
) }}

WITH 

source_n_deal_status AS (
    SELECT
        bk_deal_status_cd,
        deal_status_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        active_flg
    FROM {{ source('raw', 'n_deal_status') }}
),

final AS (
    SELECT
        bk_deal_status_cd,
        deal_status_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        active_flg
    FROM source_n_deal_status
)

SELECT * FROM final