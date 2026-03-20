{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ldsg_spend_category_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_LDSG_SPEND_CATEGORY_STG23NF',
        'target_table': 'N_LDSG_SPEND_CATEGORY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.775248+00:00'
    }
) }}

WITH 

source_st_vw_ldsg_extract AS (
    SELECT
        spend_category,
        fiscal_month_name,
        node_level05_value,
        forecast_amt,
        load_date
    FROM {{ source('raw', 'st_vw_ldsg_extract') }}
),

final AS (
    SELECT
        bk_ldsg_spend_category_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_vw_ldsg_extract
)

SELECT * FROM final