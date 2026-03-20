{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_trx_product_ta_wd0', 'batch', 'edwtd_tax_audit'],
    meta={
        'source_workflow': 'wf_m_R_TRX_PRODUCT_TA_WD0',
        'target_table': 'R_TRX_PRODUCT_TA_WD0',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.463840+00:00'
    }
) }}

WITH 

source_r_trx_product AS (
    SELECT
        product_key
    FROM {{ source('raw', 'r_trx_product') }}
),

source_wi_taxaudit_dt_cntrl AS (
    SELECT
        taxaudit_previous_year_int,
        taxaudit_current_year_int,
        taxaudit_delete_year_int,
        taxaudit_month_start_int,
        taxaudit_month_end_int,
        taxaudit_etl_run_time
    FROM {{ source('raw', 'wi_taxaudit_dt_cntrl') }}
),

final AS (
    SELECT
        product_key,
        fiscal_year_number_int
    FROM source_wi_taxaudit_dt_cntrl
)

SELECT * FROM final