{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_sol_bni_fhi_snapshot', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_SOL_BNI_FHI_SNAPSHOT',
        'target_table': 'EL_SOL_BNI_FHI_SNAPSHOT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.752237+00:00'
    }
) }}

WITH 

source_el_sol_bni_fhi_snapshot AS (
    SELECT
        fiscal_year_month_int_snpshot,
        unbilled_processed_month,
        sales_order_line_key,
        dv_bni_fhi_type_code,
        rpo_flg,
        auto_release_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_sol_bni_fhi_snapshot') }}
),

final AS (
    SELECT
        fiscal_year_month_int_snpshot,
        unbilled_processed_month,
        sales_order_line_key,
        dv_bni_fhi_type_code,
        rpo_flg,
        auto_release_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_sol_bni_fhi_snapshot
)

SELECT * FROM final