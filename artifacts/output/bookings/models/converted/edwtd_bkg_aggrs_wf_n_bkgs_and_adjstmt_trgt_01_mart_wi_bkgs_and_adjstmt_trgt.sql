{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_bkgs_and_adjstmt_trgt', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_BKGS_AND_ADJSTMT_TRGT',
        'target_table': 'WI_BKGS_AND_ADJSTMT_TRGT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.332932+00:00'
    }
) }}

WITH 

source_n_bkgs_and_adjstmt_trgt AS (
    SELECT
        sales_territory_key,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_qtr_num_int,
        dv_fiscal_year_qtr_num_int,
        adjusted_discount_trgt_pct,
        bkgs_gross_margin_trgt_pct,
        service_flg,
        pd_sales_territory_descr,
        pd_dv_sales_terr_lvl_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_bkgs_and_adjstmt_trgt') }}
),

final AS (
    SELECT
        sales_territory_key,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_qtr_num_int,
        dv_fiscal_year_qtr_num_int,
        adjusted_discount_trgt_pct,
        bkgs_gross_margin_trgt_pct,
        service_flg,
        pd_sales_territory_descr,
        pd_dv_sales_terr_lvl_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_bkgs_and_adjstmt_trgt
)

SELECT * FROM final