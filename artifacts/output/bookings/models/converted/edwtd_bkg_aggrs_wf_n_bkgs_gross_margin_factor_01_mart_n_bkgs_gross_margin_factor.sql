{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_bkgs_gross_margin_factor', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_BKGS_GROSS_MARGIN_FACTOR',
        'target_table': 'N_BKGS_GROSS_MARGIN_FACTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.930453+00:00'
    }
) }}

WITH 

source_n_bkgs_gross_margin_factor AS (
    SELECT
        sales_territory_key,
        bk_business_entity_name,
        bk_business_entity_type_cd,
        bkgs_gross_margin_factor_pct,
        service_flg,
        pd_sales_territory_descr,
        pd_dv_sales_terr_lvl_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int
    FROM {{ source('raw', 'n_bkgs_gross_margin_factor') }}
),

final AS (
    SELECT
        sales_territory_key,
        bk_business_entity_name,
        bk_business_entity_type_cd,
        bkgs_gross_margin_factor_pct,
        service_flg,
        pd_sales_territory_descr,
        pd_dv_sales_terr_lvl_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int
    FROM source_n_bkgs_gross_margin_factor
)

SELECT * FROM final