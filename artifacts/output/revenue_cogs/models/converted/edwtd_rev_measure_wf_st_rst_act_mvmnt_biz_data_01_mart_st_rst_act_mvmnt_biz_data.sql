{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rst_act_mvmnt_biz_data', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_RST_ACT_MVMNT_BIZ_DATA',
        'target_table': 'ST_RST_ACT_MVMNT_BIZ_DATA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.116695+00:00'
    }
) }}

WITH 

source_ff_rst_act_mvmnt_biz_data AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dd_ext_thtr_nm_src,
        l2_sales_trtry_descr_src,
        sales_cvrg_code_src,
        bk_business_entity_nm_src,
        sales_territory_key_src,
        dd_ext_thtr_nm_tgt,
        l2_sales_trtry_descr_tgt,
        sales_cvrg_code_tgt,
        sales_territory_key_tgt,
        be_l1,
        pct_change,
        bk_adjustment_measure_type_cd
    FROM {{ source('raw', 'ff_rst_act_mvmnt_biz_data') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        dd_ext_thtr_nm_src,
        l2_sales_trtry_descr_src,
        sales_cvrg_code_src,
        bk_business_entity_nm_src,
        bk_adjustment_measure_type_cd,
        sales_territory_key_src,
        pct_change,
        dd_ext_thtr_nm_tgt,
        l2_sales_trtry_descr_tgt,
        sales_cvrg_code_tgt,
        sales_territory_key_tgt,
        ges_update_date
    FROM source_ff_rst_act_mvmnt_biz_data
)

SELECT * FROM final