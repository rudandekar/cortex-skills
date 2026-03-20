{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ae_nrs_manual_adj', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_AE_NRS_MANUAL_ADJ',
        'target_table': 'EL_AE_NRS_MANUAL_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.689607+00:00'
    }
) }}

WITH 

source_st_ae_nrs_manual_adj AS (
    SELECT
        fiscal_month_id,
        sales_territory_key,
        product_id,
        measure_name,
        sub_measure_name,
        amount,
        offer_type_level_2,
        dual_gaap_flag,
        corporate_revenue_flag,
        created_by,
        create_datetime,
        updated_by,
        update_datetime
    FROM {{ source('raw', 'st_ae_nrs_manual_adj') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        sales_territory_key,
        product_key,
        bk_adjustment_measure_type_cd,
        bk_adj_sub_measure_type_cd,
        adjustment_allocation_usd_amt,
        offer_type_level_2,
        dual_gaap_flag,
        corporate_revenue_flag,
        created_by,
        create_datetime,
        updated_by,
        update_datetime,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_ae_nrs_manual_adj
)

SELECT * FROM final