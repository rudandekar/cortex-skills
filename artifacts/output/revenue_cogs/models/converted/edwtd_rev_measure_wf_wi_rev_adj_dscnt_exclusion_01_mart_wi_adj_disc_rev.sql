{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rev_adj_dscnt_exclusion', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_REV_ADJ_DSCNT_EXCLUSION',
        'target_table': 'WI_ADJ_DISC_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.948548+00:00'
    }
) }}

WITH 

source_wi_adj_disc_rev AS (
    SELECT
        sales_territory_key,
        sales_rep_num,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        l4_sales_territory_descr,
        l5_sales_territory_descr,
        l6_sales_territory_descr,
        source_order_num_int,
        pos_transaction_id_int,
        deal_id,
        product_key,
        product_family_id,
        product_id,
        rev_measure_trans_type_cd,
        dv_service_flg,
        dv_corporate_revenue_flg,
        dv_fiscal_year_mth_number_int,
        discount_category,
        pf_excl_flg,
        pid_excl_flg,
        deal_excl_flg,
        so_excl_flg,
        revenue_measure_key
    FROM {{ source('raw', 'wi_adj_disc_rev') }}
),

final AS (
    SELECT
        sales_territory_key,
        sales_rep_num,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        l4_sales_territory_descr,
        l5_sales_territory_descr,
        l6_sales_territory_descr,
        source_order_num_int,
        pos_transaction_id_int,
        deal_id,
        product_key,
        product_family_id,
        product_id,
        rev_measure_trans_type_cd,
        dv_service_flg,
        dv_corporate_revenue_flg,
        dv_fiscal_year_mth_number_int,
        discount_category,
        pf_excl_flg,
        pid_excl_flg,
        deal_excl_flg,
        so_excl_flg,
        revenue_measure_key
    FROM source_wi_adj_disc_rev
)

SELECT * FROM final