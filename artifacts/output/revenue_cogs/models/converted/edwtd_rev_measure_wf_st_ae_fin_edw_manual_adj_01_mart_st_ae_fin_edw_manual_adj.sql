{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_fin_edw_manual_adj', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_FIN_EDW_MANUAL_ADJ',
        'target_table': 'ST_AE_FIN_EDW_MANUAL_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.620316+00:00'
    }
) }}

WITH 

source_ff_ae_fin_edw_manual_adj AS (
    SELECT
        fiscal_month_id,
        company_name,
        sales_territory_name_code,
        product_id,
        measure_name,
        sub_measure_name,
        adj_type_code,
        bk_rprtd_adjstmnt_cntry_name,
        bk_adjstmnt_sls_subcoverge_cd,
        amount,
        goods_product_id,
        deal_id
    FROM {{ source('raw', 'ff_ae_fin_edw_manual_adj') }}
),

transformed_exp_ff_ae_fin_edw_manual_adj AS (
    SELECT
    fiscal_month_id,
    company_name,
    sales_territory_name_code,
    product_id,
    measure_name,
    sub_measure_name,
    adj_type_code,
    bk_rprtd_adjstmnt_cntry_name,
    bk_adjstmnt_sls_subcoverge_cd,
    amount,
    goods_product_id,
    deal_id,
    SYSTIMESTAMP() AS edw_create_datetime
    FROM source_ff_ae_fin_edw_manual_adj
),

final AS (
    SELECT
        fiscal_month_id,
        company_name,
        sales_territory_name_code,
        product_id,
        measure_name,
        sub_measure_name,
        adj_type_code,
        bk_rprtd_adjstmnt_cntry_name,
        bk_adjstmnt_sls_subcoverge_cd,
        amount,
        goods_product_id,
        deal_id,
        edw_create_datetime
    FROM transformed_exp_ff_ae_fin_edw_manual_adj
)

SELECT * FROM final