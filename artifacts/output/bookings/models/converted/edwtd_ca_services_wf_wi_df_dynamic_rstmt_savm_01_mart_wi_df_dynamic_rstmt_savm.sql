{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_df_dynamic_rstmt_savm', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_WI_DF_DYNAMIC_RSTMT_SAVM',
        'target_table': 'WI_DF_DYNAMIC_RSTMT_SAVM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.760679+00:00'
    }
) }}

WITH 

source_wi_df_dynamic_rstmt_savm AS (
    SELECT
        sales_order_line_key,
        restated_savm_key,
        bk_sales_account_id_int,
        as_was_savm_key,
        sales_account_group_name,
        as_was_sav_name,
        dv_last_updated_dt,
        dv_end_cust_party_key,
        dv_end_cust_ownership_splt_pct,
        bk_sa_member_id_int,
        bk_so_number_int,
        sales_territory_key,
        sales_territory_descr,
        dv_sav_reason_descr,
        dv_end_cust_reason_descr,
        bookings_measure_key,
        sales_rep_num
    FROM {{ source('raw', 'wi_df_dynamic_rstmt_savm') }}
),

final AS (
    SELECT
        sales_order_line_key,
        restated_savm_key,
        bk_sales_account_id_int,
        as_was_savm_key,
        sales_account_group_name,
        as_was_sav_name,
        dv_last_updated_dt,
        dv_end_cust_party_key,
        dv_end_cust_ownership_splt_pct,
        bk_sa_member_id_int,
        bk_so_number_int,
        sales_territory_key,
        sales_territory_descr,
        dv_sav_reason_descr,
        dv_end_cust_reason_descr,
        bookings_measure_key,
        sales_rep_num
    FROM source_wi_df_dynamic_rstmt_savm
)

SELECT * FROM final