{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_cogs_audit', 'batch', 'edwtd_audits'],
    meta={
        'source_workflow': 'wf_m_MT_COGS_AUDIT',
        'target_table': 'MT_COGS_AUDIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.071702+00:00'
    }
) }}

WITH 

source_mt_cogs_audit AS (
    SELECT
        dv_so_num_int,
        dv_type_of_entity,
        dv_country_enablement_cd,
        cogs_data_amt,
        cogs_data_trx_name,
        cogs_replacement_amt,
        cogs_replacement_trx_name,
        cogs_inclusive_order_amt,
        cogs_inclusive_order_trx_name,
        cogs_foundation_us_amt,
        cogs_foundation_us_curr_cd,
        cogs_foundation_us_trx_name,
        cogs_foundation_bv_amt,
        cogs_foundation_bv_currency_cd,
        cogs_foundation_bv_trx_name,
        cogs_tb_us_option_amt,
        cogs_tb_us_option_currency_cd,
        cogs_tb_us_option_trx_name,
        cogs_donation_amt,
        cogs_donation_rev_delta_amt,
        cogs_donation_trx_name,
        cogs_intrnl_demo_inv_amt,
        cogs_intrnl_demo_rev_delta_amt,
        cogs_intrnl_demo_inv_trx_name,
        cogs_tb_option_bv_amt,
        cogs_tb_option_bv_currency_cd,
        cogs_tb_option_bv_trx_name,
        cogs_cap_jpn_amt,
        cogs_cap_jpn_trx_name,
        cogs_foundation_cg_amt,
        cogs_foundation_cg_currency_cd,
        cogs_foundation_cg_trx_name,
        cogs_tb_cg_option_amt,
        cogs_tb_cg_option_currency_cd,
        cogs_tb_cg_option_trx_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_cogs_audit') }}
),

final AS (
    SELECT
        dv_so_num_int,
        dv_type_of_entity,
        dv_country_enablement_cd,
        cogs_data_amt,
        cogs_data_trx_name,
        cogs_replacement_amt,
        cogs_replacement_trx_name,
        cogs_inclusive_order_amt,
        cogs_inclusive_order_trx_name,
        cogs_foundation_us_amt,
        cogs_foundation_us_curr_cd,
        cogs_foundation_us_trx_name,
        cogs_foundation_bv_amt,
        cogs_foundation_bv_currency_cd,
        cogs_foundation_bv_trx_name,
        cogs_tb_us_option_amt,
        cogs_tb_us_option_currency_cd,
        cogs_tb_us_option_trx_name,
        cogs_donation_amt,
        cogs_donation_rev_delta_amt,
        cogs_donation_trx_name,
        cogs_intrnl_demo_inv_amt,
        cogs_intrnl_demo_rev_delta_amt,
        cogs_intrnl_demo_inv_trx_name,
        cogs_tb_option_bv_amt,
        cogs_tb_option_bv_currency_cd,
        cogs_tb_option_bv_trx_name,
        cogs_cap_jpn_amt,
        cogs_cap_jpn_trx_name,
        cogs_foundation_cg_amt,
        cogs_foundation_cg_currency_cd,
        cogs_foundation_cg_trx_name,
        cogs_tb_cg_option_amt,
        cogs_tb_cg_option_currency_cd,
        cogs_tb_cg_option_trx_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_cogs_audit
)

SELECT * FROM final