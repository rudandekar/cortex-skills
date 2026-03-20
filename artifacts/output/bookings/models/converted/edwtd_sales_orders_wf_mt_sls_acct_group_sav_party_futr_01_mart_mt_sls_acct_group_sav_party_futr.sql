{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sls_acct_group_sav_party_futr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_SLS_ACCT_GROUP_SAV_PARTY_FUTR',
        'target_table': 'MT_SLS_ACCT_GROUP_SAV_PARTY_FUTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.273655+00:00'
    }
) }}

WITH 

source_mt_sls_acct_group_sav_party_futr AS (
    SELECT
        sales_account_group_party_key,
        sales_account_id_int,
        sales_account_group_start_dt,
        sales_account_group_end_dt,
        sales_account_group_type_cd,
        sales_account_sub_type_cd,
        sales_acct_created_party_key,
        sales_account_creation_dt,
        sales_account_group_name,
        primary_cust_address_party_key,
        vertical_market_segment_cd,
        vertical_market_sub_segment_cd,
        sls_acct_coverage_level_int,
        sls_acct_group_sales_terr_key,
        sales_acct_sub_segment_name,
        sales_account_segment_name,
        sales_rep_num,
        sales_rep_name,
        sls_acct_grp_party_owner_type,
        hibernation_flg,
        orphaned_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_sls_acct_group_sav_party_futr') }}
),

final AS (
    SELECT
        sales_account_group_party_key,
        sales_account_id_int,
        sales_account_group_start_dt,
        sales_account_group_end_dt,
        sales_account_group_type_cd,
        sales_account_sub_type_cd,
        sales_acct_created_party_key,
        sales_account_creation_dt,
        sales_account_group_name,
        primary_cust_address_party_key,
        vertical_market_segment_cd,
        vertical_market_sub_segment_cd,
        sls_acct_coverage_level_int,
        sls_acct_group_sales_terr_key,
        sales_acct_sub_segment_name,
        sales_account_segment_name,
        sales_rep_num,
        sales_rep_name,
        sls_acct_grp_party_owner_type,
        hibernation_flg,
        orphaned_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_sls_acct_group_sav_party_futr
)

SELECT * FROM final