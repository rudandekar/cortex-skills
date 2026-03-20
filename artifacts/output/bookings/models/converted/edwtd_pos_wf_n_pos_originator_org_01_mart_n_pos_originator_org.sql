{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_originator_org', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_ORIGINATOR_ORG',
        'target_table': 'N_POS_ORIGINATOR_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.195626+00:00'
    }
) }}

WITH 

source_w_pos_originator_org AS (
    SELECT
        bk_wips_originator_id_int,
        originator_name,
        bk_originator_country_code,
        pos_originator_org_active_flag,
        two_tier_theater_name,
        theater_name,
        organization_type_code,
        reporting_source_name,
        master_distributor_name,
        iso_currency_cd,
        distributor_type_cd,
        sales_territory_key,
        bill_to_erp_cust_acct_loc_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_reported_sales_rep_name,
        bk_company_cd,
        dsv_enabled_flg,
        net_price_flg,
        action_code,
        dml_type,
        sold_for_net_price_flg
    FROM {{ source('raw', 'w_pos_originator_org') }}
),

final AS (
    SELECT
        bk_wips_originator_id_int,
        originator_name,
        bk_originator_country_code,
        pos_originator_org_active_flag,
        two_tier_theater_name,
        theater_name,
        organization_type_code,
        reporting_source_name,
        master_distributor_name,
        iso_currency_cd,
        distributor_type_cd,
        sales_territory_key,
        bill_to_erp_cust_acct_loc_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_reported_sales_rep_name,
        bk_company_cd,
        dsv_enabled_flg,
        net_price_flg,
        sold_for_net_price_flg
    FROM source_w_pos_originator_org
)

SELECT * FROM final