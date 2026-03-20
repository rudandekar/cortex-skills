{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_mtx_partner', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MTX_PARTNER',
        'target_table': 'WI_SALES_CHANNEL_MTX_PARTNER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.383644+00:00'
    }
) }}

WITH 

source_wi_sales_channel_mtx_patterns AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'wi_sales_channel_mtx_patterns') }}
),

source_n_channel_partner_site AS (
    SELECT
        partner_site_party_key,
        bk_party_id_int,
        partner_country_party_key,
        dd_grndparnt_partner_party_key,
        edw_create_user,
        edw_create_datetime,
        source_deleted_flg
    FROM {{ source('raw', 'n_channel_partner_site') }}
),

source_n_erp_party AS (
    SELECT
        erp_party_number,
        cr_reported_party_id_int,
        erp_customer_role,
        erp_vendor_role,
        erp_party_name,
        erp_active_flag,
        sk_erp_customer_id,
        party_key,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        edw_observation_datetime
    FROM {{ source('raw', 'n_erp_party') }}
),

source_n_erp_cust_acct_loc_use AS (
    SELECT
        erp_cust_account_location_key,
        start_tv_date,
        end_tv_date,
        bk_operating_unit_name_cd,
        bk_location_id_int,
        bk_erp_cal_site_use_type,
        customer_party_key,
        customer_account_site_id_int,
        match_score_code,
        match_scoring_criteria_code,
        ep_cr_party_id_int,
        ep_erp_party_id_int,
        sk_party_site_id_int,
        sk_site_use_id,
        customer_account_key,
        locator_status_code,
        dd_bk_customer_account_number,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'n_erp_cust_acct_loc_use') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_party_key_as_is,
        partner_party_key,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key,
        partner_bill_to_cust_party_key
    FROM source_n_erp_cust_acct_loc_use
)

SELECT * FROM final