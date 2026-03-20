{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_erp_pos_soldto_ta_wd0', 'batch', 'edwtd_tax_audit'],
    meta={
        'source_workflow': 'wf_m_MT_ERP_POS_SOLDTO_TA_WD0',
        'target_table': 'MT_ERP_POS_SOLDTO_TA_WD0',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.786926+00:00'
    }
) }}

WITH 

source_mt_erp_pos_soldto AS (
    SELECT
        sold_to_customer_key,
        customer_type,
        customer_party_key,
        erp_bk_customer_account_num,
        erp_bk_location_id_int,
        erp_bk_operating_unit_name_cd,
        erp_wips_site_use_id_int,
        erp_party_num,
        erp_wips_customer_name,
        erp_party_site_id_int,
        erp_party_site_num,
        erp_party_id_int,
        erp_wips_customer_id,
        erp_sales_channel_descr,
        erp_sales_channel_cd,
        erp_business_entity_cd,
        erp_internal_customer_flg,
        erp_customer_class_cd,
        erp_customer_class_descr,
        erp_customer_type_cd,
        erp_revenue_customer_flg,
        erp_customer_acct_status_cd,
        erp_customer_account_key,
        erp_wips_locator_key,
        wips_customer_site_active_flg,
        wips_theater_name,
        erp_wips_address_id_int,
        erp_wips_line_1_addr,
        erp_wips_line_2_addr,
        erp_wips_line_3_addr,
        erp_wips_line_4_addr,
        erp_wips_city_name,
        state_or_province_name,
        erp_wips_county_name,
        erp_wips_postal_cd,
        erp_wips_postal_plus4_cd,
        erp_wips_iso_country_cd,
        erp_wips_address_status_cd,
        erp_wips_completeness_flg,
        erp_wips_geo_validity_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_erp_pos_soldto') }}
),

source_wi_taxaudit_dt_cntrl AS (
    SELECT
        taxaudit_previous_year_int,
        taxaudit_current_year_int,
        taxaudit_delete_year_int,
        taxaudit_month_start_int,
        taxaudit_month_end_int,
        taxaudit_etl_run_time
    FROM {{ source('raw', 'wi_taxaudit_dt_cntrl') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        customer_type,
        customer_party_key,
        erp_bk_customer_account_num,
        erp_bk_location_id_int,
        erp_bk_operating_unit_name_cd,
        erp_wips_site_use_id_int,
        erp_party_num,
        erp_wips_customer_name,
        erp_party_site_id_int,
        erp_party_site_num,
        erp_party_id_int,
        erp_wips_customer_id,
        erp_sales_channel_descr,
        erp_sales_channel_cd,
        erp_business_entity_cd,
        erp_internal_customer_flg,
        erp_customer_class_cd,
        erp_customer_class_descr,
        erp_customer_type_cd,
        erp_revenue_customer_flg,
        erp_customer_acct_status_cd,
        erp_customer_account_key,
        erp_wips_locator_key,
        wips_customer_site_active_flg,
        wips_theater_name,
        erp_wips_address_id_int,
        erp_wips_line_1_addr,
        erp_wips_line_2_addr,
        erp_wips_line_3_addr,
        erp_wips_line_4_addr,
        erp_wips_city_name,
        state_or_province_name,
        erp_wips_county_name,
        erp_wips_postal_cd,
        erp_wips_postal_plus4_cd,
        erp_wips_iso_country_cd,
        erp_wips_address_status_cd,
        erp_wips_completeness_flg,
        erp_wips_geo_validity_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        fiscal_year_number_int
    FROM source_wi_taxaudit_dt_cntrl
)

SELECT * FROM final