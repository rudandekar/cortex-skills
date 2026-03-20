{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_channel_matrix', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHANNEL_MATRIX',
        'target_table': 'WI_SALES_CHANNEL_MATRIX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.418981+00:00'
    }
) }}

WITH 

source_wi_sales_channel_matrix_int AS (
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
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key,
        level_1_sls_hierarchy,
        partner_type_code,
        rule_cd,
        sold_to_guk,
        end_cust_guk,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'wi_sales_channel_matrix_int') }}
),

source_wi_sales_channel_mtx_drop_ship AS (
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
        partner_party_site_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key
    FROM {{ source('raw', 'wi_sales_channel_mtx_drop_ship') }}
),

source_n_customer_party AS (
    SELECT
        customer_party_key,
        customer_party_site_type,
        ru_apv_hier_parent_party_key,
        ru_legal_hier_parent_party_key,
        recognized_flag,
        company_target_id_int,
        gu_party_key,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        edw_observation_datetime,
        ru_gu_type,
        sales_acct_headquarters_role
    FROM {{ source('raw', 'n_customer_party') }}
),

source_r_sales_hierarchy AS (
    SELECT
        sales_territory_key,
        l0_sales_territory_name_code,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l4_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        l7_sales_territory_name_code,
        sales_terr_effective_date,
        sales_terr_expiration_date,
        sales_coverage_code,
        sales_subcoverage_code,
        has_country_role,
        sales_territory_node_type,
        iso_country_code,
        sales_structure_ver_name,
        sales_structure_type,
        ss_erp_version_id_int,
        sales_territory_descr,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        l4_sales_territory_descr,
        l5_sales_territory_descr,
        l6_sales_territory_descr,
        l7_sales_territory_descr,
        bk_sales_territory_name,
        sales_territory_type_code,
        l1_sales_territory_sort_descr,
        l2_sales_territory_sort_descr,
        l3_sales_territory_sort_descr,
        l4_sales_territory_sort_descr,
        l5_sales_territory_sort_descr,
        l6_sales_territory_sort_descr,
        l7_sales_territory_sort_descr,
        dv_sales_terr_level_num_int
    FROM {{ source('raw', 'r_sales_hierarchy') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        bill_to_customer_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key,
        partner_bill_to_cust_party_key
    FROM source_r_sales_hierarchy
)

SELECT * FROM final