{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_sites_d', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_SITES_D',
        'target_table': 'EL_TSS_SITES_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.850389+00:00'
    }
) }}

WITH 

source_st_tss_sites_d_cogs AS (
    SELECT
        party_number,
        party_site_id,
        party_id,
        bl_site_key,
        party_name,
        address1,
        address2,
        address3,
        address4,
        city,
        state,
        country,
        postal_code,
        theater,
        time_zone,
        ship_to_flag,
        creation_date,
        pst_creation_date,
        bl_last_update_date,
        cust_account_id,
        cust_acct_site_id,
        cust_theater,
        cust_country
    FROM {{ source('raw', 'st_tss_sites_d_cogs') }}
),

final AS (
    SELECT
        place_id,
        name,
        created_dt,
        last_updated_dt,
        last_updated_tm,
        update_date,
        create_date,
        update_user,
        create_user,
        last_updated_dttm,
        whos_place,
        address,
        city,
        state_prov,
        country,
        phone,
        status,
        created_tm,
        last_updated_id,
        rest_of_name,
        second_address,
        third_address,
        fourth_address,
        zippost,
        phy_svc_grp_a,
        global_name,
        calendar_id,
        x_coordinate,
        gmt_offset,
        created_dt_gmt,
        oracle_addr_id,
        source_system,
        mtx_place_id,
        party_id,
        bl_site_key,
        cust_account_id,
        cust_acct_site_id,
        cust_theater,
        cust_country
    FROM source_st_tss_sites_d_cogs
)

SELECT * FROM final