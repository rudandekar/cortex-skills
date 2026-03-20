{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_partner_dtl', 'batch', 'edwtdp_ww_bookings'],
    meta={
        'source_workflow': 'wf_m_R_PARTNER_DTL',
        'target_table': 'R_PARTNER_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.055390+00:00'
    }
) }}

WITH 

source_bv_partners_hierarchy_pk AS (
    SELECT
        partner_site_party_key,
        bk_party_id_int,
        partner_country_party_key,
        dd_grndparnt_partner_party_key,
        primary_name,
        status_code,
        party_ssot_party_id_int,
        party_type,
        customer_role,
        partner_country_group_role,
        cisco_worker_role,
        suspect_party_role,
        prospect_party_role,
        lead_party_role,
        opportunity_party_role,
        partner_site_role,
        partner_role,
        competitor_role,
        vendor_role,
        origin_source_system_code,
        origin_source_system_date,
        bk_iso_country_code,
        partnr_country_registered_type,
        bk_be_geo_id_int,
        partner_party_key,
        partner_type_code,
        partner_subtype_code,
        ru_qualification_code,
        subtype_description,
        type_description,
        qualification_description,
        qualification_type,
        dv_smb_select_flag,
        dv_vip_partner_flag,
        qualification_type_description,
        channel_partner_name,
        channel_partner_cntry_name
    FROM {{ source('raw', 'bv_partners_hierarchy_pk') }}
),

final AS (
    SELECT
        partner_site_party_key,
        partner_subtype_code,
        partner_type_code,
        certification_code,
        channel_partner_name
    FROM source_bv_partners_hierarchy_pk
)

SELECT * FROM final