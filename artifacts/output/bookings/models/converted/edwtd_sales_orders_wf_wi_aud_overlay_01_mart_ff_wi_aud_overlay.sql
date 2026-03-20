{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_overlay', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_OVERLAY',
        'target_table': 'FF_WI_AUD_OVERLAY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.626401+00:00'
    }
) }}

WITH 

source_mt_overlay_drr_ng AS (
    SELECT
        overlay_bookings_measure_key,
        overlay_bookings_process_date,
        ovrly_bkgs_msr_trans_type_code,
        dv_end_cust_party_key,
        bk_sa_member_id_int,
        link_customer_party_key,
        sales_account_group_party_key,
        dv_end_cust_reason_descr,
        dv_end_cust_ownership_splt_pct,
        bk_eac_org_id,
        bk_eac_group_id,
        bk_eac_rule_id,
        bk_sales_rep_num,
        sales_territory_key,
        dv_rstmt_reason_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        last_update_date,
        process_date,
        process_type,
        parent_eac_rule_id,
        profile_chk_id
    FROM {{ source('raw', 'mt_overlay_drr_ng') }}
),

final AS (
    SELECT
        overlay_bookings_measure_key,
        overlay_bookings_process_date,
        ovrly_bkgs_msr_trans_type_code,
        bk_eac_org_id,
        sum_splt,
        process_type,
        profile_chk_id
    FROM source_mt_overlay_drr_ng
)

SELECT * FROM final