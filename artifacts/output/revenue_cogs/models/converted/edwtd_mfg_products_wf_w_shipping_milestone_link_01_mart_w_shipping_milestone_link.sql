{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_shipping_milestone_link', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_SHIPPING_MILESTONE_LINK',
        'target_table': 'W_SHIPPING_MILESTONE_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.788667+00:00'
    }
) }}

WITH 

source_st_shipping_milestone_type AS (
    SELECT
        sk_gid_id,
        bk_smt_source_region_name,
        bk_smt_domain_cd,
        bk_smt_ship_to_region_name,
        bk_smt_routing_type_cd,
        organization_code,
        bk_smt_business_entity_cd,
        bk_smt_event_type_name,
        sk_gid_succ_id,
        bk_ssmt_source_region_name,
        bk_ssmt_domain_cd,
        bk_ssmt_ship_to_region_name,
        bk_ssmt_routing_type_cd,
        smst_organization_code,
        bk_ssmt_business_entity_cd,
        bk_ssmt_event_type_name,
        inter_event_drtn_seconds_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'st_shipping_milestone_type') }}
),

final AS (
    SELECT
        smt_key,
        inter_event_drtn_seconds_cnt,
        ssmt_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_shipping_milestone_type
)

SELECT * FROM final