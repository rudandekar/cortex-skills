{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dmr_dm_tm_asgnmnt_rnwl_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DMR_DM_TM_ASGNMNT_RNWL_KFKA',
        'target_table': 'ST_INT_DMR_DM_TEAM_ASSIGNMENT_RNWL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.954971+00:00'
    }
) }}

WITH 

source_el_int_dmr_dm_tm_asginmnt_rnwl_kfka AS (
    SELECT
        object_id,
        deal_object_id,
        quote_object_id,
        member_cco_id,
        member_role,
        created_by,
        created_on,
        updated_by,
        updated_on,
        active,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'el_int_dmr_dm_tm_asginmnt_rnwl_kfka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        quote_object_id,
        access_grantor_id,
        member_id,
        member_cec_id,
        member_cco_id,
        member_first_name,
        member_last_name,
        member_role,
        member_access_level,
        member_access_scope,
        member_type,
        member_visibility,
        member_position,
        email_sent_flag,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on,
        email_cc,
        edw_updated_date
    FROM source_el_int_dmr_dm_tm_asginmnt_rnwl_kfka
)

SELECT * FROM final