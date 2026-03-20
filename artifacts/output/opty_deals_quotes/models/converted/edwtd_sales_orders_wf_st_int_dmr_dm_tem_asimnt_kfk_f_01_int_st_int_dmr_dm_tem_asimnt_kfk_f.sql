{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_st_int_dmr_dm_tem_asimnt_kfk_f', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_INT_DMR_DM_TEM_ASIMNT_KFK_F',
        'target_table': 'ST_INT_DMR_DM_TEM_ASIMNT_KFK_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.918138+00:00'
    }
) }}

WITH 

source_ff_st_int_dmr_dm_team_assignment_kafka AS (
    SELECT
        parent_id,
        member_cec_id,
        member_cco_id,
        member_first_name,
        member_last_name,
        member_role,
        member_access_level,
        access_grantor_id,
        email_cc,
        email_sent_flag,
        member_access_scope,
        member_id,
        member_visibility,
        member_position,
        object_id,
        member_type,
        quote_object_id,
        active,
        created_by,
        created_on,
        edw_updated_date,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number
    FROM {{ source('raw', 'ff_st_int_dmr_dm_team_assignment_kafka') }}
),

final AS (
    SELECT
        parent_id,
        member_cec_id,
        member_cco_id,
        member_first_name,
        member_last_name,
        member_role,
        member_access_level,
        access_grantor_id,
        email_cc,
        email_sent_flag,
        member_access_scope,
        member_id,
        member_visibility,
        object_id,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        message_sequence_number,
        member_position,
        member_type,
        quote_object_id,
        active,
        created_by,
        created_on,
        edw_updated_date
    FROM source_ff_st_int_dmr_dm_team_assignment_kafka
)

SELECT * FROM final