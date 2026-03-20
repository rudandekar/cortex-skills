{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_el_int_dmr_dm_tm_asgnmnt_rnwl_kfka', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_EL_INT_DMR_DM_TM_ASGNMNT_RNWL_KFKA',
        'target_table': 'EL_INT_DMR_DM_TM_ASGINMNT_RNWL_KFKA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.900643+00:00'
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
        created_on,
        created_by,
        updated_on,
        updated_by,
        active,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'el_int_dmr_dm_tm_asginmnt_rnwl_kfka') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        quote_object_id,
        member_cco_id,
        member_role,
        created_on,
        created_by,
        updated_on,
        updated_by,
        active,
        updated_by_kafka,
        updated_on_kafka,
        offset_number,
        partition_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_el_int_dmr_dm_tm_asginmnt_rnwl_kfka
)

SELECT * FROM final