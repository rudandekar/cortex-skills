{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_item_source_system_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_ITEM_SOURCE_SYSTEM_STG23NF',
        'target_table': 'N_CUSTOMS_ITEM_SOURCE_SYSTEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.439053+00:00'
    }
) }}

WITH 

source_n_customs_item_source_system AS (
    SELECT
        bk_customs_item_src_system_cd,
        customs_item_source_sys_descr,
        source_sys_effective_start_dt,
        source_sys_effective_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_customs_item_source_system') }}
),

final AS (
    SELECT
        bk_customs_item_src_system_cd,
        customs_item_source_sys_descr,
        source_sys_effective_start_dt,
        source_sys_effective_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_customs_item_source_system
)

SELECT * FROM final