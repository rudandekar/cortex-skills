{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_direct_corp_adj_type_lvl1', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WK_DIRECT_CORP_ADJ_TYPE_LVL1',
        'target_table': 'W_DIRECT_CORP_ADJ_TYPE_LVL1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.485832+00:00'
    }
) }}

WITH 

source_st_om_cfi_adj_lookups AS (
    SELECT
        batch_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        created_by,
        creation_date,
        description,
        enabled_flag,
        end_date,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        lookup_id,
        lookup_type,
        lookup_value,
        start_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cfi_adj_lookups') }}
),

final AS (
    SELECT
        bk_direct_corp_adj_lvl1_typ_cd,
        direct_corp_adj_lvl1_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_om_cfi_adj_lookups
)

SELECT * FROM final