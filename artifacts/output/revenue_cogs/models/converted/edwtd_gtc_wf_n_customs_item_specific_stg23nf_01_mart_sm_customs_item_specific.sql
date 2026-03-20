{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_customs_item_specific_stg23nf', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_N_CUSTOMS_ITEM_SPECIFIC_STG23NF',
        'target_table': 'SM_CUSTOMS_ITEM_SPECIFIC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.339699+00:00'
    }
) }}

WITH 

source_sm_customs_item_specific AS (
    SELECT
        customs_item_specific_key,
        sk_specific_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_customs_item_specific') }}
),

source_n_customs_item_specific AS (
    SELECT
        bk_item_specific_name,
        item_specific_descr,
        source_deleted_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_specific_id_int,
        customs_item_specific_key,
        item_clsfctn_method_cd
    FROM {{ source('raw', 'n_customs_item_specific') }}
),

final AS (
    SELECT
        customs_item_specific_key,
        sk_specific_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_n_customs_item_specific
)

SELECT * FROM final