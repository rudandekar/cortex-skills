{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_batch_source_tv_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_BATCH_SOURCE_TV_OOD',
        'target_table': 'N_AR_BATCH_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.319156+00:00'
    }
) }}

WITH 

source_n_ar_batch_source_tv AS (
    SELECT
        ar_batch_source_key,
        start_tv_dt,
        end_tv_dt,
        bk_batch_source_name,
        bk_operating_unit_name_cd,
        ar_batch_source_descr,
        batch_source_type_cd,
        ar_batch_source_start_dtm,
        ar_batch_source_end_dtm,
        ar_batch_source_status_cd,
        ss_cd,
        sk_org_id_int,
        sk_batch_source_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_ar_batch_source_tv') }}
),

source_w_ar_batch_source AS (
    SELECT
        ar_batch_source_key,
        start_tv_dt,
        end_tv_dt,
        bk_batch_source_name,
        bk_operating_unit_name_cd,
        ar_batch_source_descr,
        batch_source_type_cd,
        ar_batch_source_start_dtm,
        ar_batch_source_end_dtm,
        ar_batch_source_status_cd,
        ss_cd,
        sk_org_id_int,
        sk_batch_source_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_batch_source') }}
),

final AS (
    SELECT
        ar_batch_source_key,
        bk_batch_source_name,
        bk_operating_unit_name_cd,
        ar_batch_source_descr,
        batch_source_type_cd,
        ar_batch_source_start_dtm,
        ar_batch_source_end_dtm,
        ar_batch_source_status_cd,
        ss_cd,
        sk_org_id_int,
        sk_batch_source_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_ar_batch_source
)

SELECT * FROM final