{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_adjustment_subreason', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_ADJUSTMENT_SUBREASON',
        'target_table': 'W_CCRM_ADJUSTMENT_SUBREASON',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.965077+00:00'
    }
) }}

WITH 

source_st_ngccrm_subreason_codes AS (
    SELECT
        batch_id,
        src_id,
        src_name,
        version,
        description,
        start_date,
        end_date,
        comments,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        latest_flag,
        status,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ngccrm_subreason_codes') }}
),

final AS (
    SELECT
        bk_ccrm_adj_subreason_cd,
        bk_ccrm_adj_subrsn_ver_num_int,
        subreason_descr,
        sk_subreason_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ngccrm_subreason_codes
)

SELECT * FROM final