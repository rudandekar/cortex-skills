{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dca_autovalidation_hist', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_DCA_AUTOVALIDATION_HIST',
        'target_table': 'ST_DCA_AUTOVALIDATION_HIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.660950+00:00'
    }
) }}

WITH 

source_ff_dca_autovalidation_hist AS (
    SELECT
        claim_detail_id,
        claim_id,
        check_status,
        comments_rsn_txt,
        check_code,
        result_id,
        last_update_date,
        validation_reason_code,
        correction_action,
        active_flag,
        create_datetime,
        batch_id,
        action_cd
    FROM {{ source('raw', 'ff_dca_autovalidation_hist') }}
),

final AS (
    SELECT
        claim_detail_id,
        claim_id,
        check_status,
        comments_rsn_txt,
        check_code,
        result_id,
        last_update_date,
        validation_reason_code,
        correction_action,
        active_flag,
        create_datetime,
        batch_id,
        action_cd
    FROM source_ff_dca_autovalidation_hist
)

SELECT * FROM final