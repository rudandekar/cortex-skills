{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_aud_rstd_gl_rev_meas_det', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_AUD_RSTD_GL_REV_MEAS_DET',
        'target_table': 'WI_AUD_RSTD_REV_THRSHLD_VALUES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.836789+00:00'
    }
) }}

WITH 

source_wi_aud_rstd_gl_rev_meas_det AS (
    SELECT
        audit_table_name,
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'wi_aud_rstd_gl_rev_meas_det') }}
),

transformed_exptrans AS (
    SELECT
    audit_table_name,
    audit_column_name,
    missing_keys_count,
    IFF(MISSING_KEYS_COUNT >= 1 , ABORT('THERE ARE MISSING MEASURE KEY FOR THE CURRENT OR PREVIOUS FISCAL MONTH' ) ) AS validate_missing_keys
    FROM source_wi_aud_rstd_gl_rev_meas_det
),

filtered_filtrans AS (
    SELECT *
    FROM transformed_exptrans
    WHERE FALSE
),

final AS (
    SELECT
        audit_table_name,
        audit_column_name,
        min_threshold_value,
        max_threshold_value,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM filtered_filtrans
)

SELECT * FROM final