{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_adj_reason_element_type', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_ADJ_REASON_ELEMENT_TYPE',
        'target_table': 'N_CCRM_ADJ_REASON_ELEMENT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.047779+00:00'
    }
) }}

WITH 

source_w_ccrm_adj_reason_element_type AS (
    SELECT
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_ccrm_element_type_cd,
        no_compnstn_until_deferred_flg,
        compnstn_calc_not_elig_flg,
        advance_booking_elig_flg,
        advance_compnstn_elig_flg,
        booking_deferral_flg,
        cogs_deferral_flg,
        revenue_deferral_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_adj_reason_element_type') }}
),

final AS (
    SELECT
        bk_ccrm_adjustment_reason_cd,
        bk_ccrm_adj_reason_ver_num_int,
        bk_ccrm_element_type_cd,
        no_compnstn_until_deferred_flg,
        compnstn_calc_not_elig_flg,
        advance_booking_elig_flg,
        advance_compnstn_elig_flg,
        booking_deferral_flg,
        cogs_deferral_flg,
        revenue_deferral_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_adj_reason_element_type
)

SELECT * FROM final