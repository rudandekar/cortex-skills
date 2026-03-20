{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_incr_distinct_sca_flag', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_INCR_DISTINCT_SCA_FLAG',
        'target_table': 'WI_INCR_DISTINCT_SCA_FLAG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.175524+00:00'
    }
) }}

WITH 

source_wi_incr_distinct_sca_flag AS (
    SELECT
        sales_credit_assignment_key,
        sk_line_seq_id_int,
        ss_cd,
        sc_open_flag,
        exists_flag_in_mt_rev,
        exists_flag_in_mt_bkg
    FROM {{ source('raw', 'wi_incr_distinct_sca_flag') }}
),

source_wi_all_sca_of_distinct_line_sq AS (
    SELECT
        sales_credit_assignment_key,
        sca_source_type_cd,
        sk_line_seq_id_int,
        ss_cd,
        ep_source_line_id_int,
        start_tv_dtm,
        end_tv_dtm,
        sk_sc_agent_id_int
    FROM {{ source('raw', 'wi_all_sca_of_distinct_line_sq') }}
),

final AS (
    SELECT
        sales_credit_assignment_key,
        sk_line_seq_id_int,
        ss_cd,
        sc_open_flag,
        exists_flag_in_mt_rev,
        exists_flag_in_mt_bkg,
        sk_sc_agent_id_int
    FROM source_wi_all_sca_of_distinct_line_sq
)

SELECT * FROM final