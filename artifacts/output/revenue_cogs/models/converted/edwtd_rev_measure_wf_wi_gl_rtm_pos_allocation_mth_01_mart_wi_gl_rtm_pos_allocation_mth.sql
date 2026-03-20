{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rtm_pos_allocation_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_RTM_POS_ALLOCATION_MTH',
        'target_table': 'WI_GL_RTM_POS_ALLOCATION_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.605043+00:00'
    }
) }}

WITH 

source_wi_gl_rtm_pos_allocation_mth AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_comp_us_net_rev_amt
    FROM {{ source('raw', 'wi_gl_rtm_pos_allocation_mth') }}
),

final AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_comp_us_net_rev_amt
    FROM source_wi_gl_rtm_pos_allocation_mth
)

SELECT * FROM final