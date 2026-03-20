{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rtm_pos_allocation', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_RTM_POS_ALLOCATION',
        'target_table': 'WI_GL_RTM_POS_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.566525+00:00'
    }
) }}

WITH 

source_wi_gl_rtm_pos_allocation AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_comp_us_net_rev_amt
    FROM {{ source('raw', 'wi_gl_rtm_pos_allocation') }}
),

final AS (
    SELECT
        revenue_measure_key,
        dv_fiscal_year_mth_number_int,
        rev_measure_trans_type_cd,
        dv_comp_us_net_rev_amt
    FROM source_wi_gl_rtm_pos_allocation
)

SELECT * FROM final