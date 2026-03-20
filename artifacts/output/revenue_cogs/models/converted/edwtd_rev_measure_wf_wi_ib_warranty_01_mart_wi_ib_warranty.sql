{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ib_warranty', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_IB_WARRANTY',
        'target_table': 'WI_IB_WARRANTY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.008247+00:00'
    }
) }}

WITH 

source_wi_ib_warranty AS (
    SELECT
        business_entity_descr,
        inwinc,
        inwmc,
        inwooc,
        mwinc,
        mwmc,
        mwooc,
        oowinc,
        oowmc,
        ib_ratio
    FROM {{ source('raw', 'wi_ib_warranty') }}
),

final AS (
    SELECT
        business_entity_descr,
        inwinc,
        inwmc,
        inwooc,
        mwinc,
        mwmc,
        mwooc,
        oowinc,
        oowmc,
        ib_ratio
    FROM source_wi_ib_warranty
)

SELECT * FROM final