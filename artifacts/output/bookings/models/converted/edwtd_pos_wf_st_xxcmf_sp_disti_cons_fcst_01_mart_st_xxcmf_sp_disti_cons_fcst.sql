{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_disti_cons_fcst', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_DISTI_CONS_FCST',
        'target_table': 'ST_XXCMF_SP_DISTI_CONS_FCST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.444358+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_disti_cons_fcst AS (
    SELECT
        disti_parent_name,
        product_family,
        fcst_st_date,
        publish_date,
        cons_fcst_dollars,
        creation_date,
        created_by
    FROM {{ source('raw', 'ff_xxcmf_sp_disti_cons_fcst') }}
),

final AS (
    SELECT
        distributor_master_name,
        product_family_id,
        plan_week_dt,
        publication_dt,
        cnsnss_sell_thru_fcst_usd_amt,
        creation_date,
        created_by
    FROM source_ff_xxcmf_sp_disti_cons_fcst
)

SELECT * FROM final