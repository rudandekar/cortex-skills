{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxcmf_sp_disti_item_attributes', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_XXCMF_SP_DISTI_ITEM_ATTRIBUTES',
        'target_table': 'ST_XXCMF_SP_DISTI_ITEM_ATTRI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.984605+00:00'
    }
) }}

WITH 

source_ff_xxcmf_sp_disti_item_attributes AS (
    SELECT
        disti_parent_name,
        sku,
        publish_date,
        planning_segment,
        transit_days,
        target_wos,
        wpl,
        creation_date,
        created_by
    FROM {{ source('raw', 'ff_xxcmf_sp_disti_item_attributes') }}
),

final AS (
    SELECT
        distributor_master_name,
        product_key,
        publication_dt,
        disti_product_plng_segment_cd,
        dist_prd_whlsl_prc_lst_usd_amt,
        dist_prd_trnst_days_settng_cnt,
        dist_prd_weeks_of_sply_tgt_pct,
        creation_date,
        created_by
    FROM source_ff_xxcmf_sp_disti_item_attributes
)

SELECT * FROM final