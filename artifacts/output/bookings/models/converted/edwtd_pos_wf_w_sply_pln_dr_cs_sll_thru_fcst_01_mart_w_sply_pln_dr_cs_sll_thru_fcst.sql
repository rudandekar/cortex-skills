{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sply_pln_dr_cs_sll_thru_fcst', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_W_SPLY_PLN_DR_CS_SLL_THRU_FCST',
        'target_table': 'W_SPLY_PLN_DR_CS_SLL_THRU_FCST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.668956+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_disti_cons_fcst AS (
    SELECT
        distributor_master_name,
        product_family_id,
        plan_week_dt,
        publication_dt,
        cnsnss_sell_thru_fcst_usd_amt,
        creation_date,
        created_by
    FROM {{ source('raw', 'st_xxcmf_sp_disti_cons_fcst') }}
),

transformed_exptrans AS (
    SELECT
    distributor_master_name,
    product_family_id,
    plan_week_dt,
    publication_dt,
    cnsnss_sell_thru_fcst_usd_amt,
    creation_date,
    created_by,
    'I' AS action_code,
    'I' AS dml_type
    FROM source_st_xxcmf_sp_disti_cons_fcst
),

final AS (
    SELECT
        bk_distributor_master_name,
        bk_product_family_id,
        bk_plan_week_dt,
        bk_publication_dt,
        cnsnss_sell_thru_fcst_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exptrans
)

SELECT * FROM final