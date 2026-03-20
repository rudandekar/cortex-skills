{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_claim_gaining_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CLAIM_GAINING_AGENT',
        'target_table': 'W_CLAIM_GAINING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.102058+00:00'
    }
) }}

WITH 

source_st_xxnce_gaining_agent AS (
    SELECT
        gn_agent_id,
        gaining_node_id,
        gaining_salesrep_number,
        split_percent,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        claim_id,
        node_id_level_1,
        node_id_level_2,
        node_id_level_3,
        node_id_level_4,
        node_id_level_5,
        node_id_level_6,
        node_id_level_7,
        node_id_level_8,
        node_id_level_9,
        node_id_level_10,
        node_id_level_11
    FROM {{ source('raw', 'st_xxnce_gaining_agent') }}
),

source_w_claim_gaining_agent AS (
    SELECT
        claim_gaining_agent_key,
        sk_claim_gaining_agent_id,
        claim_key,
        bk_claim_gaining_node_id_int,
        bk_sales_rep_num,
        split_pct,
        claim_gaining_agent_email_txt,
        sales_territory_key,
        create_dtm,
        last_update_dtm,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_claim_gaining_agent') }}
),

transformed_exp_xxnce_gaining_agent AS (
    SELECT
    gn_agent_id,
    gaining_node_id,
    gaining_salesrep_number,
    split_percent,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_updated_date,
    claim_id,
    node_id_level_1,
    node_id_level_2,
    node_id_level_3,
    node_id_level_4,
    node_id_level_5,
    node_id_level_6,
    node_id_level_7,
    node_id_level_8,
    node_id_level_9,
    node_id_level_10,
    node_id_level_11,
    exception_type
    FROM source_w_claim_gaining_agent
),

final AS (
    SELECT
        claim_gaining_agent_key,
        sk_claim_gaining_agent_id,
        claim_key,
        bk_claim_gaining_node_id_int,
        bk_sales_rep_num,
        split_pct,
        claim_gaining_agent_email_txt,
        sales_territory_key,
        create_dtm,
        last_update_dtm,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM transformed_exp_xxnce_gaining_agent
)

SELECT * FROM final