{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_claim_gaining_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CLAIM_GAINING_AGENT',
        'target_table': 'N_CLAIM_GAINING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.363070+00:00'
    }
) }}

WITH 

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
        edw_update_user
    FROM source_w_claim_gaining_agent
)

SELECT * FROM final