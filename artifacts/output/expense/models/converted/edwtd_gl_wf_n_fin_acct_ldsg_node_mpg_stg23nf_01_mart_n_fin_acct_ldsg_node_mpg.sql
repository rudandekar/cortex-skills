{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_acct_ldsg_node_mpg_stg23nf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FIN_ACCT_LDSG_NODE_MPG_STG23NF',
        'target_table': 'N_FIN_ACCT_LDSG_NODE_MPG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.713039+00:00'
    }
) }}

WITH 

source_st_apsp_fin_account_ldsg AS (
    SELECT
        account_num,
        spend_roll_up,
        spend_category,
        spend_group
    FROM {{ source('raw', 'st_apsp_fin_account_ldsg') }}
),

final AS (
    SELECT
        bk_ldsg_node_name,
        bk_financial_account_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_apsp_fin_account_ldsg
)

SELECT * FROM final