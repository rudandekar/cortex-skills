{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sca_rte_trx_oly_rtnr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SCA_RTE_TRX_OLY_RTNR',
        'target_table': 'SM_SCA_RTE_OLY_RTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.396482+00:00'
    }
) }}

WITH 

source_sm_sca_rte_oly_rtnr AS (
    SELECT
        sales_credit_assgn_rte_oly_key,
        ep_trx_split_sc_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sca_rte_oly_rtnr') }}
),

final AS (
    SELECT
        sales_credit_assgn_rte_oly_key,
        ep_trx_split_sc_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_sca_rte_oly_rtnr
)

SELECT * FROM final