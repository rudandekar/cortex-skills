{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cust_acct_with_wips_distri_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_CUST_ACCT_WITH_WIPS_DISTRI_STG23NF',
        'target_table': 'N_CUST_ACCT_WITH_WIPS_DISTRI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.938595+00:00'
    }
) }}

WITH 

source_st_wips_disti_cbn_mapping_intf AS (
    SELECT
        id,
        cbn_cid,
        disti_profile_id
    FROM {{ source('raw', 'st_wips_disti_cbn_mapping_intf') }}
),

final AS (
    SELECT
        customer_acct_key,
        sk_cbn_mapping_id_int,
        bk_wips_originator_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_wips_disti_cbn_mapping_intf
)

SELECT * FROM final