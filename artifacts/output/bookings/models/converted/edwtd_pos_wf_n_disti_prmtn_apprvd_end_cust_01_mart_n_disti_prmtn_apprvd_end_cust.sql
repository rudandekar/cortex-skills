{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_apprvd_end_cust', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_APPRVD_END_CUST',
        'target_table': 'N_DISTI_PRMTN_APPRVD_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.342417+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_apprvd_end_cust AS (
    SELECT
        customer_party_key,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_disti_prmtn_apprvd_end_cust') }}
),

final AS (
    SELECT
        customer_party_key,
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_disti_prmtn_apprvd_end_cust
)

SELECT * FROM final