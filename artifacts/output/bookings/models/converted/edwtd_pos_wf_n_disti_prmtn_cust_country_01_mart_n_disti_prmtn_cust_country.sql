{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_prmtn_cust_country', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_PRMTN_CUST_COUNTRY',
        'target_table': 'N_DISTI_PRMTN_CUST_COUNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.351337+00:00'
    }
) }}

WITH 

source_w_disti_prmtn_cust_country AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_disti_prmtn_cust_type_cd,
        bk_iso_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_disti_prmtn_cust_country') }}
),

final AS (
    SELECT
        bk_promotion_num,
        bk_promotion_revision_num_int,
        bk_promotion_type_cd,
        bk_disti_prmtn_cust_type_cd,
        bk_iso_country_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_disti_prmtn_cust_country
)

SELECT * FROM final