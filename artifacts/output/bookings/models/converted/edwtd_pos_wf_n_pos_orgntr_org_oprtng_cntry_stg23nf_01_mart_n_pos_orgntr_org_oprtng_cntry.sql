{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_orgntr_org_oprtng_cntry_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_ORGNTR_ORG_OPRTNG_CNTRY_STG23NF',
        'target_table': 'N_POS_ORGNTR_ORG_OPRTNG_CNTRY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.622482+00:00'
    }
) }}

WITH 

source_st_wips_disti_supported_info AS (
    SELECT
        disti_profile_id,
        country,
        supported_flag,
        contractual_flag,
        legal_flag,
        last_updated_date
    FROM {{ source('raw', 'st_wips_disti_supported_info') }}
),

final AS (
    SELECT
        bk_wips_originator_id_int,
        bk_iso_country_cd,
        supported_flg,
        contractual_flg,
        legal_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_wips_disti_supported_info
)

SELECT * FROM final