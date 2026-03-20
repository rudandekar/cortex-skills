{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_orgntr_org_named_ptnr_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_ORGNTR_ORG_NAMED_PTNR_STG23NF',
        'target_table': 'N_POS_ORGNTR_ORG_NAMED_PTNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.461131+00:00'
    }
) }}

WITH 

source_st_tbl_disti_npl_mapping1 AS (
    SELECT
        fiscal_calendar_code,
        fiscal_year_number_int,
        fiscal_quarter_number_int,
        bk_iso_country_code,
        be_geo_id,
        profile_id,
        npl_flag
    FROM {{ source('raw', 'st_tbl_disti_npl_mapping1') }}
),

final AS (
    SELECT
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_qtr_num_int,
        bk_iso_country_cd,
        bk_be_geo_id_int,
        bk_wips_originator_id_int,
        npl_flg,
        fiscal_year_qtr_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tbl_disti_npl_mapping1
)

SELECT * FROM final