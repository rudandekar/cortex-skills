{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_partner_plus_assignment_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_PARTNER_PLUS_ASSIGNMENT_STG23NF',
        'target_table': 'N_PARTNER_PLUS_ASSIGNMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.706101+00:00'
    }
) }}

WITH 

source_st_tbl_partner_plus_mapping1 AS (
    SELECT
        country,
        be_geo_id,
        tier,
        fiscal_quarter_id,
        fiscal_quarter_number_int
    FROM {{ source('raw', 'st_tbl_partner_plus_mapping1') }}
),

final AS (
    SELECT
        bk_iso_country_cd,
        bk_be_geo_id_int,
        partner_plus_tier_name,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_qtr_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tbl_partner_plus_mapping1
)

SELECT * FROM final