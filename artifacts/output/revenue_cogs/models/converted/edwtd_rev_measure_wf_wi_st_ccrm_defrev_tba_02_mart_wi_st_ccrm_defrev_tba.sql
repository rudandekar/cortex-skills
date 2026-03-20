{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_st_ccrm_defrev_tba', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_ST_CCRM_DEFREV_TBA',
        'target_table': 'WI_ST_CCRM_DEFREV_TBA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.144178+00:00'
    }
) }}

WITH 

source_wi_defrev_ngccrm_mnth_cntl AS (
    SELECT
        processed_fiscal_year_mth_int
    FROM {{ source('raw', 'wi_defrev_ngccrm_mnth_cntl') }}
),

source_wi_st_ccrm_defrev_tba AS (
    SELECT
        fiscal_year_month_int,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        rev_measurement_type_cd,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source,
        def_amt,
        oa_sku_type
    FROM {{ source('raw', 'wi_st_ccrm_defrev_tba') }}
),

final AS (
    SELECT
        fiscal_year_month_int,
        profile_id,
        agreement_name,
        reason_code,
        deal_id,
        line_type,
        so_line_id,
        rev_measurement_type_cd,
        bk_service_flg,
        active_flag,
        order_source,
        order_sub_source,
        def_amt,
        oa_sku_type
    FROM source_wi_st_ccrm_defrev_tba
)

SELECT * FROM final