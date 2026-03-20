{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_svc_gm_by_be', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SVC_GM_BY_BE',
        'target_table': 'W_SVC_GM_BY_BE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.259006+00:00'
    }
) }}

WITH 

source_w_svc_gm_by_be AS (
    SELECT
        bk_sub_business_entity_name,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_business_entity_type_cd,
        bk_fiscal_month_num_int,
        product_key,
        bk_data_set_cd,
        bk_measure_name,
        split_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_svc_gm_by_be') }}
),

final AS (
    SELECT
        bk_sub_business_entity_name,
        bk_fiscal_year_num_int,
        bk_fiscal_calendar_cd,
        bk_business_entity_type_cd,
        bk_fiscal_month_num_int,
        product_key,
        bk_data_set_cd,
        bk_measure_name,
        split_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_svc_gm_by_be
)

SELECT * FROM final