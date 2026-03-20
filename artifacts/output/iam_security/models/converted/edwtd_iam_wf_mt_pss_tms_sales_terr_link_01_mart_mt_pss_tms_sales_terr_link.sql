{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pss_tms_sales_terr_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_PSS_TMS_SALES_TERR_LINK',
        'target_table': 'MT_PSS_TMS_SALES_TERR_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.745801+00:00'
    }
) }}

WITH 

source_wi_mbr_pss_tms_lnk AS (
    SELECT
        pec,
        dv_goal_type,
        product_or_service_cd,
        commission_status,
        service_flg,
        metric_type,
        ro_flag,
        buying_program,
        business_unit,
        sub_business_unit,
        program_brand,
        sales_motion,
        fiscal_year,
        dd_bk_tech_mkt_segment_name,
        dd_tms_hierarchy_type,
        dd_tms_level_num_int,
        product_classification
    FROM {{ source('raw', 'wi_mbr_pss_tms_lnk') }}
),

final AS (
    SELECT
        dd_fiscal_year_num_int,
        quota_name,
        pec_name,
        product_or_service_cd,
        dv_goal_type,
        commission_status,
        dd_tms_hierarchy_type,
        dd_tms_level_num_int,
        dd_bk_tech_mkt_segment_name,
        service_flg,
        metric_type_code,
        ro_tms_flag,
        business_unit_descr,
        sub_business_unit_descr,
        sales_motion_code,
        buying_program_group_name,
        program_branded,
        product_classification
    FROM source_wi_mbr_pss_tms_lnk
)

SELECT * FROM final