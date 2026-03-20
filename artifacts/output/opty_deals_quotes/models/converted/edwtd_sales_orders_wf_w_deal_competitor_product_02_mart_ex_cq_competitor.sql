{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_deal_competitor_product', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_COMPETITOR_PRODUCT',
        'target_table': 'EX_CQ_COMPETITOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.954042+00:00'
    }
) }}

WITH 

source_wi_cq_competitor AS (
    SELECT
        batch_id,
        object_id,
        deal_object_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        created_by,
        created_on,
        updated_by,
        updated_on,
        dm_update_date,
        source,
        initiated_source,
        comp_prod_eu_price,
        create_datetime,
        action_code
    FROM {{ source('raw', 'wi_cq_competitor') }}
),

source_st_cq_competitor AS (
    SELECT
        batch_id,
        object_id,
        deal_object_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        created_by,
        created_on,
        updated_by,
        updated_on,
        dm_update_date,
        source,
        initiated_source,
        comp_prod_eu_price,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cq_competitor') }}
),

source_w_deal_competitor_product AS (
    SELECT
        bk_deal_id,
        bk_src_rptd_competitor_name,
        bk_competing_prdt_desc,
        competitor_prdt_type_cd,
        additional_info_txt,
        competing_technology_desc,
        src_last_update_dtm,
        dv_source_last_update_dt,
        data_source_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_competitor_product') }}
),

final AS (
    SELECT
        batch_id,
        object_id,
        deal_object_id,
        competitor_name,
        comp_prod_name,
        comp_type,
        comp_tech,
        comp_add_info,
        created_by,
        created_on,
        updated_by,
        updated_on,
        dm_update_date,
        source,
        initiated_source,
        comp_prod_eu_price,
        create_datetime,
        action_code,
        exception_type
    FROM source_w_deal_competitor_product
)

SELECT * FROM final