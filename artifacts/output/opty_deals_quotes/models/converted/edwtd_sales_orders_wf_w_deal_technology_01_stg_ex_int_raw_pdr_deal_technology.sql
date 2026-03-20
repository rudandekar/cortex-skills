{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_deal_technology', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_DEAL_TECHNOLOGY',
        'target_table': 'EX_INT_RAW_PDR_DEAL_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:26:59.877361+00:00'
    }
) }}

WITH 

source_w_deal_technology AS (
    SELECT
        deal_technology_key,
        sk_object_id_int,
        bk_deal_id,
        bk_deal_tech_name,
        bk_tech_mix_pct,
        src_created_dtm,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_deal_technology') }}
),

source_st_int_raw_pdr_deal_technology AS (
    SELECT
        object_id,
        deal_object_id,
        created_by,
        created_on,
        percentage_of_mix,
        technology,
        updated_by,
        updated_on
    FROM {{ source('raw', 'st_int_raw_pdr_deal_technology') }}
),

final AS (
    SELECT
        object_id,
        deal_object_id,
        created_by,
        created_on,
        percentage_of_mix,
        technology,
        updated_by,
        updated_on
    FROM source_st_int_raw_pdr_deal_technology
)

SELECT * FROM final