{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_xxcfir_prod_subscr_sku', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_XXCFIR_PROD_SUBSCR_SKU',
        'target_table': 'EL_CG1_XXCFIR_PROD_SUBSCR_SKU',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.472201+00:00'
    }
) }}

WITH 

source_el_cg1_xxcfir_prod_subscr_sku AS (
    SELECT
        sku_id,
        sku,
        accounting_rule,
        duration,
        start_date,
        end_date,
        inventory_item_id,
        description,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        disable_comments,
        version_number,
        biz_def_term
    FROM {{ source('raw', 'el_cg1_xxcfir_prod_subscr_sku') }}
),

final AS (
    SELECT
        sku_id,
        sku,
        accounting_rule,
        duration,
        start_date,
        end_date,
        inventory_item_id,
        description,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        disable_comments,
        version_number,
        biz_def_term
    FROM source_el_cg1_xxcfir_prod_subscr_sku
)

SELECT * FROM final