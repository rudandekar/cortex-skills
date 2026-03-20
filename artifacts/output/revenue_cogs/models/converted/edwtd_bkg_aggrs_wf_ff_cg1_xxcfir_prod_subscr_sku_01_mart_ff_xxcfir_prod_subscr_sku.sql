{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_xxcfir_prod_subscr_sku', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_XXCFIR_PROD_SUBSCR_SKU',
        'target_table': 'FF_XXCFIR_PROD_SUBSCR_SKU',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.191008+00:00'
    }
) }}

WITH 

source_xxcfir_prod_subscr_sku AS (
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
        version_number
    FROM {{ source('raw', 'xxcfir_prod_subscr_sku') }}
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
        version_number
    FROM source_xxcfir_prod_subscr_sku
)

SELECT * FROM final