{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxcfir_prod_subscr_sku1', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXCFIR_PROD_SUBSCR_SKU1',
        'target_table': 'ST_CG1_XXCFIR_PROD_SUBSCR_SKU',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.270403+00:00'
    }
) }}

WITH 

source_ff_xxcfir_prod_subscr_sku1 AS (
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
    FROM {{ source('raw', 'ff_xxcfir_prod_subscr_sku1') }}
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
    FROM source_ff_xxcfir_prod_subscr_sku1
)

SELECT * FROM final