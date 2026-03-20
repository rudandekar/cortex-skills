{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_inventory_item_v1', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_INVENTORY_ITEM_V1',
        'target_table': 'N_INVENTORY_ITEM_V1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.489239+00:00'
    }
) }}

WITH 

source_n_inventory_item_v1_tv AS (
    SELECT
        inventory_orgn_name_key,
        item_key,
        sk_inventory_item_id_int,
        sk_organization_id_int,
        asn_autoexpire_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm
    FROM {{ source('raw', 'n_inventory_item_v1_tv') }}
),

final AS (
    SELECT
        inventory_orgn_name_key,
        item_key,
        sk_inventory_item_id_int,
        sk_organization_id_int,
        asn_autoexpire_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_inventory_item_v1_tv
)

SELECT * FROM final