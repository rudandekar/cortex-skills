{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_gpf_bts_gp_inventory_item', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_GPF_BTS_GP_INVENTORY_ITEM',
        'target_table': 'EX_XXCMF_SP_BTS_EXTR_PID',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.219362+00:00'
    }
) }}

WITH 

source_st_xxcmf_sp_bts_extr_pid AS (
    SELECT
        pid,
        pid_organization_code,
        on_hand,
        unit_cost,
        eos_date,
        orderability,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_xxcmf_sp_bts_extr_pid') }}
),

source_w_gpf_bts_gp_inventory_item AS (
    SELECT
        inventory_orgn_name_key,
        goods_product_key,
        on_hand_qty,
        unit_usd_cost,
        end_of_sale_dt,
        publication_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_gpf_bts_gp_inventory_item') }}
),

final AS (
    SELECT
        pid,
        pid_organization_code,
        on_hand,
        unit_cost,
        eos_date,
        orderability,
        publish_date,
        creation_date,
        created_by,
        batch_id,
        create_datetime,
        action_code
    FROM source_w_gpf_bts_gp_inventory_item
)

SELECT * FROM final