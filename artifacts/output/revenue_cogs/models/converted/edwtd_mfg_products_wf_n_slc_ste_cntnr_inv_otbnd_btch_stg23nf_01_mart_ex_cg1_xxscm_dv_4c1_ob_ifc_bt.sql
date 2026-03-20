{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_slc_ste_cntnr_inv_otbnd_btch_stg23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SLC_STE_CNTNR_INV_OTBND_BTCH_STG23NF',
        'target_table': 'EX_CG1_XXSCM_DV_4C1_OB_IFC_BT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.211136+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_dv_4c1_ob_iface AS (
    SELECT
        message_id,
        cycle_count_id,
        organization_name,
        order_number,
        ship_set_number,
        slc_site_name,
        requested_date,
        age_bucket,
        percent_evaluation,
        selected_order_shipset,
        message_status,
        handling_unit_id,
        random_number,
        part_duns_number,
        cisco_duns_number,
        retry_count,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_dv_4c1_ob_iface') }}
),

update_strategy_upd_update AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM source_st_cg1_xxscm_dv_4c1_ob_iface
    WHERE DD_UPDATE != 3
),

update_strategy_upd_ins AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_update
    WHERE DD_INSERT != 3
),

transformed_exp_upd AS (
    SELECT
    cycle_count_id,
    ship_set_number,
    slc_site_name,
    handling_unit_id,
    sales_order_key,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM update_strategy_upd_ins
),

transformed_exp_ins AS (
    SELECT
    cycle_count_id,
    ship_set_number,
    slc_site_name,
    handling_unit_id,
    sales_order_key
    FROM transformed_exp_upd
),

final AS (
    SELECT
        message_id,
        cycle_count_id,
        organization_name,
        order_number,
        ship_set_number,
        slc_site_name,
        requested_date,
        age_bucket,
        percent_evaluation,
        selected_order_shipset,
        message_status,
        handling_unit_id,
        random_number,
        part_duns_number,
        cisco_duns_number,
        retry_count,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        exception_type
    FROM transformed_exp_ins
)

SELECT * FROM final