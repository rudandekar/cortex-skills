{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_material_transaction_type_src23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MATERIAL_TRANSACTION_TYPE_SRC23NF',
        'target_table': 'N_MATERIAL_TRANSACTION_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.827168+00:00'
    }
) }}

WITH 

source_cg1_mtl_transaction_type AS (
    SELECT
        transaction_type_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        transaction_type_name,
        description,
        transaction_action_id,
        transaction_source_type_id,
        shortage_msg_background_flag,
        shortage_msg_online_flag,
        disable_date,
        user_defined_flag,
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
        attribute_category,
        type_class,
        status_control_flag,
        location_required_flag,
        global_name,
        ges_update_date
    FROM {{ source('raw', 'cg1_mtl_transaction_type') }}
),

transformed_exptrans AS (
    SELECT
    transaction_type_name,
    disable_date,
    description,
    transaction_type_id,
    transaction_source_type_id,
    IFF(ISNULL(DESCRIPTION),'UNKNOWN',DESCRIPTION) AS o_transaction_type_descr,
    TO_INTEGER(TRANSACTION_TYPE_ID) AS o_transaction_type_id,
    TO_INTEGER(TRANSACTION_SOURCE_TYPE_ID) AS o_transaction_source_type_id,
    CURRENT_DATE() AS edw_update_dtm
    FROM source_cg1_mtl_transaction_type
),

lookup_lkp_n_material_trx_src_types AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exptrans a
    LEFT JOIN {{ source('raw', 'n_material_trnsctn_source_type') }} b
        ON a.transaction_source_type_id = b.transaction_source_type_id
),

update_strategy_upd_n_material_transaction_type_update AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM lookup_lkp_n_material_trx_src_types
    WHERE DD_UPDATE != 3
),

update_strategy_upd_n_material_transaction_type_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_n_material_transaction_type_update
    WHERE DD_INSERT != 3
),

routed_rtr_n_material_transaction_type AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM update_strategy_upd_n_material_transaction_type_insert
),

lookup_lkp_n_material_transaction_type AS (
    SELECT
        a.*,
        b.*
    FROM routed_rtr_n_material_transaction_type a
    LEFT JOIN {{ source('raw', 'n_material_transaction_type') }} b
        ON a.transaction_type_id = b.transaction_type_id
),

final AS (
    SELECT
        bk_transaction_type_name,
        disable_dt,
        transaction_type_descr,
        sk_transaction_type_id_int,
        bk_source_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_n_material_transaction_type
)

SELECT * FROM final