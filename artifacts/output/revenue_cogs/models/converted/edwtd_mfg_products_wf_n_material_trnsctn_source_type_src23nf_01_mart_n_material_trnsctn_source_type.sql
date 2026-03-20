{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_material_trnsctn_source_type_src23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_MATERIAL_TRNSCTN_SOURCE_TYPE_SRC23NF',
        'target_table': 'N_MATERIAL_TRNSCTN_SOURCE_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.089691+00:00'
    }
) }}

WITH 

source_cg1_mtl_txn_source_types AS (
    SELECT
        transaction_source_type_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        transaction_source_type_name,
        description,
        disable_date,
        user_defined_flag,
        validated_flag,
        descriptive_flex_context_code,
        attribute_category,
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
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        transaction_source_category,
        transaction_source,
        global_name,
        ges_update_date
    FROM {{ source('raw', 'cg1_mtl_txn_source_types') }}
),

transformed_exp_cg1_mtl_txn_source_types AS (
    SELECT
    transaction_source_type_name,
    transaction_source_type_id,
    disable_date,
    description,
    TO_INTEGER(IFF(ISNULL(TRANSACTION_SOURCE_TYPE_ID),-999,TRANSACTION_SOURCE_TYPE_ID)) AS o_transaction_source_type_id,
    IFF(ISNULL(DESCRIPTION),'UNKNOWN',DESCRIPTION) AS source_type_descr1,
    CURRENT_DATE() AS edw_update_dtm
    FROM source_cg1_mtl_txn_source_types
),

update_strategy_upd_n_material_trnsctn_source_type_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_cg1_mtl_txn_source_types
    WHERE DD_INSERT != 3
),

update_strategy_upd_n_material_trnsctn_source_type_update AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_n_material_trnsctn_source_type_insert
    WHERE DD_UPDATE != 3
),

routed_rtr_n_material_trnsctn_source_type AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            WHEN TRUE THEN 'INPUT'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM update_strategy_upd_n_material_trnsctn_source_type_update
),

lookup_lkp_n_material_trnsctn_source_type AS (
    SELECT
        a.*,
        b.*
    FROM routed_rtr_n_material_trnsctn_source_type a
    LEFT JOIN {{ source('raw', 'n_material_trnsctn_source_type') }} b
        ON a.o_transaction_source_type_id = b.o_transaction_source_type_id
),

final AS (
    SELECT
        bk_source_type_name,
        sk_trnsctn_source_type_id_int,
        source_type_disable_dt,
        source_type_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM lookup_lkp_n_material_trnsctn_source_type
)

SELECT * FROM final