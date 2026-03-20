{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_credit_asgnmnt_reason_src23nf', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_CREDIT_ASGNMNT_REASON_SRC23NF',
        'target_table': 'N_SALES_CREDIT_ASGNMNT_REASON_UPDATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.388651+00:00'
    }
) }}

WITH 

source_xxotm_lookups AS (
    SELECT
        lookup_type,
        lookup_code,
        meaning,
        description,
        enabled_flag,
        start_date_active,
        end_date_active,
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
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login
    FROM {{ source('raw', 'xxotm_lookups') }}
),

lookup_lkp_n_sales_credit_asgnmnt_reason AS (
    SELECT
        a.*,
        b.*
    FROM source_xxotm_lookups a
    LEFT JOIN {{ source('raw', 'n_sales_credit_asgnmnt_reason') }} b
        ON a.in_lookup_code = b.in_lookup_code
),

transformed_exp_n_sales_credit_asgnmnt_reason AS (
    SELECT
    lkp_bk_sls_crdt_asgnmnt_reason_cd,
    lookup_code,
    description,
    CURRENT_DATE() AS edw_create_dtm,
    'E_SLSORD_BATCH' AS edw_create_user,
    CURRENT_DATE() AS edw_update_dtm,
    'E_SLSORD_BATCH' AS edw_update_user
    FROM lookup_lkp_n_sales_credit_asgnmnt_reason
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exp_n_sales_credit_asgnmnt_reason
),

update_strategy_upd_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtrtrans
    WHERE DD_INSERT != 3
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
    FROM update_strategy_upd_insert
    WHERE DD_UPDATE != 3
),

final AS (
    SELECT
        bk_sls_crdt_asgnmnt_reason_cd,
        sls_crdt_asgnmnt_reason_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_update
)

SELECT * FROM final