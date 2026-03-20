{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_et_expense_accounts_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_ET_EXPENSE_ACCOUNTS_SRC2EL',
        'target_table': 'EL_ET_EXPENSE_ACCOUNTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.957553+00:00'
    }
) }}

WITH 

source_et_expense_accounts AS (
    SELECT
        account_id,
        account_bucket_id,
        description,
        db_prefix,
        create_date,
        create_user,
        update_date,
        update_user,
        centralized_allocation_flag
    FROM {{ source('raw', 'et_expense_accounts') }}
),

update_strategy_upd_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM source_et_expense_accounts
    WHERE DD_UPDATE != 3
),

update_strategy_ins_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_upd_updtrans
    WHERE DD_INSERT != 3
),

transformed_exptrans AS (
    SELECT
    account_id,
    account_bucket_id,
    description,
    db_prefix,
    centralized_allocation_flag,
    TO_INTEGER(ACCOUNT_BUCKET_ID) AS out_account_bucket_id
    FROM update_strategy_ins_updtrans
),

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exptrans a
    LEFT JOIN {{ source('raw', 'el_et_expense_accounts') }} b
        ON a.in_account_id = b.in_account_id
),

transformed_exptrans1 AS (
    SELECT
    account_id,
    account_bucket_id,
    description,
    db_prefix,
    centralized_allocation_flag,
    lkp_account_id,
    lkp_account_bucket_id,
    lkp_description,
    lkp_db_prefix,
    lkp_centralized_allocation_flag
    FROM lookup_lkptrans
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
    FROM transformed_exptrans1
),

final AS (
    SELECT
        account_id,
        account_bucket_id,
        description,
        db_prefix,
        centralized_allocation_flag
    FROM routed_rtrtrans
)

SELECT * FROM final