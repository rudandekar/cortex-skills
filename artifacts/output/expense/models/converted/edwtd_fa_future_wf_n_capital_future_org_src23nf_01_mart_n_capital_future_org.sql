{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_capital_future_org_src23nf', 'batch', 'edwtd_fa_future'],
    meta={
        'source_workflow': 'wf_m_N_CAPITAL_FUTURE_ORG_SRC23NF',
        'target_table': 'N_CAPITAL_FUTURE_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.151638+00:00'
    }
) }}

WITH 

source_cap_organizations AS (
    SELECT
        update_date,
        create_date,
        created_by,
        org_name,
        org_id,
        updated_by,
        block_users,
        block_org,
        org_months
    FROM {{ source('raw', 'cap_organizations') }}
),

lookup_lkp_n_captial_future_org AS (
    SELECT
        a.*,
        b.*
    FROM source_cap_organizations a
    LEFT JOIN {{ source('raw', 'n_capital_future_org') }} b
        ON a.org_name = b.org_name
),

transformed_exp_n_captial_future AS (
    SELECT
    bk_cptl_future_org_abbr_name,
    org_name,
    org_id,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_n_captial_future_org
),

filtered_ftr_n_capital_future_org AS (
    SELECT *
    FROM transformed_exp_n_captial_future
    WHERE ISNULL(BK_CPTL_FUTURE_ORG_ABBR_NAME)
),

update_strategy_upd_n_capital_future_org_insert AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM filtered_ftr_n_capital_future_org
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_cptl_future_org_abbr_name,
        sk_org_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_n_capital_future_org_insert
)

SELECT * FROM final