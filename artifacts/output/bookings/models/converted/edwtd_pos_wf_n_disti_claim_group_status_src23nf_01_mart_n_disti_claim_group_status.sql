{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_disti_claim_group_status_src23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_DISTI_CLAIM_GROUP_STATUS_SRC23NF',
        'target_table': 'N_DISTI_CLAIM_GROUP_STATUS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.310370+00:00'
    }
) }}

WITH 

source_dca_lookups AS (
    SELECT
        lookup_code,
        lookup_type,
        lookup_desc,
        ext_ref_code,
        ext_ref_id,
        comments,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5
    FROM {{ source('raw', 'dca_lookups') }}
),

routed_rtr_n_disti_claim_group_status_src23nf AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM source_dca_lookups
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
    FROM routed_rtr_n_disti_claim_group_status_src23nf
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

lookup_lkp_n_disti_claim_group_status_src23nf AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_upd_update a
    LEFT JOIN {{ source('raw', 'n_disti_claim_group_status') }} b
        ON a.in_lookup_code = b.in_lookup_code
),

transformed_exp_n_disti_claim_group_status_src23nf AS (
    SELECT
    bk_disti_claim_group_status_cd,
    lookup_code,
    lookup_desc,
    CURRENT_TIMESTAMP() AS edw_create_dtm,
    CURRENT_TIMESTAMP() AS edw_update_dtm
    FROM lookup_lkp_n_disti_claim_group_status_src23nf
),

final AS (
    SELECT
        bk_disti_claim_group_status_cd,
        cd_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exp_n_disti_claim_group_status_src23nf
)

SELECT * FROM final