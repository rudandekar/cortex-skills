{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sc_pf_rprtng_structure_sop23nf', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_SC_PF_RPRTNG_STRUCTURE_SOP23NF',
        'target_table': 'N_SC_PF_RPRTNG_STRUCTURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.535523+00:00'
    }
) }}

WITH 

source_sc_pf_rprtng_structure AS (
    SELECT
        pf,
        new_mpm,
        dmo,
        vp,
        row_id,
        line_id,
        batch_id
    FROM {{ source('raw', 'sc_pf_rprtng_structure') }}
),

transformed_exp_sc_pf_rprtng_structure AS (
    SELECT
    bk_product_family_id,
    mpm_name,
    dmo_name,
    vp_name
    FROM source_sc_pf_rprtng_structure
),

update_strategy_ins_sc_pf_rprtng_structure AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_sc_pf_rprtng_structure
    WHERE DD_INSERT != 3
),

update_strategy_ins_sc_pf_rprtng_structure1 AS (
    SELECT
        *,
        CASE 
            WHEN DD_UPDATE = 0 THEN 'INSERT'
            WHEN DD_UPDATE = 1 THEN 'UPDATE'
            WHEN DD_UPDATE = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM update_strategy_ins_sc_pf_rprtng_structure
    WHERE DD_UPDATE != 3
),

transformed_exp_sc_pf_rprtng_structure1 AS (
    SELECT
    bk_product_family_id,
    mpm_name,
    dmo_name,
    vp_name
    FROM update_strategy_ins_sc_pf_rprtng_structure1
),

final AS (
    SELECT
        bk_product_family_id,
        mpm_name,
        dmo_name,
        vp_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM transformed_exp_sc_pf_rprtng_structure1
)

SELECT * FROM final