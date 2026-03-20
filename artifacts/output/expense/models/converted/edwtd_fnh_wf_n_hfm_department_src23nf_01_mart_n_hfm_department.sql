{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_hfm_department_src23nf', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_N_HFM_DEPARTMENT_SRC23NF',
        'target_table': 'N_HFM_DEPARTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.859775+00:00'
    }
) }}

WITH 

source_vw_n_financial_dept_pc_hier AS (
    SELECT
        parent_dept,
        child_dept,
        dept_number,
        dept_code,
        child_dept_desc,
        refresh_date
    FROM {{ source('raw', 'vw_n_financial_dept_pc_hier') }}
),

transformed_exptrans_n_hfm_department AS (
    SELECT
    lkp_bk_hfm_department_cd,
    bk_hfm_department_cd,
    hfm_department_descr,
    hfm_department_category_cd,
    SYSTIMESTAMP('SS') AS edw_update_dtm
    FROM source_vw_n_financial_dept_pc_hier
),

lookup_lkp_n_hfm_department AS (
    SELECT
        a.*,
        b.*
    FROM transformed_exptrans_n_hfm_department a
    LEFT JOIN {{ source('raw', 'n_hfm_department') }} b
        ON a.in_child_dept = b.in_child_dept
),

routed_rtr_n_hfm_department AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            WHEN TRUE THEN 'UPDATE'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM lookup_lkp_n_hfm_department
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
    FROM routed_rtr_n_hfm_department
    WHERE DD_UPDATE != 3
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
    FROM update_strategy_upd_update
    WHERE DD_INSERT != 3
),

final AS (
    SELECT
        bk_hfm_department_cd,
        hfm_department_descr,
        hfm_department_category_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM update_strategy_upd_insert
)

SELECT * FROM final