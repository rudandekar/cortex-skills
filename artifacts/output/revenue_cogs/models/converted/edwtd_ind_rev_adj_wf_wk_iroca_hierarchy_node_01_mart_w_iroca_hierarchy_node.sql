{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_iroca_hierarchy_node', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_WK_IROCA_HIERARCHY_NODE',
        'target_table': 'W_IROCA_HIERARCHY_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.286084+00:00'
    }
) }}

WITH 

source_st_ae_sub_measure_pub AS (
    SELECT
        batch_id,
        sub_measure_key,
        sub_measure_id,
        sub_measure_name,
        sub_measure_description,
        measure_id,
        source_system_id,
        old_source_system_name,
        start_effective_date,
        end_effective_date,
        start_fiscal_period_id,
        end_fiscal_period_id,
        dollar_upload_flag,
        percentage_upload_flag,
        discount_flag,
        approve_flag,
        dept_acct_flag,
        status_flag,
        source_system_adj_type_id,
        sox_tie_in,
        freq_data_posted,
        data_route,
        gmc_rollup,
        report_level_1,
        report_level_2,
        report_level_3,
        create_user,
        update_user,
        update_datetime,
        create_datetime,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ae_sub_measure_pub') }}
),

transformed_exp_st_ae_sub_measure_pub_root AS (
    SELECT
    batch_id,
    bk_iroca_hierarchy_node_id,
    start_tv_dt,
    node_type,
    end_tv_dt,
    ru_parent_hierarchy_node_id,
    create_datetime,
    action_code,
    rank_index,
    dml_type
    FROM source_st_ae_sub_measure_pub
),

transformed_exp_st_ae_sub_measure_middle AS (
    SELECT
    batch_id,
    bk_iroca_hierarchy_node_id,
    node_type,
    ru_parent_hierarchy_node_id,
    start_tv_dt,
    end_tv_dt,
    create_datetime,
    action_code,
    rank_index,
    dml_type
    FROM transformed_exp_st_ae_sub_measure_pub_root
),

transformed_exp_st_ae_sub_measure_pub_leaf AS (
    SELECT
    batch_id,
    bk_iroca_hierarchy_node_id,
    node_type,
    ru_parent_hierarchy_node_id,
    start_tv_dt,
    end_tv_dt,
    create_datetime,
    action_code,
    rank_index,
    dml_type
    FROM transformed_exp_st_ae_sub_measure_middle
),

final AS (
    SELECT
        bk_iroca_hierarchy_node_id,
        start_tv_dt,
        node_type,
        end_tv_dt,
        ru_parent_hierarchy_node_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_st_ae_sub_measure_pub_leaf
)

SELECT * FROM final