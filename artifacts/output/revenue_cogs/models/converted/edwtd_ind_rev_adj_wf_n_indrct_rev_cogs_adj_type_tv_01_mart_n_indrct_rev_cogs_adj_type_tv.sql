{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_indirect_rev_cogs_adj_type_tv', 'batch', 'edwtd_ind_rev_adj'],
    meta={
        'source_workflow': 'wf_m_N_INDIRECT_REV_COGS_ADJ_TYPE_TV',
        'target_table': 'N_INDRCT_REV_COGS_ADJ_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.518746+00:00'
    }
) }}

WITH 

source_w_indrct_rev_cogs_adj_type AS (
    SELECT
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        start_tv_dt,
        end_tv_dt,
        adj_type_effective_start_dtm,
        adj_type_effective_end_dtm,
        adjustment_type_descr,
        bk_iroca_hierarchy_node_id,
        adjustment_source_cd,
        discount_flg,
        status_flg,
        sox_tie_in_descr,
        adj_data_update_frequency_cd,
        data_route_descr,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        create_dtm,
        update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_adjustment_type_id_int,
        retained_earnings_flg,
        nrs_transition_flg,
        corporate_revenue_flg,
        dual_gaap_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_indrct_rev_cogs_adj_type') }}
),

final AS (
    SELECT
        bk_adjustment_type_cd,
        bk_revenue_or_cogs_type_cd,
        start_tv_dt,
        end_tv_dt,
        adj_type_effective_start_dtm,
        adj_type_effective_end_dtm,
        adjustment_type_descr,
        bk_iroca_hierarchy_node_id,
        adjustment_source_cd,
        discount_flg,
        status_flg,
        sox_tie_in_descr,
        adj_data_update_frequency_cd,
        data_route_descr,
        crtd_by_cisco_worker_party_key,
        upd_by_cisco_worker_party_key,
        create_dtm,
        update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sk_adjustment_type_id_int,
        retained_earnings_flg,
        nrs_transition_flg,
        corporate_revenue_flg,
        dual_gaap_flg
    FROM source_w_indrct_rev_cogs_adj_type
)

SELECT * FROM final