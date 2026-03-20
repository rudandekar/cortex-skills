{{ config(
    materialized='table',
    schema='tgt_schema',
    tags=['wf_m_td_to_td_complex', 'TODO_freq', 'TODO_domain'],
    meta={
        'source_workflow': 'wf_m_TD_to_TD_complex',
        'target_table': 'TD_TGT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-15T13:12:35.099586+00:00'
    }
) }}

WITH 

source_td_src AS (
    SELECT
        key_id,
        col_a,
        col_b,
        amt,
        upd_ts
    FROM {{ source('raw', 'td_src') }}
),

transformed_exp_standardize AS (
    SELECT
    key_id,
    col_a,
    col_b,
    amt,
    upd_ts
    FROM source_td_src
),

lookup_lkp_dim AS (
    SELECT
        a.*,
        b.dim_desc
    FROM transformed_exp_standardize a
    LEFT JOIN {{ source('raw', 'lookup_table') }} b
        ON COL_B_STD = IN_COL_B_STD
),

aggregated_agg_bykey AS (
    SELECT
    key_id,
    amt
    FROM lookup_lkp_dim
    GROUP BY key_id, amt
),

routed_rtr_validreject AS (
    SELECT *
    FROM aggregated_agg_bykey
    WHERE TRUE
),

final AS (
    SELECT
        key_id,
        col_a,
        col_b_std,
        amt,
        dim_desc,
        load_dt,
        upd_ts,
        ins_ts
    FROM routed_rtr_validreject
)

SELECT * FROM final