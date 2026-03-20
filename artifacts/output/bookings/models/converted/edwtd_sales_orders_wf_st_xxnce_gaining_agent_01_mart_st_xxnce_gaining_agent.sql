{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxnce_gaining_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXNCE_GAINING_AGENT',
        'target_table': 'ST_XXNCE_GAINING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.581979+00:00'
    }
) }}

WITH 

source_xxnce_gaining_agent AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        gn_agent_id,
        gaining_node_id,
        gaining_salesrep_number,
        split_percent,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        claim_id,
        node_id_level_1,
        node_id_level_2,
        node_id_level_3,
        node_id_level_4,
        node_id_level_5,
        node_id_level_6,
        node_id_level_7,
        node_id_level_8,
        node_id_level_9,
        node_id_level_10,
        node_id_level_11
    FROM {{ source('raw', 'xxnce_gaining_agent') }}
),

transformed_exp_ff_xxnce_gaining_agent AS (
    SELECT
    gn_agent_id,
    gaining_node_id,
    gaining_salesrep_number,
    split_percent,
    object_version_number,
    created_by,
    creation_date,
    last_updated_by,
    last_updated_date,
    claim_id,
    node_id_level_1,
    node_id_level_2,
    node_id_level_3,
    node_id_level_4,
    node_id_level_5,
    node_id_level_6,
    node_id_level_7,
    node_id_level_8,
    node_id_level_9,
    node_id_level_10,
    node_id_level_11,
    TO_CHAR(GAINING_SALESREP_NUMBER) AS o_gaining_salesrep_number
    FROM source_xxnce_gaining_agent
),

final AS (
    SELECT
        gn_agent_id,
        gaining_node_id,
        gaining_salesrep_number,
        split_percent,
        object_version_number,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        claim_id,
        node_id_level_1,
        node_id_level_2,
        node_id_level_3,
        node_id_level_4,
        node_id_level_5,
        node_id_level_6,
        node_id_level_7,
        node_id_level_8,
        node_id_level_9,
        node_id_level_10,
        node_id_level_11
    FROM transformed_exp_ff_xxnce_gaining_agent
)

SELECT * FROM final