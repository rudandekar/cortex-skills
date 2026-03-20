{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dmp_co_return_line', 'batch', 'edwtd_cs'],
    meta={
        'source_workflow': 'wf_m_ST_DMP_CO_RETURN_LINE',
        'target_table': 'ST_DMP_CO_RETURN_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.212236+00:00'
    }
) }}

WITH 

source_st_dmp_co_return_line AS (
    SELECT
        batch_id,
        return_line_id,
        return_id,
        parent_return_line_id,
        part_number,
        description,
        line_number,
        unit_net_price,
        quantity,
        ext_net_price,
        ship_to_site_use_id,
        orig_erp_line_id,
        requested_ship_date,
        actual_ship_date,
        created_on,
        created_by,
        updated_on,
        updated_by,
        status,
        active,
        has_children,
        grand_parent_return_line_id,
        territory_id,
        share_node_id,
        item_type,
        unit_list_price,
        create_datetime,
        source_commit_time,
        refresh_datetime,
        action_code
    FROM {{ source('raw', 'st_dmp_co_return_line') }}
),

final AS (
    SELECT
        batch_id,
        return_line_id,
        return_id,
        parent_return_line_id,
        part_number,
        description,
        line_number,
        unit_net_price,
        quantity,
        ext_net_price,
        ship_to_site_use_id,
        orig_erp_line_id,
        requested_ship_date,
        actual_ship_date,
        created_on,
        created_by,
        updated_on,
        updated_by,
        status,
        active,
        has_children,
        grand_parent_return_line_id,
        territory_id,
        share_node_id,
        item_type,
        unit_list_price,
        create_datetime,
        source_commit_time,
        refresh_datetime,
        action_code
    FROM source_st_dmp_co_return_line
)

SELECT * FROM final