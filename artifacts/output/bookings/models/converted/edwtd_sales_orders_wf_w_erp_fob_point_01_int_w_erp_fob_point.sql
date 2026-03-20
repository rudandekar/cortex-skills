{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_w_erp_fob_point', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_ERP_FOB_POINT',
        'target_table': 'W_ERP_FOB_POINT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.727251+00:00'
    }
) }}

WITH 

source_el_fnd_lookup_values AS (
    SELECT
        lookup_type,
        lang,
        security_group_id,
        lookup_code,
        meaning,
        view_application_id,
        global_name,
        enabled_flag,
        description,
        create_datetime,
        ges_update_date,
        start_date_active,
        end_date_active
    FROM {{ source('raw', 'el_fnd_lookup_values') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

transformed_exp_el_fnd_lookup_values AS (
    SELECT
    lookup_code,
    ges_update_date,
    end_tv_date,
    meaning,
    description,
    enabled_flag,
    start_date_active,
    end_date_active,
    source_system_code,
    action_code,
    rank_index,
    dml_type,
    edw_update_datetime
    FROM source_n_source_system_codes
),

final AS (
    SELECT
        bk_fob_point_code,
        start_tv_date,
        end_tv_date,
        fob_point_name,
        fob_point_description,
        fob_point_enabled_flag,
        fob_point_start_active_dtm,
        fob_point_end_active_dtm,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        edw_observation_datetime,
        action_code,
        dml_type
    FROM transformed_exp_el_fnd_lookup_values
)

SELECT * FROM final