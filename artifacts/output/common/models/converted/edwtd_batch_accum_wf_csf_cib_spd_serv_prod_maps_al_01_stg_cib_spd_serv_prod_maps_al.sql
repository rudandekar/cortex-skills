{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_cib_spd_serv_prod_maps_al', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_CIB_SPD_SERV_PROD_MAPS_AL',
        'target_table': 'STG_CIB_SPD_SERV_PROD_MAPS_AL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.741412+00:00'
    }
) }}

WITH 

source_stg_cib_spd_serv_prod_maps_al AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        service_prod_map_id,
        generic_service_item_id,
        prod_based_service_item_id,
        product_item_id,
        generic_service_item,
        prod_based_service_item,
        product_item,
        status,
        start_date_active,
        end_date_active,
        product_type,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        comments,
        date_processed,
        hold,
        group_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        gg_dequeue_time,
        gg_enqueue_time
    FROM {{ source('raw', 'stg_cib_spd_serv_prod_maps_al') }}
),

source_csf_cib_spd_serv_prod_maps_al AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        service_prod_map_id,
        generic_service_item_id,
        prod_based_service_item_id,
        product_item_id,
        generic_service_item,
        prod_based_service_item,
        product_item,
        status,
        start_date_active,
        end_date_active,
        product_type,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        comments,
        date_processed,
        hold,
        group_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        gg_dequeue_time,
        gg_enqueue_time
    FROM {{ source('raw', 'csf_cib_spd_serv_prod_maps_al') }}
),

transformed_exptrans AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    service_prod_map_id,
    generic_service_item_id,
    prod_based_service_item_id,
    product_item_id,
    generic_service_item,
    prod_based_service_item,
    product_item,
    status,
    start_date_active,
    end_date_active,
    product_type,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    comments,
    date_processed,
    hold,
    group_id,
    attribute1,
    attribute2,
    attribute3,
    attribute4,
    attribute5,
    gg_dequeue_time,
    gg_enqueue_time
    FROM source_csf_cib_spd_serv_prod_maps_al
),

final AS (
    SELECT
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        service_prod_map_id,
        generic_service_item_id,
        prod_based_service_item_id,
        product_item_id,
        generic_service_item,
        prod_based_service_item,
        product_item,
        status,
        start_date_active,
        end_date_active,
        product_type,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        comments,
        date_processed,
        hold,
        group_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        gg_dequeue_time,
        gg_enqueue_time
    FROM transformed_exptrans
)

SELECT * FROM final