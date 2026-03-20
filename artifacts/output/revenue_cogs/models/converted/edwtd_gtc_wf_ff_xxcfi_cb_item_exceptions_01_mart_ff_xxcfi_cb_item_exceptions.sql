{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxcfi_cb_item_exceptions', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_FF_XXCFI_CB_ITEM_EXCEPTIONS',
        'target_table': 'FF_XXCFI_CB_ITEM_EXCEPTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.026861+00:00'
    }
) }}

WITH 

source_xxcfi_cb_item_exceptions AS (
    SELECT
        notify_seq,
        ordno,
        order_number,
        order_group,
        item_name,
        rec_type,
        slc_ctry,
        ship_to_ctry,
        slc_ovr_ctry,
        ship_to_ovr_ctry,
        shipset,
        missing_part_flg,
        missing_slc_hts_flg,
        missing_ship_to_hts_flg,
        creation_date,
        imported_flag,
        classified_flag,
        ignore_flag,
        work_bench_id,
        ship_to_ovr_cty_grp,
        slc_ovr_cty_grp,
        created_by,
        fcd_date,
        occ_ctry,
        occ_ovr_cty_grp,
        missing_occ_hts_flg,
        superbom,
        workbench_flag,
        reason,
        removed_date
    FROM {{ source('raw', 'xxcfi_cb_item_exceptions') }}
),

transformed_exptrans AS (
    SELECT
    notify_seq,
    ordno,
    order_number,
    order_group,
    item_name,
    rec_type,
    slc_ctry,
    ship_to_ctry,
    slc_ovr_ctry,
    ship_to_ovr_ctry,
    shipset,
    missing_part_flg,
    missing_slc_hts_flg,
    missing_ship_to_hts_flg,
    creation_date,
    imported_flag,
    classified_flag,
    ignore_flag,
    work_bench_id,
    ship_to_ovr_cty_grp,
    slc_ovr_cty_grp,
    created_by,
    fcd_date,
    occ_ctry,
    occ_ovr_cty_grp,
    missing_occ_hts_flg,
    superbom,
    workbench_flag,
    reason,
    removed_date,
    last_updated_date,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_xxcfi_cb_item_exceptions
),

final AS (
    SELECT
        notify_seq,
        ordno,
        order_number,
        order_group,
        item_name,
        rec_type,
        slc_ctry,
        ship_to_ctry,
        slc_ovr_ctry,
        ship_to_ovr_ctry,
        shipset,
        missing_part_flg,
        missing_slc_hts_flg,
        missing_ship_to_hts_flg,
        creation_date,
        imported_flag,
        classified_flag,
        ignore_flag,
        work_bench_id,
        ship_to_ovr_cty_grp,
        slc_ovr_cty_grp,
        created_by,
        fcd_date,
        occ_ctry,
        occ_ovr_cty_grp,
        missing_occ_hts_flg,
        superbom,
        workbench_flag,
        reason,
        removed_date,
        last_updated_date,
        create_datetime,
        action_code
    FROM transformed_exptrans
)

SELECT * FROM final