{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_qt_wth_sbs_prc_dlvrbl_dtl_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QT_WTH_SBS_PRC_DLVRBL_DTL_SEP',
        'target_table': 'N_AS_QT_WTH_SBS_PRC_DLVRBL_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.976283+00:00'
    }
) }}

WITH 

source_n_as_qt_wth_sbs_prc_dlvrbl_dtl AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        total_trips_cnt,
        total_nonlabor_usd_cost,
        total_work_hrs_qty,
        total_nights_away_cnt,
        service_prd_net_price_usd_amt,
        service_prd_lst_price_usd_amt,
        service_prd_cost_usd_amt,
        service_prd_qty,
        ordered_qty,
        deliverable_type_cd,
        source_deleted_flg,
        total_travel_hrs_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM {{ source('raw', 'n_as_qt_wth_sbs_prc_dlvrbl_dtl') }}
),

final AS (
    SELECT
        bk_deliverable_name,
        bk_service_component_name,
        bk_service_program_name,
        bk_as_quote_num,
        total_trips_cnt,
        total_nonlabor_usd_cost,
        total_work_hrs_qty,
        total_nights_away_cnt,
        service_prd_net_price_usd_amt,
        service_prd_lst_price_usd_amt,
        service_prd_cost_usd_amt,
        service_prd_qty,
        ordered_qty,
        deliverable_type_cd,
        source_deleted_flg,
        total_travel_hrs_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM source_n_as_qt_wth_sbs_prc_dlvrbl_dtl
)

SELECT * FROM final