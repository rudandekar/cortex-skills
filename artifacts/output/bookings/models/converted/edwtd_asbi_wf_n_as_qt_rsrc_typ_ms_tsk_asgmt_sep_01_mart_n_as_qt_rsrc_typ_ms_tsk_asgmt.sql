{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_as_qt_rsrc_typ_ms_tsk_asgmt_sep', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_N_AS_QT_RSRC_TYP_MS_TSK_ASGMT_SEP',
        'target_table': 'N_AS_QT_RSRC_TYP_MS_TSK_ASGMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.867393+00:00'
    }
) }}

WITH 

source_n_as_qt_rsrc_typ_ms_tsk_asgmt AS (
    SELECT
        resource_assignment_key,
        bk_resource_type,
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        sk_task_resource_id_int,
        lump_sum_usd_amt,
        source_deleted_flg,
        ru_num_of_nights_cnt,
        ru_num_of_trips_cnt,
        ru_travel_hours_cnt,
        ru_non_labor_usd_cost,
        ru_work_hours_cnt,
        ru_job_role_cd,
        ru_delivery_group_name,
        ru_partner_name,
        ru_third_party_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        resource_usd_cost,
        travel_and_expense_usd_cost,
        bk_ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM {{ source('raw', 'n_as_qt_rsrc_typ_ms_tsk_asgmt') }}
),

final AS (
    SELECT
        resource_assignment_key,
        bk_resource_type,
        bk_as_quote_num,
        bk_as_quote_milestone_num_int,
        bk_as_quote_milestone_task_num,
        sk_task_resource_id_int,
        lump_sum_usd_amt,
        source_deleted_flg,
        ru_num_of_nights_cnt,
        ru_num_of_trips_cnt,
        ru_travel_hours_cnt,
        ru_non_labor_usd_cost,
        ru_work_hours_cnt,
        ru_job_role_cd,
        ru_delivery_group_name,
        ru_partner_name,
        ru_third_party_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        resource_usd_cost,
        travel_and_expense_usd_cost,
        bk_ss_cd,
        onsite_hrs_cnt,
        remote_hrs_cnt
    FROM source_n_as_qt_rsrc_typ_ms_tsk_asgmt
)

SELECT * FROM final