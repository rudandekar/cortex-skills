{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_xxscm_dv_otm_delv_ib_ic', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_XXSCM_DV_OTM_DELV_IB_IC',
        'target_table': 'EL_CG1_XXSCM_DV_OTM_DELV_IB_IC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.528643+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_dv_otm_delv_ib_ic AS (
    SELECT
        plan_seq_id,
        delivery_id,
        release_weight,
        freight_charge,
        assignment_status,
        last_updated_by,
        last_update_date,
        created_by,
        creation_date,
        request_id,
        program_id,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        source_commit_time,
        global_name,
        create_datetime,
        source_dml_type,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_dv_otm_delv_ib_ic') }}
),

final AS (
    SELECT
        plan_seq_id,
        delivery_id,
        attribute7,
        source_commit_time,
        global_name
    FROM source_st_cg1_xxscm_dv_otm_delv_ib_ic
)

SELECT * FROM final