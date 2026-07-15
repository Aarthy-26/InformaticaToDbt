=============================================
-- Author: Ascendion AAVA
-- Created on:
-- Description: Informatica to dbt Conversion - Employee Staging Model
=============================================

-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping (m_stg_employee)
-- Target Platform : dbt (SQL Model)
-- Conversion Approach :
-- - Translated Informatica mapping logic (transformations, expressions, lookups) into dbt SQL model constructs
-- - Replaced Informatica built-in functions (IIF, DECODE, NVL, TRUNC, INSTR, SUBSTR, SYSDATE, etc.) with dbt/Jinja + warehouse SQL equivalents
-- - Converted Informatica sources and targets into dbt {{ source() }} / {{ ref() }} references
-- - Replaced Update Strategy transformations with dbt incremental model logic (is_incremental(), unique_key)
-- - Converted Rank / Sequence transformations into window functions or surrogate key macros (e.g. dbt_utils.generate_surrogate_key)
-- - Converted Slowly Changing Dimension logic into dbt snapshot constructs where applicable
-- - Added inline migration comments where source logic required dbt-specific restructuring
-- - Ensured compatibility with dbt Core / dbt Cloud on the target warehouse
-- Major Risks / Checks :
-- - Validate datatype mappings between Informatica source/target ports and warehouse column types
-- - Validate NULL semantics and string concatenation behavior
-- - Validate incremental/merge logic against the original Update Strategy behavior
-- - Validate that the ref()/source() dependency graph resolves correctly (no missing or broken sources)
-- - Validate materialization choice (view / table / incremental) matches the original load pattern
-- - Validate companion schema.yml tests reflect the original Informatica data quality checks
-- =========================================================

{{ config(
    materialized='incremental',
    unique_key='EMPLOYEE_ID',
    tags=['staging', 'employee']
) }}

-- Reason: Source qualifier logic converted to CTE with {{ source() }} reference instead of hardcoded table name
with source_data as (
    select
        EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE_NUMBER,
        HIRE_DATE,
        JOB_ID,
        SALARY,
        COMMISSION_PCT,
        MANAGER_ID,
        DEPARTMENT_ID
    from {{ source('hr_source', 'EMPLOYEES') }}
    {% if is_incremental() %}
    -- Reason: Incremental filter replaces Informatica session-level filter for delta loads
    where HIRE_DATE > (select max(HIRE_DATE) from {{ this }})
    {% endif %}
),

-- Reason: Expression transformation logic converted to SQL CASE statements and functions
transformed_data as (
    select
        EMPLOYEE_ID,
        -- Reason: NVL converted to COALESCE for NULL handling
        coalesce(FIRST_NAME, 'UNKNOWN') as FIRST_NAME,
        coalesce(LAST_NAME, 'UNKNOWN') as LAST_NAME,
        -- Reason: String concatenation using SQL || operator
        coalesce(FIRST_NAME, '') || ' ' || coalesce(LAST_NAME, '') as FULL_NAME,
        upper(EMAIL) as EMAIL,
        PHONE_NUMBER,
        HIRE_DATE,
        JOB_ID,
        -- Reason: IIF converted to CASE statement for conditional logic
        case
            when SALARY is null then 0
            else SALARY
        end as SALARY,
        -- Reason: NVL converted to COALESCE with default value
        coalesce(COMMISSION_PCT, 0) as COMMISSION_PCT,
        SALARY * coalesce(COMMISSION_PCT, 0) as COMMISSION_AMOUNT,
        MANAGER_ID,
        DEPARTMENT_ID,
        -- Reason: SYSDATE converted to CURRENT_TIMESTAMP for warehouse compatibility
        current_timestamp as LOAD_DATE,
        -- Reason: Date arithmetic converted to DATEDIFF for tenure calculation
        datediff(day, HIRE_DATE, current_date) as TENURE_DAYS,
        -- Reason: DECODE converted to CASE statement for multi-condition logic
        case
            when datediff(year, HIRE_DATE, current_date) < 1 then 'NEW'
            when datediff(year, HIRE_DATE, current_date) between 1 and 5 then 'INTERMEDIATE'
            when datediff(year, HIRE_DATE, current_date) > 5 then 'SENIOR'
            else 'UNKNOWN'
        end as EMPLOYEE_CATEGORY,
        -- Reason: Salary band logic using CASE for range-based categorization
        case
            when SALARY < 5000 then 'LOW'
            when SALARY between 5000 and 10000 then 'MEDIUM'
            when SALARY > 10000 then 'HIGH'
            else 'UNCLASSIFIED'
        end as SALARY_BAND,
        -- Reason: Email validation using CHARINDEX (INSTR equivalent)
        case
            when charindex('@', EMAIL) > 0 then 'VALID'
            else 'INVALID'
        end as EMAIL_STATUS,
        -- Reason: SUBSTR converted to SUBSTRING for string extraction
        substring(JOB_ID, 1, 3) as JOB_PREFIX,
        -- Reason: TRUNC converted to CAST for numeric truncation
        cast(SALARY as integer) as SALARY_ROUNDED,
        'ACTIVE' as RECORD_STATUS,
        current_timestamp as CREATED_DATE,
        current_timestamp as UPDATED_DATE
    from source_data
),

-- Reason: Lookup transformation converted to LEFT JOIN with subquery
department_lookup as (
    select
        DEPARTMENT_ID,
        DEPARTMENT_NAME,
        LOCATION_ID
    from {{ source('hr_source', 'DEPARTMENTS') }}
),

-- Reason: Join logic to enrich employee data with department information
final_data as (
    select
        t.EMPLOYEE_ID,
        t.FIRST_NAME,
        t.LAST_NAME,
        t.FULL_NAME,
        t.EMAIL,
        t.PHONE_NUMBER,
        t.HIRE_DATE,
        t.JOB_ID,
        t.SALARY,
        t.COMMISSION_PCT,
        t.COMMISSION_AMOUNT,
        t.MANAGER_ID,
        t.DEPARTMENT_ID,
        -- Reason: Lookup result with NULL handling via COALESCE
        coalesce(d.DEPARTMENT_NAME, 'UNASSIGNED') as DEPARTMENT_NAME,
        d.LOCATION_ID,
        t.LOAD_DATE,
        t.TENURE_DAYS,
        t.EMPLOYEE_CATEGORY,
        t.SALARY_BAND,
        t.EMAIL_STATUS,
        t.JOB_PREFIX,
        t.SALARY_ROUNDED,
        t.RECORD_STATUS,
        t.CREATED_DATE,
        t.UPDATED_DATE
    from transformed_data t
    left join department_lookup d
        on t.DEPARTMENT_ID = d.DEPARTMENT_ID
)

-- Reason: Final SELECT replaces Informatica target load; dbt materialization handles INSERT/UPDATE
select * from final_data
