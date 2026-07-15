-- =============================================
-- Author: Ascendion AAVA
-- Created on:
-- Description: Employee data load mapping with transformations
-- =============================================

-- =========================================================
-- CONVERSION LOG
-- =========================================================
-- Input Type : Informatica Mapping
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
    materialized='table',
    tags=['employee', 'load']
) }}

-- Reason: Source qualifier logic converted to CTE with dbt source reference
with source_data as (
    select
        EMPNO,
        ENAME,
        JOB,
        MGR,
        HIREDATE,
        SAL,
        COMM,
        DEPTNO
    from {{ source('hr_system', 'EMP') }}
),

-- Reason: Expression transformation converted to SQL CASE statements and calculations
transformed_data as (
    select
        EMPNO,
        ENAME,
        JOB,
        MGR,
        HIREDATE,
        SAL,
        COMM,
        DEPTNO,
        -- Reason: IIF function converted to CASE WHEN for dbt compatibility
        case
            when COMM is null then 0
            else COMM
        end as COMM_ADJUSTED,
        -- Reason: Simple arithmetic expression preserved
        SAL + coalesce(COMM, 0) as TOTAL_COMP,
        -- Reason: SYSDATE converted to CURRENT_TIMESTAMP for dbt warehouse compatibility
        current_timestamp as LOAD_DATE
    from source_data
),

-- Reason: Filter transformation converted to WHERE clause in CTE
filtered_data as (
    select
        EMPNO,
        ENAME,
        JOB,
        MGR,
        HIREDATE,
        SAL,
        COMM,
        DEPTNO,
        COMM_ADJUSTED,
        TOTAL_COMP,
        LOAD_DATE
    from transformed_data
    where DEPTNO is not null
),

-- Reason: Lookup transformation converted to LEFT JOIN with subquery
lookup_dept as (
    select
        DEPTNO,
        DNAME,
        LOC
    from {{ source('hr_system', 'DEPT') }}
),

final_data as (
    select
        f.EMPNO,
        f.ENAME,
        f.JOB,
        f.MGR,
        f.HIREDATE,
        f.SAL,
        f.COMM,
        f.DEPTNO,
        f.COMM_ADJUSTED,
        f.TOTAL_COMP,
        f.LOAD_DATE,
        -- Reason: Lookup ports added via LEFT JOIN to preserve non-matching records
        coalesce(d.DNAME, 'UNKNOWN') as DEPT_NAME,
        coalesce(d.LOC, 'UNKNOWN') as DEPT_LOCATION
    from filtered_data f
    left join lookup_dept d
        on f.DEPTNO = d.DEPTNO
)

-- Reason: Final SELECT replaces Informatica target; dbt materialization handles INSERT
select
    EMPNO,
    ENAME,
    JOB,
    MGR,
    HIREDATE,
    SAL,
    COMM,
    DEPTNO,
    COMM_ADJUSTED,
    TOTAL_COMP,
    LOAD_DATE,
    DEPT_NAME,
    DEPT_LOCATION
from final_data
