with
    date_range as (
        -- 분석 대상일, 시작, 끝
        select
            '2020-03-01'::timestamp as start_date,
            '2020-04-01'::timestamp as end_date,
            interval '1 months' as inactivity_interval
    ),
    start_accounts as (
        -- 시작일 1개월 전 부터 시작일까지 이벤트 발생한 계정 목록
        select distinct account_id
        from {{ source('churn', 'event') }} e
        inner join
            date_range d
            on d.start_date - d.inactivity_interval < e.event_time
            and e.event_time <= d.start_date
    ),
    start_count as (
        -- 시작일 기준 사용자 수
        select count(*) as n_start from start_accounts
    ),

    end_accounts as (
        -- 끝일 1개월 전 부터 끝일까지 이벤트 발생한 계정 목록
        select distinct account_id
        from {{ source('churn', 'event') }} e
        inner join
            date_range d
            on d.end_date - inactivity_interval < e.event_time
            and e.event_time <= d.end_date
    ),
    end_count as (
        -- 끝일 기준 사용자 수
        select count(*) as n_end from end_accounts
    ),
    churned_accounts as (
        -- 시작일 계정 목록에서 끝일 계정 목록을 left outer join 후,
        -- 끝일 계정이 없는 경우 churned 계정 목록
        select distinct s.account_id
        from start_accounts s
        left outer join end_accounts e on s.account_id = e.account_id
        where e.account_id is null
    ),
    churn_count as (
        -- churn 개수
        select count(*) as n_churn from churned_accounts
    )
select
    n_churn::float / n_start::float as churn_rate,
    1.0 - n_churn::float / n_start::float as retention_rate,
    n_start,
    n_churn
from start_count, end_count, churn_count churn_count
