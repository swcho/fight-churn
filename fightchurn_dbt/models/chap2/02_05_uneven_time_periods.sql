with
    date_range as (
        -- 분석할 시작일 끝일
        select '2020-03-01'::date as start_date, '2020-04-01'::date as end_date
    ),
    start_accounts as (
        -- 시작일 구독자
        select distinct account_id
        from {{ source('churn', 'subscription') }} s
        inner join
            date_range d
            on s.start_date <= d.start_date
            and (d.start_date < s.end_date or s.end_date is null)
    ),
    end_accounts as (
        -- 끝일 구독자
        select distinct account_id
        from {{ source('churn', 'subscription') }} s
        inner join
            date_range d
            on s.start_date <= d.end_date
            and (d.end_date < s.end_date or s.end_date is null)
    ),
    churned_accounts as (
        -- 구독자를 left outer join 해서 시작일 구독자 목록 기준으로 목록 생성
        -- 끝일 구독자가 없는 것, 즉 구독 해지한 사용자 필터링하여 churned 목록 생성
        select s.account_id
        from start_accounts s
        left outer join end_accounts e on s.account_id = e.account_id
        where e.account_id is null
    ),
    start_count as (
        -- 시작일 사용자 수
        select count(*) as n_start from start_accounts
    ),
    churn_count as (
        -- churned 사용자 수
        select count(*) as n_churn from churned_accounts
    ),
    magic_numbers as (
        select
            end_date - start_date as duration,
            -- churn 사용자 수 / 시작일 사용자 수 = churn rate --
            n_churn::float / n_start::float as churn_rate
        from start_count, churn_count, date_range
    )
select
    n_start,
    n_churn,
    churn_rate as measured_churn,
    -- 시작날짜와 끝날짜의 차이, 즉 분석 대상 기간
    duration as period_days,
    -- eq 2.18
    1.0 - power(1.0 - churn_rate, 365.0 / duration::float) as annual_churn,
    -- eq 2.19
    1.0 - power(1.0 - churn_rate, (365.0 / 12.0) / duration::float) as monthly_churn
from start_count, churn_count, date_range, magic_numbers
