with
    date_range as (
        -- 분석 범위: 시작날짜, 끝날짜
        select '2020-03-01'::date as start_date, '2020-04-01'::date as end_date
    ),
    start_accounts as (
        -- 시작날짜 기준 가입자 목록과 mrr 합계
        select account_id, sum(mrr) as total_mrr
        from {{source('churn', 'subscription')}} s
        inner join
            date_range d
            on s.start_date <= d.start_date
            and (d.start_date < s.end_date or s.end_date is null)
        group by account_id
    ),
    end_accounts as (
        -- 끝날짜 기준 가입자 목록과 mrr 합계
        select account_id, sum(mrr) as total_mrr
        from {{source('churn', 'subscription')}} s
        inner join
            date_range d
            on s.start_date <= d.end_date
            and (d.end_date < s.end_date or s.end_date is null)
        group by account_id
    ),
    churned_accounts as (
        -- 이탈 가입자 목록: 시작날짜에만 존재하는 가입자 목록과 mrr 합계
        select s.account_id, sum(s.total_mrr) as total_mrr
        from start_accounts s
        left outer join end_accounts e on s.account_id = e.account_id
        where e.account_id is null
        group by s.account_id
    ),
    downsell_accounts as (
        -- 다운셀, 다운판매(downsell) 목록: 끝날짜의 mrr이 시작날짜의 mrr보다 줄은 계정과 그 차이
        select s.account_id, s.total_mrr - e.total_mrr as downsell_amount
        from start_accounts s
        inner join end_accounts e on s.account_id = e.account_id
        where e.total_mrr < s.total_mrr
    ),
    start_mrr as (
        -- 시작날짜의 mrr 합계
        select sum(total_mrr) as start_mrr from start_accounts
    ),
    churn_mrr as (
        -- 이탈가입자 mrr 합계
        select sum(total_mrr) as churn_mrr from churned_accounts
    ),
    downsell_mrr as (
        -- 다운판매 mrr 합계
        select coalesce(sum(downsell_amount), 0.0) as downsell_mrr
        from downsell_accounts
    )
select
    (churn_mrr::float + downsell_mrr::float) / start_mrr::float as mrr_churn_rate,
    start_mrr,
    churn_mrr,
    downsell_mrr
from start_mrr, churn_mrr, downsell_mrr
  downsell_mrr
