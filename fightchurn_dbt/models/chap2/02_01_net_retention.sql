with
    date_range as (
        -- churn 계산을 할 날짜 범위 지정
        select '2020-03-01'::date as start_date, '2020-04-01'::date as end_date
    ),
    start_accounts as (
        -- 시작 시점에 구동중인 계정과 그 mrr
        select account_id, sum(mrr) as total_mrr
        from {{source('churn', 'subscription')}} s
        inner join
            date_range d
            on s.start_date <= d.start_date
            and (d.start_date < s.end_date or s.end_date is null)
        group by account_id
    ),
    end_accounts as (
        -- 끝나는 시점에 구동중인 계정과 그 mrr
        select account_id, sum(mrr) as total_mrr
        from {{source('churn', 'subscription')}} s
        inner join
            date_range d
            on s.start_date <= d.end_date
            and (d.end_date < s.end_date or s.end_date is null)
        group by account_id
    ),
    retained_accounts as (
        -- 시작과 끝 시점의 사용자들을 join 하여 retention 목록 생성
        select s.account_id, sum(e.total_mrr) as total_mrr
        from start_accounts s
        inner join end_accounts e on s.account_id = e.account_id
        group by s.account_id
    ),
    start_mrr as (
        -- 시작 시점의 mrr 합산
        select sum(start_accounts.total_mrr) as start_mrr from start_accounts
    ),
    retain_mrr as (
        -- retention mrr 합산
        select sum(retained_accounts.total_mrr) as retain_mrr from retained_accounts
    )
select
    (retain_mrr / start_mrr) as net_mrr_retention_rate,
    (1.0 - retain_mrr / start_mrr) as net_mrr_churn_rate,
    start_mrr,
    retain_mrr
from start_mrr, retain_mrr
  retain_mrr
