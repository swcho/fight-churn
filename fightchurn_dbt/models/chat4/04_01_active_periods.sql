with recursive
    active_period_params as (
        select interval '7 days' as allowed_gap, '2020-05-10'::date as calc_date
    ),
    active as (
        select distinct s.account_id, min(s.start_date) as start_date
        from subscription s
        inner join
            active_period_params a
            on (a.calc_date < s.end_date or end_date is null)
            and s.start_date <= a.calc_date
        group by s.account_id
        union
        select s.account_id, s.start_date
        from subscription s
        cross join active_period_params a
        inner join
            active e
            on s.account_id = e.account_id
            and (e.start_date - a.allowed_gap)::date <= s.end_date
            and s.start_date < e.start_date
    )
select account_id, min(start_date) as start_date, null::date as churn_date
from active
group by account_id, churn_date  {# 11911 #}
