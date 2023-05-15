with
    date_range as (
        select
            '2020-04-01'::timestamp as start_date, '2020-05-06'::timestamp as end_date
    ),
    account_count as (
        select count(distinct s.account_id) as n_account
        from subscription s
        inner join
            date_range d
            on s.start_date <= d.end_date
            and (d.start_date <= s.end_date or s.end_date is null)
    )
select
    n.metric_name,
    count(distinct m.account_id) as count_with_metric,
    c.n_account as n_account,
    (count(distinct m.account_id)::float / c.n_account::float) as pcnt_with_metric,
    avg(m.metric_value) as avg_value,
    min(m.metric_value) as min_value,
    max(m.metric_value) as max_value,
    min(m.metric_time) as earliest_metric,
    max(m.metric_time) as last_metric
from metric m
cross join account_count c
inner join date_range d on d.start_date <= m.metric_time and metric_time <= end_date
inner join metric_name n on m.metric_name_id = n.metric_name_id
inner join
    subscription s
    on s.account_id = m.account_id
    and s.start_date <= m.metric_time
    and (m.metric_time <= s.end_date or s.end_date is null)
group by n.metric_name, c.n_account
order by n.metric_name
