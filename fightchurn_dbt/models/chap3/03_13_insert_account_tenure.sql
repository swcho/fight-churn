with recursive
    date_vals as (
        select i::timestamp as metric_date
        from generate_series('2020-02-02', '2020-05-10', '7 day'::interval) i
    ),
    earlier_starts as (
        select s.account_id, d.metric_date, min(s.start_date) as start_date
        from subscription s
        inner join
            date_vals d
            on (d.metric_date < s.end_date or s.end_date is null)
            and s.start_date <= d.metric_date
        group by s.account_id, d.metric_date

        union

        select s.account_id, e.metric_date, s.start_date
        from subscription s
        inner join
            earlier_starts e
            on s.account_id = e.account_id
            and (e.start_date - 31) <= s.end_date
            and s.start_date < e.start_date
    )
insert into metric (account_id, metric_time, metric_name_id, metric_value)
select
    account_id,
    metric_date,
    8 as metric_name_id,
    extract(days from metric_date - min(start_date)) as metric_value
from earlier_starts
group by account_id, metric_date
order by
    account_id,
    metric_date

{# 162599 #}
{# 403115 #}
