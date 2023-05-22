with
    date_vals as (
        select i::timestamp as metric_date
        from generate_series('2020-04-02', '2020-04-09', '7 day'::interval) i
    )
select s.account_id, d.metric_date, sum(s.mrr) as total_mrr
from subscription s
inner join
    date_vals d
    on (d.metric_date < s.end_date or s.end_date is null)
    and s.start_date <= d.metric_date
group by s.account_id, d.metric_date
