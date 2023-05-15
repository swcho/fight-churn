with
    date_vals as (
        select i::timestamp as metric_date, 1 as duration
        from generate_series('2020-01-08', '2020-12-31', '7 day'::interval) i
    )
select e.account_id, d.metric_date::date, sum(d.duration) as local_call_duration
from event e
inner join
    date_vals d
    on d.metric_date - interval '28 day' <= e.event_time
    and e.event_time < d.metric_date
inner join event_type t on t.event_type_id = e.event_type_id
where t.event_type_name = 'like'
group by e.account_id, d.metric_date
order by e.account_id, d.metric_date
