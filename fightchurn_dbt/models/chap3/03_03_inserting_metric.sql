with
    date_vals as (
        select i::timestamp as metric_date
        from generate_series('2020-01-29', '2020-04-16', '7 day'::interval) i
    )
insert into metric (account_id, metric_time, metric_name_id, metric_value)
select e.account_id, d.metric_date, 0 as metric_name_id, count(*) as metric_value
from event e
inner join
    date_vals d
    on d.metric_date - interval '28 day' <= e.event_time
    and e.event_time < d.metric_date
inner join event_type t on t.event_type_id = e.event_type_id
where t.event_type_name = 'post'
group by e.account_id, d.metric_date
