with
    date_range as (
        select i::timestamp as calc_date
        from generate_series('2020-04-01', '2020-05-06', '7 day'::interval) i
    ),
    the_metric as (
        select *
        from metric m
        inner join metric_name n on m.metric_name_id = n.metric_name_id
        where n.metric_name = 'like_per_month'

    )
select
    d.calc_date,
    avg(m.metric_value),
    count(m.*) as n_calc,
    min(m.metric_value),
    max(m.metric_value)
from date_range d
left outer join the_metric m on d.calc_date = m.metric_time
group by d.calc_date
order by d.calc_date
