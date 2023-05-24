with
    periods as (
        select
            i::timestamp as period_start, i::timestamp + '7 day'::interval as period_end
        from generate_series('2020-02-09', '2020-05-10', '7 day'::interval) i
    )
insert into active_week
    (account_id, start_date, end_date)
select e.account_id, p.period_start, p.period_end
from event e
inner join periods p on p.period_start <= e.event_time and e.event_time < p.period_end
group by account_id, period_start, period_end
