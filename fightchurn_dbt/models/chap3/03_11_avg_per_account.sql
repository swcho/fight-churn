with
    date_range as (
        select
            '2020-01-01'::timestamp as start_date, '2020-12-31'::timestamp as end_date
    ),
    account_count as (
        select count(distinct account_id) as n_account
        from subscription s
        inner join
            date_range d
            on (d.start_date <= s.end_date or s.end_date is null)
            and s.start_date <= d.end_date
    )
select
    event_type_name,
    count(*) as n_event,
    c.n_account as n_account,
    extract(days from d.end_date - d.start_date)::float / 28 as n_months,
    (count(*)::float / c.n_account::float) / (
        extract(days from d.end_date - d.start_date)::float / 28
    ) as events_per_account_per_month
from event e
cross join account_count c
inner join event_type t on t.event_type_id = e.event_type_id
inner join date_range d on d.start_date <= e.event_time and e.event_time <= d.end_date
group by e.event_type_id, c.n_account, d.end_date, d.start_date, t.event_type_name
order by events_per_account_per_month desc
