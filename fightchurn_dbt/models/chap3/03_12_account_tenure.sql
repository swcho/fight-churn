with recursive
    date_range as (select '2020-07-01'::date as calc_date),
    earlier_starts as (
        select s.account_id, min(s.start_date) as start_date
        from subscription s
        inner join
            date_range d
            on s.start_date <= d.calc_date
            and (d.calc_date < s.end_date or s.end_date is null)
        group by account_id
        union
        select s.account_id, s.start_date
        from subscription s
        inner join
            earlier_starts e
            on s.account_id = e.account_id
            and (e.start_date - 31) <= s.end_date
            and s.start_date < e.start_date
    )
select
    e.account_id,
    min(e.start_date) as earliest_start,
    d.calc_date - min(e.start_date) as subscriber_tenure_days
from earlier_starts e
cross join date_range d
group by e.account_id, d.calc_date
order by e.account_id
