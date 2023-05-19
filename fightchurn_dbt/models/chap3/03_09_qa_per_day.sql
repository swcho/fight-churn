with date_range as (
  select i::timestamp as calc_date
  from generate_series('2020-01-01', '2020-12-31', '1 day'::interval) i
)
select e.event_time::date as event_date, count(*) as n_event
from date_range d left outer join event e
on d.calc_date = e.event_time::date
inner join event_type t on t.event_type_id = e.event_type_id
where t.event_type_name = 'like'
group by event_date
order by event_date
