with calc_date as (select '2020-05-06'::timestamp as the_date)
inner join
    calc_date d
    on d.the_date - interval '28 day' < e.event_time
    and e.event_time <= d.the_date
inner join event_type t on e.event_type_id = t.event_type_id
where t.event_type_name = 'like'
group by e.account_id
