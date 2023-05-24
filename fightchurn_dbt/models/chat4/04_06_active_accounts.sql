with metric_date as (select max(metric_time) as last_metric_time from metric)
select
    m.account_id,
    m.metric_time,
    sum(
        case when m.metric_name_id = 0 then m.metric_value else 0 end
    ) as like_per_month,
    sum(
        case when m.metric_name_id = 1 then m.metric_value else 0 end
    ) as newfriend_per_month,
    sum(
        case when m.metric_name_id = 2 then m.metric_value else 0 end
    ) as post_per_month,
    sum(
        case when m.metric_name_id = 3 then m.metric_value else 0 end
    ) as adview_feed_per_month,
    sum(
        case when m.metric_name_id = 4 then m.metric_value else 0 end
    ) as dislike_per_month,
    sum(
        case when m.metric_name_id = 5 then m.metric_value else 0 end
    ) as unfriend_per_month,
    sum(
        case when m.metric_name_id = 6 then m.metric_value else 0 end
    ) as message_per_month,
    sum(
        case when m.metric_name_id = 7 then m.metric_value else 0 end
    ) as reply_per_month,
    sum(case when m.metric_name_id = 8 then m.metric_value else 0 end) as account_tenure
from metric m
inner join metric_date d on m.metric_time = d.last_metric_time
inner join subscription s on m.account_id = s.account_id
where
    (d.last_metric_time < s.end_date or s.end_date is null)
    and s.start_date <= d.last_metric_time
group by m.account_id, m.metric_time
order by m.account_id
