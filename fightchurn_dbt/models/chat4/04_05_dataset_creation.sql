with
    observation_params as (
        select
            interval '7 day' as metric_period,
            '2020-02-09'::timestamp as obs_start,
            '2020-05-10'::timestamp as obs_end
    )
select
    m.account_id,
    o.observation_date,
    o.is_churn,
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
inner join observation_params p on m.metric_time between p.obs_start and p.obs_end
inner join
    observation o
    on m.account_id = o.account_id
    and (o.observation_date - p.metric_period)::timestamp < m.metric_time
    and m.metric_time <= o.observation_date::timestamp
group by m.account_id, m.metric_time, o.observation_date, o.is_churn
order by o.observation_date, m.account_id
