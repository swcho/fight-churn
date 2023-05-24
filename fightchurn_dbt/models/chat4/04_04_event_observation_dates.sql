with recursive
    observation_params as (
        select
            interval '1 month' as obs_interval,
            interval '1 week' as lead_time,
            '2020-02-09'::date as obs_start,
            '2020-05-10'::date as obs_end
    ),
    observations as (
        select
            a.account_id,
            a.start_date,
            1 as obs_count,
            (a.start_date + p.obs_interval - p.lead_time)::date as obs_date,
            case
                when
                    (
                        (
                            (a.start_date + p.obs_interval - p.lead_time)::date
                            <= a.churn_date
                        )
                        and (
                            a.churn_date
                            < (a.start_date + 2 * p.obs_interval - p.lead_time)::date
                        )
                    )
                then true
                else false
            end as is_churn
        from active_period a
        inner join
            observation_params p
            on (p.obs_start + p.obs_interval - p.lead_time)::date < a.churn_date
            or a.churn_date is null

        union

        select
            o.account_id,
            o.start_date,
            (o.obs_count + 1) as obs_count,
            (o.start_date + (o.obs_count + 1) * p.obs_interval - p.lead_time)::date
            as obs_date,
            case
                when
                    (
                        (
                            (
                                o.start_date
                                + (o.obs_count + 1) * p.obs_interval
                                - p.lead_time
                            )::date
                            <= a.churn_date
                        )
                        and (
                            a.churn_date < (
                                o.start_date
                                + (o.obs_count + 1) * p.obs_interval
                                - p.lead_time
                            )::date
                        )
                    )
                then true
                else false
            end as is_churn
        from observations o
        inner join
            observation_params p
            on (o.start_date + (o.obs_count + 1) * p.obs_interval - p.lead_time)::date
            <= p.obs_end
        inner join
            active_period a
            on a.account_id = o.account_id
            and (
                (
                    (
                        o.start_date + (o.obs_count + 1) * p.obs_interval - p.lead_time
                    )::date
                    < a.churn_date
                )
                or a.churn_date is null
            )
            and (
                a.start_date <= (
                    o.start_date + (o.obs_count + 1) * p.obs_interval - p.lead_time
                )::date
            )
    )
select distinct account_id, obs_date, is_churn
from observations o
inner join
    observation_params p on o.obs_date between p.obs_start and p.obs_end

    {# 37999 #}
