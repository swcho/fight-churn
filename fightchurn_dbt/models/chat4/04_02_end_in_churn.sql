with recursive
    active_period_params as (
        select
            interval '14 day' as allowed_gap,
            '2020-05-10'::date as observe_end,
            '2020-02-09'::date as observe_start
    ),
    {# subscription has unique start and end dates for every accout  #}
    end_dates as (
        select distinct
            account_id,
            start_date,
            end_date,
            (end_date + allowed_gap)::date as extension_max
        from subscription s
        inner join
            active_period_params p
            on s.end_date between p.observe_start and p.observe_end
    ),
    {# subscriptions contains extend the end dates #}
    extensions as (
        select distinct e.account_id, e.end_date
        from end_dates e
        inner join
            subscription s
            on e.account_id = s.account_id
            and (e.end_date < s.end_date or s.end_date is null)
            and s.start_date <= e.extension_max
    ),
    churns as (
        select e.account_id, e.start_date, e.end_date as churn_date
        from end_dates e
        left outer join
            extensions x on e.account_id = x.account_id and e.end_date = x.end_date
        where x.end_date is null

        union

        select s.account_id, s.start_date, e.churn_date
        from subscription s
        cross join active_period_params p
        inner join
            churns e
            on s.account_id = e.account_id
            and (e.start_date - p.allowed_gap)::date <= s.end_date
            and s.start_date < e.start_date
    )
    {# insert into active_period (account_id, start_date, churn_date) #}
select account_id, min(start_date) as start_date, churn_date
from churns
group by
    account_id,
    churn_date

    {# 1652 #}
