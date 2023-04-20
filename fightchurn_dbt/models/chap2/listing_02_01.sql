WITH date_range AS (
  -- churn 계산을 할 날짜 범위 지정
  SELECT
    '2020-03-01' :: DATE AS start_date,
    '2020-04-01' :: DATE AS end_date
),
start_accounts AS (
  -- 시작 시점에 구동중인 계정과 그 mrr
  SELECT
    account_id,
    SUM(mrr) AS total_mrr
  FROM
    {{ source(
      'churn',
      'subscription'
    ) }}
    s
    INNER JOIN date_range d
    ON s.start_date <= d.start_date
    AND (
      d.start_date < s.end_date
      OR s.end_date IS NULL
    )
  GROUP BY
    account_id
),
end_accounts AS (
  SELECT
    account_id,
    SUM(mrr) AS total_mrr
  FROM
    {{ source(
      'churn',
      'subscription'
    ) }}
    s
    INNER JOIN date_range d
    ON s.start_date <= d.end_date
    AND (
      d.end_date < s.end_date
      OR s.end_date IS NULL
    )
  GROUP BY
    account_id
),
retained_accounts AS (
  SELECT
    s.account_id,
    SUM(
      e.total_mrr
    ) AS total_mrr
  FROM
    start_accounts s
    INNER JOIN end_accounts e
    ON s.account_id = e.account_id
  GROUP BY
    s.account_id
),
start_mrr AS (
  SELECT
    SUM(
      start_accounts.total_mrr
    ) AS start_mrr
  FROM
    start_accounts
),
retain_mrr AS (
  SELECT
    SUM(
      retained_accounts.total_mrr
    ) AS retain_mrr
  FROM
    retained_accounts
)
SELECT
  (
    retain_mrr / start_mrr
  ) AS net_mrr_retention_rate,
  (
    1.0 - retain_mrr / start_mrr
  ) AS net_mrr_churn_rate,
  start_mrr,
  retain_mrr
FROM
  start_mrr,
  retain_mrr
