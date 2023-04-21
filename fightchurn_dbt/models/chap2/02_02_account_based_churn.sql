WITH date_range AS (
  -- 분석할 시작일 끝일
  SELECT
    '2020-03-01' :: DATE AS start_date,
    '2020-04-01' :: DATE AS end_date
),
start_accounts AS (
  -- 시작일 구독자
  SELECT
    DISTINCT account_id
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
),
end_accounts AS (
  -- 끝일 구독자
  SELECT
    DISTINCT account_id
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
),
churned_accounts AS (
  -- 구독자를 left outer join 해서 시작일 구독자 목록 기준으로 목록 생성
  -- 끝일 구독자가 없는 것, 즉 구독 해지한 사용자 필터링하여 churned 목록 생성
  SELECT
    s.account_id
  FROM
    start_accounts s
    LEFT OUTER JOIN end_accounts e
    ON s.account_id = e.account_id
  WHERE
    e.account_id IS NULL
),
start_count AS (
  -- 시작일 사용자 수
  SELECT
    COUNT(*) AS n_start
  FROM
    start_accounts
),
churn_count AS (
  -- churned 사용자 수
  SELECT
    COUNT(*) AS n_churn
  FROM
    churned_accounts
)
SELECT
  -- churn 사용자 수 / 시작일 사용자 수 = churn rate
  n_churn :: FLOAT / n_start :: FLOAT AS churn_rate,
  -- 1 - churn 사용자 수 / 시작일 사용자 수 = retention rate
  1.0 - n_churn :: FLOAT / n_start :: FLOAT AS retention_rate,
  n_start,
  n_churn
FROM
  start_count,
  churn_count
