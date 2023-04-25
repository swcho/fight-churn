WITH date_range AS (
  -- 분석 대상일, 시작, 끝
  SELECT
    '2020-03-01' :: TIMESTAMP AS start_date,
    '2020-04-01' :: TIMESTAMP AS end_date,
    INTERVAL '1 months' AS inactivity_interval
),
start_accounts AS (
  -- 시작일 1개월 전 부터 시작일까지 이벤트 발생한 계정 목록
  SELECT
    DISTINCT account_id
  FROM
    {{ source(
      'churn',
      'event'
    ) }}
    e
    INNER JOIN date_range d
    ON d.start_date - d.inactivity_interval < e.event_time
    AND e.event_time <= d.start_date
),
start_count AS (
  -- 시작일 기준 사용자 수
  SELECT
    COUNT(
      *
    ) AS n_start
  FROM
    start_accounts
),
end_accounts AS (
  -- 끝일 1개월 전 부터 끝일까지 이벤트 발생한 계정 목록
  SELECT
    DISTINCT account_id
  FROM
    {{ source(
      'churn',
      'event'
    ) }}
    e
    INNER JOIN date_range d
    ON d.end_date - inactivity_interval < e.event_time
    AND e.event_time <= d.end_date
),
end_count AS (
  -- 끝일 기준 사용자 수
  SELECT
    COUNT(*) AS n_end
  FROM
    end_accounts
),
churned_accounts AS (
  -- 시작일 계정 목록에서 끝일 계정 목록을 left outer join 후,
  -- 끝일 계정이 없는 경우 churned 계정 목록
  SELECT
    DISTINCT s.account_id
  FROM
    start_accounts s
    LEFT OUTER JOIN end_accounts e
    ON s.account_id = e.account_id
  WHERE
    e.account_id IS NULL
),
churn_count AS (
  -- churn 개수
  SELECT
    COUNT(*) AS n_churn
  FROM
    churned_accounts
)
SELECT
  n_churn :: FLOAT / n_start :: FLOAT AS churn_rate,
  1.0 - n_churn :: FLOAT / n_start :: FLOAT AS retention_rate,
  n_start,
  n_churn
FROM
  start_count,
  end_count,
  churn_count
