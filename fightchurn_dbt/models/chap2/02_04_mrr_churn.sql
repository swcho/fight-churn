WITH date_range AS (
  -- 분석 범위: 시작날짜, 끝날짜
  SELECT
    '2020-03-01' :: DATE AS start_date,
    '2020-04-01' :: DATE AS end_date
),
start_accounts AS (
  -- 시작날짜 기준 가입자 목록과 mrr 합계
  SELECT
    account_id,
    SUM(mrr) AS total_mrr
  FROM
    socialnet7.subscription s
    INNER JOIN date_range d ON s.start_date <= d.start_date
    AND (
      d.start_date < s.end_date
      OR s.end_date IS NULL
    )
  GROUP BY
    account_id
),
end_accounts AS (
  -- 끝날짜 기준 가입자 목록과 mrr 합계
  SELECT
    account_id,
    SUM(mrr) AS total_mrr
  FROM
    subscription s
    INNER JOIN date_range d ON s.start_date <= d.end_date
    AND (
      d.end_date < s.end_date
      OR s.end_date IS NULL
    )
  GROUP BY
    account_id
),
churned_accounts AS (
  -- 이탈 가입자 목록: 시작날짜에만 존재하는 가입자 목록과 mrr 합계
  SELECT
    s.account_id,
    SUM(s.total_mrr) AS total_mrr
  FROM
    start_accounts s
    LEFT OUTER JOIN end_accounts e ON s.account_id = e.account_id
  WHERE
    e.account_id IS NULL
  GROUP BY
    s.account_id
),
downsell_accounts AS (
  -- 다운셀, 다운판매(downsell) 목록: 끝날짜의 mrr이 시작날짜의 mrr보다 줄은 계정과 그 차이
  SELECT
    s.account_id,
    s.total_mrr - e.total_mrr AS downsell_amount
  FROM
    start_accounts s
    INNER JOIN end_accounts e ON s.account_id = e.account_id
  WHERE
    e.total_mrr < s.total_mrr
),
start_mrr AS (
  -- 시작날짜의 mrr 합계
  SELECT
    SUM(total_mrr) AS start_mrr
  FROM
    start_accounts
),
churn_mrr AS (
  -- 이탈가입자 mrr 합계
  SELECT
    SUM(total_mrr) AS churn_mrr
  FROM
    churned_accounts
),
downsell_mrr AS (
  -- 다운판매 mrr 합계
  SELECT
    COALESCE(SUM(downsell_amount), 0.0) AS downsell_mrr
  FROM
    downsell_accounts
)
SELECT
  (
    churn_mrr :: FLOAT + downsell_mrr :: FLOAT
  ) / start_mrr :: FLOAT AS mrr_churn_rate,
  start_mrr,
  churn_mrr,
  downsell_mrr
FROM
  start_mrr,
  churn_mrr,
  downsell_mrr