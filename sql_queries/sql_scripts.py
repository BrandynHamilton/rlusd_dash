def lp_data(address,start_date='2024-12-17 00:00:00'):
    query = f"""

  WITH RECURSIVE date_series AS (
  SELECT
    TIMESTAMP '{start_date}' AS DT
  UNION
  ALL
  SELECT
    DT + INTERVAL '1 HOUR'
  FROM
    date_series
  WHERE
    DT + INTERVAL '1 HOUR' <= CURRENT_TIMESTAMP
),



main_tokens as (

SELECT
    USD_VALUE_NOW,
    symbol, 
    CONTRACT_ADDRESS,
    CURRENT_BAL,
    decimals,
  FROM
    ethereum.core.ez_current_balances
  WHERE
    user_address = lower('{address}')
  AND USD_VALUE_NOW IS NOT NULL
  order by USD_VALUE_NOW desc 
  LIMIT 2
),

symbols AS (
  SELECT
    distinct symbol from main_tokens
),
date_series_symbol AS (
  SELECT
    d.DT,
    s.symbol
  FROM
    date_series d
    CROSS JOIN symbols s
),

hourly_lp AS (
  SELECT
    date_trunc('hour', block_timestamp) AS dt,
    symbol,
    AVG(current_bal) AS current_bal,
    MAX(decimals) AS decimals
  FROM
    ethereum.core.ez_balance_deltas
  WHERE
    user_address = lower('{address}')
    AND contract_address in (select distinct contract_address from main_tokens)
  GROUP BY
    date_trunc('hour', block_timestamp),
    symbol
),
joined_data AS (
  SELECT
    dss.DT,
    dss.symbol,
    hlp.current_bal,
    hlp.decimals
  FROM
    date_series_symbol dss
    LEFT JOIN hourly_lp hlp ON dss.DT = hlp.dt
    AND dss.symbol = hlp.symbol
),
front_filled_data AS (
  SELECT
    DT,
    symbol,
    LAST_VALUE(current_bal IGNORE NULLS) OVER (
      PARTITION BY symbol
      ORDER BY
        DT ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS current_bal,
    LAST_VALUE(decimals IGNORE NULLS) OVER (
      PARTITION BY symbol
      ORDER BY
        DT ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS decimals
  FROM
    joined_data
),
prices as (
  select
    hour,
    symbol,
    price
  from
    ethereum.price.ez_prices_hourly
  where
    token_address in (select distinct contract_address from main_tokens)
    and hour <= date_trunc('hour',current_timestamp)
  order by
    hour desc
),
tvl_per_token as (
  SELECT
    ff.DT,
    ff.symbol,
    ff.current_bal,
    p.price,
    ff.current_bal * p.price as TVL
  FROM
    front_filled_data ff
    join prices p on p.hour = ff.DT
    and p.symbol = ff.symbol
  ORDER BY
    DT,
    symbol
),
total_tvl as (
  select
    dt,
    sum(TVL) over (
      partition by dt
      order by
        dt desc
    ) as Total_TVL
  from
    tvl_per_token
  order by
    dt desc
)
select
  a.dt,
  a.symbol,
  a.current_bal,
  a.TVL,
  sum(a.TVL) over (
    partition by dt
    order by
      dt desc
  ) as Total_TVL
from
  tvl_per_token a
order by
  a.dt desc

"""
    return query