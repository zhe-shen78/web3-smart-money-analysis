with params as (
  select
    'FET' as token_symbol,
    'ethereum' as chain_name,
    lower('0xaea46a60368a7bd060eec7df8cba43b7ef41ad85') as evm_contract,
    cast(null as varchar) as sol_mint,
    45 as lookback_days,
    10 as min_active_days,
    1000 as min_net_flow_usd
  union all
  select
    'RNDR_SOL' as token_symbol,
    'solana' as chain_name,
    cast(null as varchar) as evm_contract,
    'rndrizKT3MK1iimdxRdWabcF7Zg7AR5T4nud4EkHBof' as sol_mint,
    45 as lookback_days,
    10 as min_active_days,
    1000 as min_net_flow_usd
),
evm_flows as (
  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    lower(concat('0x', to_hex(t."to"))) as address_key,
    cast(t.amount as double) as flow_token,
    coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens.transfers t
    on p.chain_name = 'ethereum'
   and t.blockchain = 'ethereum'
   and t.contract_address = from_hex(substring(p.evm_contract, 3))
  where p.token_symbol = 'FET'
    and t."to" is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())

  union all

  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    lower(concat('0x', to_hex(t."from"))) as address_key,
    -cast(t.amount as double) as flow_token,
    -coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens.transfers t
    on p.chain_name = 'ethereum'
   and t.blockchain = 'ethereum'
   and t.contract_address = from_hex(substring(p.evm_contract, 3))
  where p.token_symbol = 'FET'
    and t."from" is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())
),
sol_flows as (
  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    t.to_owner as address_key,
    cast(t.amount as double) as flow_token,
    coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens_solana.transfers t
    on p.chain_name = 'solana'
   and t.token_mint_address = p.sol_mint
  where p.token_symbol = 'RNDR_SOL'
    and t.to_owner is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())

  union all

  select
    p.token_symbol,
    p.chain_name,
    date_trunc('day', t.block_time) as d,
    t.from_owner as address_key,
    -cast(t.amount as double) as flow_token,
    -coalesce(cast(t.amount_usd as double), 0) as flow_usd
  from params p
  join tokens_solana.transfers t
    on p.chain_name = 'solana'
   and t.token_mint_address = p.sol_mint
  where p.token_symbol = 'RNDR_SOL'
    and t.from_owner is not null
    and t.block_time >= date_add('day', -p.lookback_days, now())
),
all_flows as (
  select * from evm_flows
  union all
  select * from sol_flows
),
daily_address_flow as (
  select
    token_symbol,
    chain_name,
    d,
    address_key,
    sum(flow_token) as net_flow_token_daily,
    sum(flow_usd) as net_flow_usd_daily
  from all_flows
  where flow_usd is not null
    and not (chain_name = 'ethereum' and address_key = '0x0000000000000000000000000000000000000000')
  group by 1,2,3,4
),
address_state as (
  select
    token_symbol,
    chain_name,
    d,
    address_key,
    sum(net_flow_token_daily) over (
      partition by token_symbol, chain_name, address_key
      order by d
      rows between unbounded preceding and current row
    ) as net_flow_token_to_date,
    sum(net_flow_usd_daily) over (
      partition by token_symbol, chain_name, address_key
      order by d
      rows between unbounded preceding and current row
    ) as net_flow_usd_to_date,
    count(*) over (
      partition by token_symbol, chain_name, address_key
      order by d
      rows between unbounded preceding and current row
    ) as active_days_to_date
  from daily_address_flow
),
eligible_daily as (
  select
    s.token_symbol,
    s.chain_name,
    s.d,
    s.address_key,
    s.net_flow_token_to_date,
    s.net_flow_usd_to_date
  from address_state s
  join params p
    on s.token_symbol = p.token_symbol
   and s.chain_name = p.chain_name
  where s.active_days_to_date >= p.min_active_days
    and s.net_flow_usd_to_date > 0
),
daily_thresholds as (
  select
    token_symbol,
    chain_name,
    d,
    approx_percentile(net_flow_usd_to_date, 0.8) as p80_net_flow_usd_to_date,
    count(*) as eligible_address_count
  from eligible_daily
  group by 1,2,3
),
daily_candidates as (
  select
    e.token_symbol,
    e.chain_name,
    e.d,
    e.address_key,
    e.net_flow_token_to_date,
    e.net_flow_usd_to_date
  from eligible_daily e
  join daily_thresholds t
    on e.token_symbol = t.token_symbol
   and e.chain_name = t.chain_name
   and e.d = t.d
  join params p
    on e.token_symbol = p.token_symbol
   and e.chain_name = p.chain_name
  where e.net_flow_usd_to_date >= t.p80_net_flow_usd_to_date
    and e.net_flow_usd_to_date >= p.min_net_flow_usd
),
price_daily as (
  select
    date_trunc('day', minute) as d,
    avg(case when symbol = 'FET' then price end) as fet_price_usd,
    avg(case when symbol = 'RNDR' then price end) as rndr_price_usd
  from prices.usd
  where symbol in ('FET', 'RNDR')
    and minute >= date_add(
      'day',
      -(select max(lookback_days) from params),
      now()
    )
  group by 1
)
select
  t.token_symbol,
  t.chain_name,
  t.d,
  t.p80_net_flow_usd_to_date,
  t.eligible_address_count,
  p.fet_price_usd,
  p.rndr_price_usd,
  case
    when t.token_symbol = 'FET' then p.fet_price_usd
    when t.token_symbol = 'RNDR_SOL' then p.rndr_price_usd
    else null
  end as token_price_usd,
  count(distinct c.address_key) as candidate_count_daily,
  coalesce(sum(c.net_flow_token_to_date), 0) as candidate_net_flow_token_sum_daily,
  coalesce(sum(c.net_flow_usd_to_date), 0) as candidate_net_flow_usd_sum_daily,
  coalesce(sum(c.net_flow_usd_to_date), 0) / nullif(count(distinct c.address_key), 0) as candidate_avg_net_flow_usd_daily,
  avg(t.p80_net_flow_usd_to_date) over (
    partition by t.token_symbol, t.chain_name
    order by t.d
    rows between 6 preceding and current row
  ) as p80_net_flow_usd_to_date_ma7
from daily_thresholds t
left join daily_candidates c
  on t.token_symbol = c.token_symbol
 and t.chain_name = c.chain_name
 and t.d = c.d
left join price_daily p
  on t.d = p.d
group by 1,2,3,4,5,6,7
order by t.d desc, t.token_symbol
