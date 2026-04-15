# Web3 智能资金行为分析（FET / RNDR）

<p align="left">
  <img src="https://img.shields.io/badge/Track-AI%20Tokens-6C5CE7" alt="Track"/>
  <img src="https://img.shields.io/badge/Chain-Ethereum%20%7C%20Solana-0984E3" alt="Chain"/>
  <img src="https://img.shields.io/badge/Window-45%20Days-00B894" alt="Window"/>
  <img src="https://img.shields.io/badge/Stack-DuneSQL-FDCB6E" alt="Stack"/>
</p>

> 识别高置信 Smart Money 候选地址，并把链上资金流转成可解释的风险信号。

## 项目介绍

在 AI 代币波动较大的阶段，仅看价格通常不够。  
这个项目通过 DuneSQL 对链上地址行为做结构化分析，核心目标是：

- 识别高置信 Smart Money 候选地址
- 观察候选群体的资金变化和收益结构
- 输出可读、可复盘的风险信号看板

## 结果图示（建议阅读顺序）

- Dune Dashboard: [smart-money-fet-rndr-pnl](https://dune.com/wudide/smart-money-fet-rndr-pnl)

### 1) 总览 KPI（先看全局）

![总览 KPI](assets/dashboard/Image%20display%201.png)

### 2) 候选池趋势与地址画像（再看过程）

![趋势与画像](assets/dashboard/Image%20display%202.png)

### 3) 成本与分层结果（最后看结论）

![成本与分层](assets/dashboard/Image%20display%203.png)

## 项目亮点

- 双链统一口径：同一框架下观察 FET（Ethereum）与 RNDR（Solana）趋势。
- 候选筛选可复现：通过规则组合筛选地址，而非手工挑选。
- 结论可解释：从候选池到收益分层，指标之间有连续逻辑。

## 如何阅读这个项目

1. 先看上面的 3 张图，按“总览 -> 过程 -> 结论”快速理解项目输出。  
2. 查看 Dashboard 交互版，确认指标与图表联动。  
3. 按 `D3 -> D4 -> D5` 顺序阅读 SQL，再看完整说明文档。  

## 快速跳转

- Dashboard：<https://dune.com/wudide/smart-money-fet-rndr-pnl>
- SQL：
  - [D3_candidate_pool_fet_eth_rndr_sol.sql](sql/D3_candidate_pool_fet_eth_rndr_sol.sql)
  - [D4_cost_basis_fet_eth.sql](sql/D4_cost_basis_fet_eth.sql)
  - [D4_cost_basis_validation_fet_eth_rndr_sol.sql](sql/D4_cost_basis_validation_fet_eth_rndr_sol.sql)
  - [D5_pnl_snapshot_fet_eth.sql](sql/D5_pnl_snapshot_fet_eth.sql)
- 项目长文档：[项目简介.md](项目简介.md)
