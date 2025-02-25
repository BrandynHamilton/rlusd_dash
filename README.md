# RLUSD Dashboard Documentation

This repo holds the code associated with the RLUSD Dashboard which can be found here: https://www.optimizerfinance.com/rlusd_dash/

## Overview

RLUSD is a multi-chain stablecoin existing on both Ethereum and XRPL, requiring data from multiple sources to provide an accurate overview of its supply, liquidity, and trading volume. The RLUSD Dashboard consolidates this information through several API integrations and smart contract queries.

## Methodology
### Data Sources and Collection
- Supply:
  - Ethereum RLUSD Supply: Collected hourly via the TotalSupply function of the RLUSD contract on Ethereum.
  - XRPL RLUSD Supply: Queried hourly using the [XRPScan API](https://docs.xrpscan.com/)
- RLUSD in Liquity Pools:
  - RLUSD Locked in XRPL's Liquidity Pool: Collected hourly through the XRPScan API to track balances in the main [RLUSD/XRP liquidity pool](https://www.geckoterminal.com/xrpl/pools/524C555344000000000000000000000000000000.rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De_XRP).
  - RLUSD Locked in Liquidity Pools on Ethereum: Flipside query which tracks the two main pools on [Uniswap](https://www.geckoterminal.com/eth/pools/0xcc6d2f26d363836f85a42d249e145ec0320d3e55) and [Curve](https://www.geckoterminal.com/eth/pools/0xd001ae433f254283fece51d4acce8c53263aa186)
- DEX Trading Volume:
  - Ethereum: collected via a [Dune analytics query](https://dune.com/queries/4695750/7808654)
  - XRPL: Collected using GeckoTerminal data for the main [RLUSD/XRP pool](https://www.geckoterminal.com/xrpl/pools/524C555344000000000000000000000000000000.rMxCKbEDwqr76QuheSUMdEGf4B9xJ8m5De_XRP)
### Refresh Schedule
- The dashboard refreshes weekly due to the costs associated with integrating data from 5+ APIs.
- Hourly Data Collection: The data_collection.py script collects hourly data for:
  - RLUSD supply on Ethereum
  - RLUSD supply on XRPL
  - RLUSD locked in XRPL's DEX
### Data Processing and Visualization
- Backend: The data_processing.py script is responsible for querying all APIs, processing the data, and preparing it for visualization.
- Dashboard Framework: The dashboard is built using Dash (a Python framework for web-based data visualization).

## Installation
- Clone the Repo
- Create a Python Virtual Environment
- Install Dependencies
- Create .env file to store API keys and other environment variables

## Future Improvements
- Automate Liquidity Pool data collection (no more hardcoding the exact Uniswap and Curve pools)
- Add more RLUSD usage metrics (active addresses, # of Txs, etc)
- RLUSD yield metrics (APY for RLUSD Liquidity Pools, Lending Protocols, etc)
- Total supply over time

## Contributing
- Contributions are welcome!  Please open an issue or submit a pull request for any enhancements or bug fixes.

## Contact
- For questions or support, please contact Brandyn Hamilton at [brandynham1120@gmail.com](brandynham1120@gmail.com) or reach out via X/Twitter @bhami628.
