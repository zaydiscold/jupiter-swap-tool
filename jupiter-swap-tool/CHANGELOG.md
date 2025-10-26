# Changelog

## [1.2.2] - 2025-10-26

### Fixed
- **ATA Creation Optimization**: Removed excessive RPC calls during gas estimation. ATA checks now happen only during swap execution, preventing rate limiting issues when processing multiple wallets.
- **Sweep-to-BTC-ETH Flow**: Each wallet now sweeps to a single randomly selected token instead of spreading across all targets. Excludes USDT/USDC while including all swappable tokens (SPYX, NVDAX, CRCLX, JUPSOL, etc.).

### Pending
- Jupiter Lend withdraw dust handling for amounts below protocol threshold

## [1.2.1] - Previous Release
- Initial multi-wallet support
- Jupiter Ultra API integration
- Prewritten flow system
