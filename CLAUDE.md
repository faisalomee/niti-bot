# Crypto Trading Bot Project

You are my crypto trading and backtesting assistant.

## Main Goal

Help me develop, test and improve crypto trading strategies using my existing historical trading data.

## IMPORTANT DATA RULE

Use the existing trading data stored in this repository.

Do NOT ask me to upload the same trading data again if it already exists in the repository.

Never modify the original historical data.

Do not print the entire dataset in chat.

Use Python to analyse the data and run backtests.

## Backtesting

For every backtest calculate:

- Total trades
- Win rate
- Net profit
- Profit factor
- Maximum drawdown
- Average R
- Average winning trade
- Average losing trade
- Long performance
- Short performance

Consider fees and slippage when appropriate.

Avoid look-ahead bias and data leakage.

## Experiment System

Every meaningful strategy change must be recorded as a new experiment.

Never overwrite previous experiment results.

Use:

EXP001
EXP002
EXP003
etc.

After every important backtest:

1. Save the result.
2. Update TRADING_MEMORY.md.
3. Record what changed.
4. Record the important results.
5. Record what worked.
6. Record what failed.
7. Record what should be tested next.

## New Chat

When starting a new chat/session:

1. Read CLAUDE.md.
2. Read TRADING_MEMORY.md.
3. Inspect the latest backtest result.
4. Continue from the existing project state.

Do not ask me to explain previous work if it is already recorded in the project.

## Token Efficiency

Keep chat responses concise.

Do not paste large datasets into chat.

Do not paste huge amounts of code unless requested.

Analyse files directly using Python.

Only report important findings.

Do not unnecessarily repeat information already stored in project files.
