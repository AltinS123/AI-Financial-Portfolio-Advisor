# AI Portfolio Advisor Architecture

## Overview

The AI Portfolio Advisor is an end-to-end analytics solution built on Microsoft Fabric that combines portfolio data, live market prices, and financial news to generate AI-powered investment insights.

## Data Sources

### Trading212 Portfolio
- Historical buy/sell transactions
- Current portfolio positions
- Deposits
- Dividends

### Yahoo Finance
- Current stock prices
- Previous close
- Market information

### Finnhub API
- Financial news articles
- Company-specific news
- Market headlines

---

## Microsoft Fabric Components

### Fabric Pipeline

The pipeline orchestrates the complete workflow:

1. News Ingestion
2. News Cleaning
3. Sentiment Analysis
4. Market Intelligence
5. Portfolio Assistant
6. Semantic Model Refresh

---

### Lakehouse

The AI_Financial_Lakehouse stores all processed Delta tables used by the solution.

Main tables include:

- financial_news_raw
- financial_news_clean
- financial_news_sentiment
- ticker_market_intelligence
- portfolio_assistant_signals

Portfolio information is accessed through OneLake Shortcuts from the Investment Lakehouse:

- portfolio_positions
- stock_prices

---

### Semantic Model

The Microsoft Fabric Semantic Model provides a single analytical layer consumed by Power BI.

Relationships connect:

- Financial News
- Sentiment
- Market Intelligence
- Portfolio Positions
- Live Market Prices

---

## Power BI

Power BI presents the final analytics through an interactive dashboard including:

- Portfolio Summary
- AI Analysis
- News Insights
- Personalized Portfolio Advisor
