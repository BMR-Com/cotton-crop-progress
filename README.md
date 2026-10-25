# US Cotton Crop Monitor

Live page: https://bmr-com.github.io/cotton-crop-progress/

Tabs: **Crop monitor** (this page) · **Price & technicals** (`technicals.html`) · **Production model** · **Conditional FV** · **Master model** · **Spreads** · **Classic charts**.

A weekly view of the US cotton crop:

- **Crop estimate by state** (US upland, 480-lb bales), alongside USDA's latest forecast
- **Weekly brief**: ratings, Texas abandonment, gaps to USDA, harvest, ginnings, classing and quality
- **Crop conditions, drought and progress**, with this year, last year and the five-year average
- **Classing pace**: upland bales classed by gin state, season to date and weekly
- **NASS ginnings**: running bales ginned by state
- **Fibre quality and tenderability**: color, leaf, staple, mike, strength and uniformity, this season vs last
- **Crop projection from ginnings**: ginning and classing so far against harvest progress, on projected harvested acres
- **Price & technicals**: candlestick charts of the continuous contract and the active Dec/Mar/May/Jul contracts, with moving averages, Bollinger/Keltner/Donchian bands, Ichimoku, Parabolic SAR, pivots, RSI, MACD, stochastics, ADX, ATR, CCI, Williams %R, OBV, MFI and momentum, plus a daily technical summary

## Crop projection

The weekly brief opens with the BMR-Com projection of the final US crop (upland and all cotton, Texas, with 68/95/99% ranges), published in `docs/data/crop_projection.json`. It is built from:

- NASS crop progress by state (squaring, setting bolls, bolls opening, harvested) and crop condition ratings
- US Drought Monitor share of cotton area in D2–D4, weighted by crop stage, through October
- NASS Cotton Ginnings by state, converted from running bales to 480-lb bales of the state where the cotton was grown
- USDA AMS bales classed by classing office (Lubbock, Lamesa, Abilene and Corpus Christi split West and South Texas)
- USDA FSA planted and failed acres, and NASS planted acres
- Target: NASS final upland production (May annual report)

## Data (refreshed automatically)

| Source | What | Folder |
|---|---|---|
| USDA NASS Crop Progress | weekly condition and progress by state | `data/cotton_*.csv` |
| USDA NASS QuickStats | ginnings by state; acreage, yield and production | `data/ginning/` |
| USDA AMS MyMarketNews (report 1654) | weekly cotton classing by gin state | `data/classing/` |
| USDA AMS classing by classing office (weekly, archive from 2006) | bales classed by office | `data/classing/office.csv` |
| NASS Cotton Ginnings report archive (ESMIS) | state ginnings for crop years before QuickStats coverage (2008–2013) | `data/ginning/ginnings_esmis.csv` |
| USDA FSA crop acreage data | planted, failed and prevented acres by state | `data/fsa/` |
| US Drought Monitor | D0–D4 area by state (via BMR-Com/drought-monitor) | fetched at build |
| Yahoo Finance / ICE settlements | cotton futures prices (charts, technicals, model tabs) | `docs/data/technicals.json`, `docs/data/cotton_px.csv` |
| WASDE (via BMR-Com/wasde-analytics) | US production and ending stocks used to prefill the model tabs | fetched at build |

API keys are read from repository secrets (`USDA_API_KEY`, `AMS_API_KEY`) and never stored in the repo.

## Schedule

- Crop Progress: Mondays after the 4 pm ET release, with a Tuesday catch-up
- Ginnings, classing and FSA: weekdays, August through May
- The page data is rebuilt after each data update and every weekday after the ICE close (prices and technicals)

Research output only; not investment advice.
