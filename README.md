# US Cotton Crop Monitor

Live page: https://bmr-com.github.io/cotton-crop-progress/

A weekly view of the US cotton crop:

- **Crop estimate by state** (US upland, 480-lb bales), alongside USDA's latest forecast
- **Weekly brief**: ratings, Texas abandonment, gaps to USDA, harvest, ginnings, classing and quality
- **Crop conditions, drought and progress**, with this year, last year and the five-year average
- **Classing pace**: upland bales classed by gin state, season to date and weekly
- **NASS ginnings**: running bales ginned by state
- **Fibre quality and tenderability**: color, leaf, staple, mike, strength and uniformity, this season vs last

## Data (refreshed automatically)

| Source | What | Folder |
|---|---|---|
| USDA NASS Crop Progress | weekly condition and progress by state | `data/cotton_*.csv` |
| USDA NASS QuickStats | ginnings by state; acreage, yield and production | `data/ginning/` |
| USDA AMS MyMarketNews (report 1654) | weekly cotton classing by gin state | `data/classing/` |
| USDA FSA crop acreage data | planted, failed and prevented acres by state | `data/fsa/` |
| US Drought Monitor | D0–D4 area by state (via BMR-Com/drought-monitor) | fetched at build |

API keys are read from repository secrets (`USDA_API_KEY`, `AMS_API_KEY`) and never stored in the repo.

## Schedule

- Crop Progress: Mondays after the 4 pm ET release, with a Tuesday catch-up
- Ginnings, classing and FSA: weekdays, August through May
- The page data (`docs/data/crop.json`) is rebuilt after each data update

Research output only; not investment advice.
