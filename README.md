# Sales Analytics Pipeline

A Databricks notebook, written in pandas, that takes a month of sales
transactions through the usual steps of a batch pipeline: quality checks,
transformations, customer segments, KPIs, summary tables and CSV export.

**The data is generated, not real:** 1,000 transactions across 8 products
and about 200 customers, with a fixed random seed so every run gives the
same numbers. The point is the pipeline steps, which work the same on real
sales data.

## What each step does

1. **Generate data:** 1,000 transactions in July 2025 with product,
   quantity, price (0.8 to 1.1 times list price), channel, payment method
   and region.
2. **Quality checks:** missing values, duplicate transaction ids and
   non-positive prices, combined into a 0 to 100 health score.
3. **Transform:** date parts (year, month, day, hour, weekday), an order
   size band, and per-customer totals, order counts and average order
   value. Customers are labelled VIP (over $2,000 and over 10 orders),
   Regular (over $1,000 or over 5 orders) or New.
4. **KPIs:** total revenue, average order value, unique customers, share
   of repeat customers, and revenue by category, channel and product.
5. **More analysis:** revenue by hour, weekend against weekday, and by
   region.
6. **Summary tables:** monthly revenue, a category by channel matrix, and
   revenue by customer segment.
7. **Export:** writes seven tables to `output/*.csv`.

## Output

From a run of the notebook:

```
💰 Total Revenue: $1,404,033.33
🛒 Total Transactions: 1,000
📊 Average Order Value: $1404.03
👥 Unique Customers: 199
🔄 Repeat Customers: 98.0%

🏆 TOP PERFORMING CATEGORY: Electronics ($1,134,575.87)
📱 BEST SALES CHANNEL: Online ($505,691.47)
⏰ Peak Sales Hour: 11:00 ($79,847.50)
📅 Weekend vs Weekday Revenue: $380,348.59 vs $1,023,684.74
🎖️ VIP Customers: 5
🌟 Regular Customers: 187
```

The other 7 customers are New.

## How to run it

**On Databricks:** import `Sales_Analytics_Pipeline.py` as a notebook
(it is in Databricks source format), attach it to any cluster and run all
cells.

**On your own machine:** it needs only pandas and NumPy. Outside Databricks
or Jupyter, tables print as plain text.

```bash
pip install pandas numpy
python Sales_Analytics_Pipeline.py
```

## Limits

- **Generated data.** Customers, prices and dates are random, so the
  results say nothing about a real business.
- **The segment rules are too loose for this data.** With an average order
  of $1,404, almost every customer passes the $1,000 bar, so 187 of 199
  land in Regular. Thresholds based on percentiles would split them more
  usefully.
- **pandas, not Spark.** It runs on the Databricks driver only. For large
  data the same steps would move to PySpark.
- **No tests yet.**

## Author

Tiiso Letsapa, Letsapamyron07@gmail.com
