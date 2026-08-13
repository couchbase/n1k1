---
type: dashboard
title: Revenue by region
description: Weekly revenue split by region, for the exec review.
tags: [sales, exec]
status: stable
owner: analytics
resource: https://dashboards.example.com/revenue-by-region
---

Built on [orders](/concepts/orders) joined to the regional mapping. See the
[warehouse docs](https://example.com/warehouse) for how the region column is
derived.

# Definition

Revenue is `SUM(total)` over non-cancelled orders, bucketed by ISO week.

## Caveats

Refunds are not netted out yet.

# Examples

```sql
SELECT region, ROUND(SUM(amount), 2) AS amount
  FROM sales
 GROUP BY region
 ORDER BY amount DESC
```
