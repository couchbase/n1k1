---
type: table
title: Orders
description: One row per customer order.
tags: [sales, core]
status: stable
owner: data-eng
generated:
  by: dbt
  at: 2026-07-01T10:00:00Z
sources:
  - resource: bigquery://acme/raw/orders_raw
    author: ingest
---

The canonical order fact table. One row per placed order, including cancelled
ones — filter on `status` if you only want fulfilled revenue.

# Schema

- `id` — order id, unique
- `customer` — FK to [customers](/concepts/customers)
- `total` — order total, in USD
- `status` — `shipped` | `pending` | `cancelled`

## Notes

Cancelled rows are kept on purpose, so churn analysis can see them. The
[refunds](/concepts/refunds) concept does not exist yet — that link is
deliberately dangling, so the dead-link query below has something to find.

# Examples

```sql
SELECT status, COUNT(*) AS n, ROUND(SUM(total), 2) AS revenue
  FROM orders
 GROUP BY status
```
