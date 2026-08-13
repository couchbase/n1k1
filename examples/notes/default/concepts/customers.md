---
type: table
title: Customers
description: One row per customer account.
tags: [crm, core]
status: stable
owner: growth
generated:
  by: dbt
  at: 2026-07-01T10:04:00Z
---

Account-level dimension table, joined to [orders](/concepts/orders) on
`orders.customer = customers.id`.

# Schema

- `id` — customer id
- `name` — display name
- `city` — billing city
- `since` — first order year

# Examples

```sql
SELECT city, COUNT(*) AS n FROM customers GROUP BY city
```
