---
type: policy
title: Freshness policy
description: How stale a table may get before it is flagged.
tags: [governance]
status: draft
owner: data-eng
stale_after: 2026-12-31
---

Draft. Every `type: table` concept should declare an owner and a refresh
cadence; see [orders](/concepts/orders) for the shape we want.

# Rules

1. Core tables refresh daily.
2. A table with no `generated.at` inside 48h is flagged stale.

## Exceptions

Archive tables are exempt.
