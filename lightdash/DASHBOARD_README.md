# Lightdash Dashboard Artifacts

This directory drives Lightdash's auto-provisioning. On `make up`, the setup
script discovers all JSON templates here and creates/updates them in Lightdash
automatically.

## Directory layout

```
lightdash/
├── charts/                              # SQL runner chart definitions
│   └── starter_inventory_snapshot.json
├── starter_dashboard.json               # Dashboard definitions (tiles + layout)
├── setup.py                             # Auto-provisioning script
└── DASHBOARD_README.md
```

## How auto-discovery works

| Pattern | What it creates |
|---------|-----------------|
| `charts/*.json` | SQL runner charts (created first) |
| `*.json` (root) | Dashboards with tile layout (created second, can reference chart slugs) |

Charts are synced before dashboards so that tile references resolve correctly.

## Chart JSON format

```json
{
  "name": "My Chart",
  "slug": "my-chart",
  "description": "...",
  "sql": "SELECT ...",
  "limit": 100,
  "config": { "type": "vertical_bar", "fieldConfig": { ... }, "display": { ... } }
}
```

## Dashboard JSON format

```json
{
  "version": 1,
  "slug": "my-dashboard",
  "name": "My Dashboard",
  "description": "...",
  "spaceSlug": "shared",
  "pinToHomepage": true,
  "tabs": [],
  "tiles": [
    { "x": 0, "y": 0, "w": 12, "h": 6, "type": "saved_chart", "properties": { "chartSlug": "my-chart" } }
  ]
}
```

Set `"pinToHomepage": true` to pin a dashboard to the Lightdash home page.

## For candidates

Add your own chart and dashboard JSON files to this directory. They will be
synced to Lightdash the next time `make restart` is run. Reviewers will inspect
your dashboard logic from git history alongside your dbt model changes.
