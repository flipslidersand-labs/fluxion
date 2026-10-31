window.BENCHMARK_DATA = {
  "lastUpdate": 1793442130040,
  "repoUrl": "https://github.com/flipslidersand-labs/fluxion",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "205122478+flipslidersand@users.noreply.github.com",
            "name": "flipslidersand",
            "username": "flipslidersand"
          },
          "committer": {
            "email": "205122478+flipslidersand@users.noreply.github.com",
            "name": "flipslidersand",
            "username": "flipslidersand"
          },
          "distinct": true,
          "id": "68d87675ec7614ca62fe4d2e04553672195ecfc4",
          "message": "docs: fix repository links",
          "timestamp": "2026-10-31T18:48:32+09:00",
          "tree_id": "b964433d424729d01d8161656c45521063cab623",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/68d87675ec7614ca62fe4d2e04553672195ecfc4"
        },
        "date": 1793442129441,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1235809,
            "range": "± 112288",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 31151829,
            "range": "± 3089714",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 20320,
            "range": "± 489",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 81766,
            "range": "± 1608",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1263406,
            "range": "± 24779",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5719621,
            "range": "± 577474",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 5358566,
            "range": "± 319387",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 12967259,
            "range": "± 514077",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 10334154,
            "range": "± 494718",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 21208109,
            "range": "± 1158835",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 15671744,
            "range": "± 587801",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}