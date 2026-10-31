window.BENCHMARK_DATA = {
  "lastUpdate": 1793444204620,
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
      },
      {
        "commit": {
          "author": {
            "email": "yukihanastudy@gmail.com",
            "name": "flipslidersand",
            "username": "flipslidersand"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ba1cdcbd4a43d6d190ad923c4fa7882c1047240f",
          "message": "test: fire_due_schedules の claim テストを分の境界で不安定にしない (#3) (#4)\n\ncron を毎分(\"0 * * * * *\")から毎年 1 月 1 日(\"0 0 0 1 1 *\")に変える。\nclaim 後の next_run_at が数か月先になり、テスト中に分を跨いでも再び due にならない。\nCI で 10:22:00.08Z(分の境界)に失敗したことが根拠。\n\nCo-authored-by: flipslidersand <205122478+flipslidersand@users.noreply.github.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-31T19:37:30+09:00",
          "tree_id": "8c5b691698649b150c05c4d85adda2ddc5dc78b4",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/ba1cdcbd4a43d6d190ad923c4fa7882c1047240f"
        },
        "date": 1793443294122,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 2103478,
            "range": "± 10427",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 53196091,
            "range": "± 1004284",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 32695,
            "range": "± 155",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 130625,
            "range": "± 1380",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 2203703,
            "range": "± 10026",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5973118,
            "range": "± 106350",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 6010940,
            "range": "± 153041",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 14636761,
            "range": "± 355899",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 10702759,
            "range": "± 478836",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 24733948,
            "range": "± 592149",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 15785175,
            "range": "± 376374",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "yukihanastudy@gmail.com",
            "name": "flipslidersand",
            "username": "flipslidersand"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5d3888095832a3401f5bc983dfaf7f9a59e9f941",
          "message": "feat: Release workflow を追加する(事前ビルド済みバイナリ + デモ用 Wasm) (#1) (#2)\n\nタグ(v*) push で CI ビルド → パス混入検査 → draft Release を作成。\nworkflow_dispatch は artifact のみ(Release は作らない)。\n手元ビルドは /home/<user> が 1,000 箇所超埋め込まれるため、CI で\n--remap-path-prefix を付けてビルドし、残っていれば失敗させる。\n\nCo-authored-by: flipslidersand <205122478+flipslidersand@users.noreply.github.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-31T19:52:53+09:00",
          "tree_id": "008ed60cbc7fdec1ea535d5065d306bb0e74d20f",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/5d3888095832a3401f5bc983dfaf7f9a59e9f941"
        },
        "date": 1793444203967,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1245789,
            "range": "± 16760",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 27739138,
            "range": "± 831081",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 20043,
            "range": "± 240",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 81364,
            "range": "± 1134",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1245593,
            "range": "± 16784",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5205310,
            "range": "± 288592",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 5151520,
            "range": "± 873563",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 12191404,
            "range": "± 606594",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 9600907,
            "range": "± 325794",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 19256892,
            "range": "± 1346857",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 14776102,
            "range": "± 1155369",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}