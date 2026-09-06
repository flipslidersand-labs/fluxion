window.BENCHMARK_DATA = {
  "lastUpdate": 1788706597841,
  "repoUrl": "https://github.com/flipslidersand-labs/fluxion",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "name": "flipslidersand",
            "email": "yukihanashopping0212@gmail.com"
          },
          "committer": {
            "name": "flipslidersand",
            "email": "yukihanashopping0212@gmail.com"
          },
          "id": "e336ccc702ab15b8c9ec0e2ef8243d6a54b1a840",
          "message": "fix(ci): bench ジョブに contents: write を付与 — gh-pages push 権限不足を解消\n\nリポジトリの default_workflow_permissions が read のため、\ngithub-action-benchmark の gh-pages push (auto-push: true) が\n'Permission ... denied to github-actions[bot]' で失敗していた。\nbench ジョブ単体に contents: write を明示付与する。\n\n併せて gh-pages ブランチが存在しなかった問題は別途 orphan ブランチ作成で解消済み。\n\nCo-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-06T14:43:49Z",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/e336ccc702ab15b8c9ec0e2ef8243d6a54b1a840"
        },
        "date": 1788706597311,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1944400,
            "range": "± 13762",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 43366929,
            "range": "± 929186",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 32937,
            "range": "± 390",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 131876,
            "range": "± 2555",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1991829,
            "range": "± 7754",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5340526,
            "range": "± 39426",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 5369591,
            "range": "± 54449",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 12296449,
            "range": "± 123539",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 8036688,
            "range": "± 170223",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 19613112,
            "range": "± 210830",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 11259389,
            "range": "± 134734",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}