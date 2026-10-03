window.BENCHMARK_DATA = {
  "lastUpdate": 1791031415652,
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
          "id": "39cd6ea9e0c5f1bf8c1b7b0ae49728c5f3a98d0d",
          "message": "feat(issue 775): add design-phase issue template + PR checklist (#222)\n\n- .github/ISSUE_TEMPLATE/feature_request.yml: required checkboxes for\n  interface/edge-cases/test-plan/impact before a feat issue can even be\n  submitted (GitHub issue forms enforce this client-side)\n- .github/ISSUE_TEMPLATE/bug_report.yml\n- .github/ISSUE_TEMPLATE/config.yml: blank_issues_enabled: false\n- .github/pull_request_template.md: checklist item to verify the linked\n  issue carries design-complete before merging a feat PR\n- design-complete label created (repo setting, not in this diff)\n\nGitHub Actions automation to auto-check the label (issue's optional item 3)\ndeliberately deferred — no new workflow file in this PR, given today's\nqa-workflows permissions incident. Can be added as a follow-up once the\nmanual process is proven.\n\nCo-authored-by: flipslidersand <yukihanashopping0212@gmail.com>\nCo-authored-by: Claude Sonnet 5 <noreply@anthropic.com>",
          "timestamp": "2026-09-07T23:56:23+09:00",
          "tree_id": "875e8bbf9b7db6d6b947854c017357585f9ec923",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/39cd6ea9e0c5f1bf8c1b7b0ae49728c5f3a98d0d"
        },
        "date": 1788793759033,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1750330,
            "range": "± 3856",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 44215409,
            "range": "± 668354",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 31754,
            "range": "± 2312",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 124706,
            "range": "± 1173",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1790419,
            "range": "± 10309",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 6352141,
            "range": "± 246771",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 6298927,
            "range": "± 91009",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 14111735,
            "range": "± 177071",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 10311738,
            "range": "± 138943",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 22315282,
            "range": "± 323422",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 14602812,
            "range": "± 184711",
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
          "id": "9c339980228572388bc17d5793ea9ef94ce3d2ad",
          "message": "test(host): cache テストを ci feature で有効化し e2e ジョブで実行 (#302) (#327)\n\n* test(host): cache テストを ci feature で有効化し e2e ジョブで実行 (#302)\n\ncache.rs の2テストを cfg_attr(not(feature=ci), ignore) に変更し、e2e ジョブで --lib も実行。\n\nCo-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>\n\n* style(host): cache.rs の cfg_attr を rustfmt 整形 (#302)\n\nCo-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: flipslidersand <yukihanashopping0212@gmail.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-03T20:48:46+09:00",
          "tree_id": "de07efaf20c70111f881d28a97e44b355bc3fcde",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/9c339980228572388bc17d5793ea9ef94ce3d2ad"
        },
        "date": 1791028669633,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1232713,
            "range": "± 130748",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 24966974,
            "range": "± 1269542",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 20341,
            "range": "± 524",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 81344,
            "range": "± 715",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1255654,
            "range": "± 7542",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 4979191,
            "range": "± 165137",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 5061268,
            "range": "± 206788",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 10500949,
            "range": "± 280205",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 7981964,
            "range": "± 316133",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 16429994,
            "range": "± 235333",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 11725966,
            "range": "± 519657",
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
          "id": "7f526e381b8209031bd10f7b5e4c7ae613d63977",
          "message": "test(host): run_remote_async と async_dispatch フェイルオーバーのテストを追加 (#277) (#356)\n\n* test(host): run_remote_async と async_dispatch フェイルオーバーのテストを追加 (#277)\n\nCo-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>\n\n* test(host): async failover テストのワーカー順序を RoundRobin 明示で固定 (#277)\n\nCo-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>\n\n---------\n\nCo-authored-by: flipslidersand <yukihanashopping0212@gmail.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-03T21:09:50+09:00",
          "tree_id": "b2e5847c8629fde11f6571e41c12d08d9fe40c82",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/7f526e381b8209031bd10f7b5e4c7ae613d63977"
        },
        "date": 1791029645363,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1694825,
            "range": "± 140694",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 38239937,
            "range": "± 2535083",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 26831,
            "range": "± 1826",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 108691,
            "range": "± 7216",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1752477,
            "range": "± 74020",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 7125170,
            "range": "± 2628037",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 7338047,
            "range": "± 1790003",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 23873857,
            "range": "± 9781710",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 14961215,
            "range": "± 7181766",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 28332501,
            "range": "± 8943097",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 27831740,
            "range": "± 7576912",
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
          "id": "3d7c9cadd68d30e65f25bd39d0f7ba93fc2e95c2",
          "message": "refactor(worker): replace rustls-pemfile with rustls pki_types PemObject (#305) (#354)\n\nCo-authored-by: flipslidersand <yukihanashopping0212@gmail.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-03T21:34:49+09:00",
          "tree_id": "8826ef16c77cecda434804d49b6d4c7e4c4951c0",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/3d7c9cadd68d30e65f25bd39d0f7ba93fc2e95c2"
        },
        "date": 1791031166766,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1785819,
            "range": "± 5504",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 44289415,
            "range": "± 623060",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 30840,
            "range": "± 173",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 123453,
            "range": "± 596",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1847057,
            "range": "± 12062",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5826954,
            "range": "± 101628",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 6022557,
            "range": "± 289650",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 13070936,
            "range": "± 220919",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 9559961,
            "range": "± 1216288",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 20565831,
            "range": "± 655286",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 13261432,
            "range": "± 1577079",
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
          "id": "2b5891f1d27026da30f55864fdf1c18feaab1bfb",
          "message": "deps: migrate serde_yaml (deprecated) to serde_yaml_ng 0.10 (#290) (#351)\n\nCo-authored-by: flipslidersand <yukihanashopping0212@gmail.com>\nCo-authored-by: Claude Sonnet 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-10-03T21:39:34+09:00",
          "tree_id": "61491927778293c849e873addc136e21483444b9",
          "url": "https://github.com/flipslidersand-labs/fluxion/commit/2b5891f1d27026da30f55864fdf1c18feaab1bfb"
        },
        "date": 1791031414870,
        "tool": "cargo",
        "benches": [
          {
            "name": "cache/load_hit",
            "value": 1293635,
            "range": "± 25192",
            "unit": "ns/iter"
          },
          {
            "name": "cache/store_cold",
            "value": 26571346,
            "range": "± 2241201",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/50",
            "value": 20179,
            "range": "± 1399",
            "unit": "ns/iter"
          },
          {
            "name": "dag_build/200",
            "value": 84923,
            "range": "± 1553",
            "unit": "ns/iter"
          },
          {
            "name": "run_component/hello_warm_cache",
            "value": 1300331,
            "range": "± 39049",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/1",
            "value": 5204090,
            "range": "± 269720",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/1",
            "value": 4995349,
            "range": "± 399547",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/3",
            "value": 12491449,
            "range": "± 1110360",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/3",
            "value": 8655996,
            "range": "± 342043",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/sequential/5",
            "value": 17345495,
            "range": "± 577763",
            "unit": "ns/iter"
          },
          {
            "name": "workflow_run/parallel/5",
            "value": 12600754,
            "range": "± 315026",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}