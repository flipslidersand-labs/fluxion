window.BENCHMARK_DATA = {
  "lastUpdate": 1788793759987,
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
      }
    ]
  }
}