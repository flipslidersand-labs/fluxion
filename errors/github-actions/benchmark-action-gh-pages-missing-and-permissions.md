---
title: "github-action-benchmark の auto-push が gh-pages 不在・権限不足で二重に失敗する"
tags: [github-actions, ci, benchmark]
severity: medium
date: "2026-09-07"
---

## 症状

`benchmark-action/github-action-benchmark@v1`（`auto-push: true`）の
`Store benchmark results` ステップが、bench 出力自体は正しく生成された後も
以下の順で2種類のエラーで失敗した。

1. `fatal: couldn't find remote ref gh-pages`
2. （1を解消後）`remote: Permission ... denied to github-actions[bot]`

## 原因

1. `auto-push: true` は履歴保存先として `gh-pages` ブランチ（既定の
   `benchmark-data-dir-path: dev/bench`）を前提にするが、当該リポには
   一度も `gh-pages` ブランチが作られていなかった。
2. リポジトリの `default_workflow_permissions` が `read` のため、
   `GITHUB_TOKEN` に `gh-pages` への push 権限が無かった。ワークフロー側で
   `permissions:` を明示していないと、この repo デフォルトがそのまま適用される。

## 解決策

- 空の orphan `gh-pages` ブランチを1回だけ作成して push する
  （`git checkout --orphan gh-pages && git rm -rf . && git commit --allow-empty ... && git push`）。
  以後は action が `dev/bench/data.js` 等を自動生成・更新する。
- 対象ジョブに `permissions: contents: write` を明示する。

```yaml
jobs:
  bench:
    permissions:
      contents: write
```

## 予防

- `auto-push: true` を使う benchmark-action を新規リポに導入する際は、
  「gh-pages ブランチの存在」と「ジョブの permissions」の両方を事前に
  チェックリスト化する。片方だけ直すと別の失敗に化けて分かりにくい。
