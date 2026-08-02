---
title: "gh pr create --base master が失敗 — デフォルトブランチが main"
tags: [git, github, pr, branch]
severity: low
date: "2026-07-18"
---

## 症状

```
pull request create failed: GraphQL: No commits between main and fix/mcp-json-portability,
Base ref must be a branch (createPullRequest)
```

`gh pr create --base master` で PR 作成が失敗。

## 原因

fluxion リポジトリのデフォルトブランチは `main`（`master` ではない）。
`--base master` を指定したが `master` ブランチが存在しないため失敗した。

## 解決策

```bash
# デフォルトブランチ確認
git branch -a | grep -E "origin/(main|master)"
# または
gh repo view flipslidersand/fluxion --json defaultBranchRef --jq .defaultBranchRef.name

# 正しい base を指定
gh pr create --base main ...
```

## 予防

PR 作成前に `git branch -a` または `gh repo view --json defaultBranchRef` でデフォルトブランチを確認する。
Platform リポジトリは `master`、fluxion 等の個人プロジェクトは `main` が多い。
