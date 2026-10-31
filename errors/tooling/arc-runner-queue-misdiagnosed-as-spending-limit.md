---
title: "ARC runner queue 詰まりを spending limit 超過と誤診断"
tags: [github-actions, arc-runner, billing]
severity: medium
date: "2026-08-22"
---

## 症状

e2e ジョブが `queued` / `pending` のまま進まない。
spending limit が原因と思い ubuntu-latest への変更・limit 引き上げを提案してしまった。

## 原因

実際は spending limit に余裕があった（$2.04/$3.00）。
ARC scale set `arc-fluxion` の `maxRunners` が低く、複数 PR 同時実行時に pool が枯渇していた。

## 解決策

1. `gh run list` でジョブが `queued` になっている理由を切り分ける
2. GitHub Billing → Budgets で残量を確認してから判断する
3. runner 問題なら ARC scale set の `maxRunners` を上げるか、`GATE_RUNNER` (org スコープ) に統一する

今回は全ジョブを `${{ vars.GATE_RUNNER }}` 専用に統一（ubuntu-latest フォールバック廃止）で対応。

## 予防

「jobs が queued で止まる」= spending limit と即断しない。
まず billing 画面を見てから runner pool 側を疑う。
