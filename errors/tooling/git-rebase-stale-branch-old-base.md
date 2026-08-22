---
title: "worktree ブランチが古い base を持ち rebase で不要コミットが混入"
tags: [git, worktree, rebase]
severity: medium
date: "2026-08-22"
---

## 症状

`git rebase origin/main` を実行したら 7 コミットが対象になり、
Cargo.lock / runner.rs / ui.rs でコンフリクトが発生。
本来の変更は ci.yml 1行のみのはずだった。

## 原因

worktree ブランチを古い main の状態（数十コミット前）から切ったため、
その間に main に入ったコミットが rebase 対象として列挙された。
一部は既に upstream にあり `dropping` されたが、残りがコンフリクト。

## 解決策

```bash
git rebase --abort
git reset --hard origin/main   # 最新 main に合わせる
# 変更を再適用
git add ... && git commit ...
git push --force-with-lease
```

## 予防

- 新しい fix ブランチを切る前に必ず `git fetch origin main` して最新を確認する
- または `git checkout -b fix/xxx origin/main` で最新から切る（今回はこれが正解）
- worktree を長期間放置すると base がどんどん古くなる
