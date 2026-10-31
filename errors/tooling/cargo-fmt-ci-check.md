---
title: "cargo fmt --check が CI で失敗 — ローカルの rustfmt バージョン差異"
tags: [rust, cargo, fmt, ci]
severity: low
date: "2026-08-02"
---

## 症状

CI の fmt ジョブが以下のような diff で失敗する:

```
Diff in crates/fluxion-host/src/lib.rs:139:
-                self.mem_cache.write().unwrap().insert(key.clone(), Arc::clone(&c));
+                self.mem_cache
+                    .write()
+                    .unwrap()
+                    .insert(key.clone(), Arc::clone(&c));
```

ローカルでは `cargo fmt` を実行しても問題なく見えていた。

## 原因

ローカルと CI の rustfmt バージョンが異なる場合、行長に関するフォーマット判定が変わる。
CI は GitHub Actions の最新 stable toolchain を使うため、ローカルより新しい場合がある。

## 解決策

コミット前に CI と同じ条件で確認する:

```bash
cargo fmt --all           # まず適用
cargo fmt --all -- --check  # 差分がないことを確認してからコミット
```

## 予防

コミット前に `cargo fmt --all -- --check` を実行する習慣をつける。
husky などの pre-commit hook に追加するとよい。
