---
title: "clippy::print_literal — println!(\"{}\", \"LITERAL\") が警告"
tags: [rust, clippy, ci]
severity: low
date: "2026-07-16"
---

## 症状

CI の clippy ジョブが以下の警告で失敗する:

```
warning: literal with an empty format string
  --> crates/fluxion-cli/src/main.rs:152:74
   |
   |   println!("{:<28}  {:<20}  {}", "RUN ID", "WORKFLOW", "STATUS");
   |                                                          ^^^^^^^^
   = note: `#[warn(clippy::print_literal)]` on by default
```

## 原因

`println!("{}", "STATUS")` は文字列リテラルを format 引数として渡しているが、
フォーマット指定子が `{}` のみの場合リテラルを直接埋め込める。
Clippy はこれを "冗長なフォーマット" として警告する。

## 解決策

```rust
// Before
println!("{:<28}  {:<20}  {}", "RUN ID", "WORKFLOW", "STATUS");

// After
println!("{:<28}  {:<20}  STATUS", "RUN ID", "WORKFLOW");
```

`cargo clippy --fix --bin "fluxion"` でも自動修正可能。

## 予防

`println!` で末尾にリテラル文字列を `{}` で渡している箇所は、
直接埋め込みに変更する。位置指定フォーマット（`{:<N}` 等）がない場合は特に注意。
