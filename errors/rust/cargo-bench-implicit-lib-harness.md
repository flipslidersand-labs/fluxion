---
title: "cargo bench が [[bench]] harness=false より先に暗黙の lib ベンチハーネスを実行して失敗する"
tags: [rust, cargo, criterion, ci]
severity: high
date: "2026-09-07"
---

## 症状

`cargo bench -p fluxion-host -- --output-format bencher` が
`error: Unrecognized option: 'output-format'` で失敗し、bench-output.txt が
空のまま CI の `Store benchmark results`（github-action-benchmark）が
`No benchmark result was found` で落ち続けていた。

`[[bench]] name = "host_bench" harness = false` を正しく設定していても発生する。

## 原因

Cargo は lib ターゲットを持つクレートに対して、明示的な `[[bench]]` とは別に
暗黙のベンチハーネス（`benches src/lib.rs`）を自動生成する（`[lib] bench`
のデフォルトは `true`）。`cargo bench -p <crate>` は登録された全ベンチ
バイナリを順に実行し、この暗黙ハーネス（中身は空で `#[bench]` 関数も無い）が
Criterion 独自の `--output-format bencher` フラグを認識できず即座にエラー
終了する。cargo はデフォルトで fail-fast のため、後続の実 Criterion ベンチ
（`host_bench`）には到達しない。

さらに CI 側は `cargo bench ... | tee bench-output.txt` としており、
`set -o pipefail` が無いと `tee` 自体の終了コードで step が判定されるため、
この失敗が握りつぶされ green のまま bench-output.txt が空になっていた。

## 解決策

`Cargo.toml` の `[package]` の下に以下を追加し、暗黙ハーネスを無効化する。

```toml
[lib]
bench = false
```

CI ワークフローの `cargo bench | tee` には必ず `set -o pipefail` を先に置く。

## 予防

- lib + bench 両方を持つクレートを新規作成したら、まず
  `cargo bench -p <crate> --no-run` の出力に `Executable benches src/lib.rs`
  が並んでいないか確認する（並んでいれば `[lib] bench = false` が要る）。
- パイプで終了コードを見るシェルステップには常に `set -o pipefail` を付ける。
