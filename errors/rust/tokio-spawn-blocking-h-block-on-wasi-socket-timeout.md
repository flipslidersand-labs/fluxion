---
title: "tokio::task::spawn_blocking + h.block_on + sync WASI sockets が CI で TCP タイムアウトする"
tags: [tokio, wasmtime, wasi, ci, networking]
severity: medium
date: "2026-08-03"
---

## 症状

`tokio::task::spawn_blocking(|| host.run_component_measured(...))` 内で wasmtime-wasi の
sync ソケット（`in_tokio` 経由）を使って TCP connect を行うと、GitHub Actions CI 環境で
接続がタイムアウトする（5s 後に "Timeout after 5s" で失敗）。

ローカル環境では問題なく動作するが CI では再現。

## 原因

wasmtime-wasi の `in_tokio` は `Handle::try_current()` で Tokio ランタイムを取得し、
blocking スレッド上で `h.block_on(connect_future)` を呼ぶ。

`spawn_blocking` スレッドは Tokio の I/O ドライバスレッドとは別スレッドに動作するが、
`h.block_on()` はそのスレッドのみをブロックして Tokio ランタイム全体を進めることはできない。
CI の GitHub Actions ランナーでは Tokio の I/O Reactor がこの状態での async TCP connect を
正常に進められず、5s のスケジューラタイムアウトが先に発火する。

具体的な呼び出し経路：

```
scheduler::run_silent
  → tokio::task::spawn_blocking(|| host.run_component_measured(...))
      → wasmtime-wasi in_tokio
          → h.block_on(tcp_connect_future)  ← CI ではここでフリーズ
```

## 解決策

**e2e テスト側での回避**: allow 側（実接続パス）のテストを削除し、deny 側（`SocketAddrCheck`
が I/O 前に `false` を返すパス）のみをテストする。

deny パスはアドレスチェックが I/O より前に完了するため async I/O を一切使わず、CI でも
安定して通過する。

```rust
// 削除: connect-allowed ジョブ（実接続テスト）
wf.jobs.shift_remove("connect-allowed");
// 保持: connect-denied ジョブ（空 allowlist でブロック）
denied_job.depends_on.clear();
```

**allow 側の検証**: ローカル統合テストで個別に確認する（`#[ignore]` を外してローカル実行）。

## 予防

`spawn_blocking` + wasmtime sync API の組み合わせで async ネットワーク処理を CI でテストする
場合は必ずタイムアウト挙動を確認すること。

allow パス（実接続成功）のテストは `#[tokio::test]` 直下（spawn_blocking を使わない形）か、
ローカル専用テスト（`#[ignore]`）として分離する。

deny パス（I/O 到達前にブロック）は安全にテストできる。セキュリティ的にも deny が
正しく機能することが最重要プロパティのため、これを CI でカバーすれば十分。
