---
title: "wasmtime v28+ で wasmtime::VERSION 定数が存在しない"
tags: [rust, wasmtime, cache]
severity: medium
date: "2026-08-02"
---

## 症状

```rust
use wasmtime::VERSION; // compile error: unresolved import
```

または

```rust
let key = format!("{}-{}", wasmtime::VERSION, sha256_hex);
// error[E0433]: failed to resolve: use of undeclared type `VERSION`
```

## 原因

wasmtime v28 以降、`wasmtime::VERSION` 定数は公開されていない。

## 解決策

cwasm アーティファクトの stale 検出は `Component::deserialize_file` の `Err` 戻り値を使う:

```rust
match unsafe { Component::deserialize_file(engine, &path) } {
    Ok(c) => Some(c),
    Err(_) => {
        // stale or wrong version — evict and treat as cache miss
        std::fs::remove_file(&path).ok();
        None
    }
}
```

wasmtime はバージョンが違う .cwasm をロードしようとすると `Err` を返す（UB は発生しない）。

## 注記

`engine.precompile_component()` で生成した .cwasm はバージョンマジックを含むため、
同一バージョンの engine でないと `deserialize_file` が `Err` を返す。
これを利用して「バージョンミスマッチ = stale」として evict する方式が安全。
