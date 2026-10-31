---
title: "IpAddr を直接 format! で port と連結すると IPv6 が不正な文字列になる"
tags: [rust, networking, ipv6]
severity: medium
date: "2026-09-07"
---

## 症状

`tests::resolve_concurrent_same_host` が GitHub Actions の ubuntu-latest
ランナーでのみ `task 0 returned invalid entry: ::1:12345` で失敗した。
開発環境（ローカル）では再現しなかった。

## 原因

```rust
.map(|ip| match port {
    Some(p) => format!("{ip}:{p}"),   // ip: IpAddr
    None => ip.to_string(),
})
```

`IpAddr` の `Display` 実装は IPv6 でも角括弧を付けない（`::1`）。これを
`:port` と単純結合すると `::1:12345` になり、コロン区切りが `SocketAddr`
としてパース不能な曖昧な文字列になる（`[::1]:12345` が正しい形式）。

"localhost" の DNS 解決結果は環境によって IPv4 (`127.0.0.1`) 優先か IPv6
(`::1`) 優先かが異なる。GitHub Actions の ubuntu-latest ランナーは `::1`
を優先するため CI でのみ顕在化し、IPv4 優先のローカル環境では
再現しなかった。

## 解決策

`IpAddr` + port を文字列化するときは `format!("{ip}:{port}")` ではなく
`std::net::SocketAddr::new(ip, port).to_string()` を使う。
`SocketAddr` の `Display` 実装は IPv6 を正しく角括弧で囲む。

```rust
use std::net::SocketAddr;
Some(p) => SocketAddr::new(*ip, p).to_string(),  // "[::1]:12345"
```

## 予防

- `IpAddr` を扱うコードで `format!("{ip}:{port}")` パターンを見たら要注意。
  必ず `SocketAddr::new(ip, port)` 経由にする。
- IPv6 関連のバグは IPv4-only なローカル開発環境では再現しないことが
  多い。「ローカルで通ったから大丈夫」を過信しない。CI 環境（特に
  GitHub Actions の ubuntu-latest）はデュアルスタックで `localhost` を
  IPv6 優先で解決することがある点を踏まえる。
