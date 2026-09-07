---
title: "テスト用モックHTTPサーバーがボディ無しリクエスト(HEAD等)でデッドロックする"
tags: [rust, tokio, testing, http]
severity: high
date: "2026-09-07"
---

## 症状

`scheduler::tests::fails_over_to_healthy_worker` が原因不明のまま最大6時間
ハングし、GitHub Actions のデフォルト上限で強制キャンセルされていた
（Issue #220）。`tokio::time::timeout` で包んでも 20 秒待っても完了しない、
真の（タイミング依存ではない）デッドロックだった。

## 原因

`run_remote()` は本体の POST の前に `HEAD /components/{sha256}` で CAS
（コンテンツアドレス可能ストレージ）チェックを行うよう変更されていたが、
テスト用の `spawn_mock_worker()` はリクエストを以下のロジックで
ドレインしていた:

```rust
while let Ok(n) = sock.read(&mut tmp).await {
    if n == 0 { break; }               // クライアントが接続を閉じた
    // ...header/Content-Length を解析...
    if let (Some(he), Some(cl)) = (header_end, content_len)
        && buf.len() >= he + cl
    { break; }                          // Content-Length 分読み終えた
}
```

`HEAD` リクエストにはボディが無く `Content-Length` ヘッダも無いため、
`content_len` が `None` のまま残り、2つの break 条件のどちらも成立しない。
一方クライアント（reqwest）は `HEAD` 送信後、同じコネクション上で
サーバーからの応答を待っている。サーバーはこれ以上来ないボディを
待って `read()` をブロックし続け、クライアントは来ない応答を待つ —
双方が相手を待つ真のデッドロック。

## 解決策

ヘッダーを解析した時点で `Content-Length` が無ければ「ボディ無し
リクエスト」とみなし、即座にドレインを打ち切る。

```rust
if header_end.is_none() && let Some(p) = find(&buf, b"\r\n\r\n") {
    header_end = Some(p + 4);
    // ...content_len 解析...
    if content_len.is_none() {
        break;  // GET/HEAD 等ボディ無しリクエストとして扱う
    }
}
```

併せて呼び出し側テストにも `tokio::time::timeout(Duration::from_secs(20), ...)`
を被せ、将来同種の問題が再発しても静かに無期限ハングするのではなく
明確に失敗するようにした（timeout は必要条件であって十分条件ではない —
今回は timeout を追加しても根本原因のデッドロックは直らなかった。
必ず真因を特定すること）。

## 予防

- テスト用の簡易 HTTP モックサーバーを自作する場合、`Content-Length` が
  無いリクエスト（GET/HEAD/一部の DELETE）を必ず考慮する。
  「ヘッダーの終端が見えたら、Content-Length が無い限りボディは無い」が
  安全なデフォルト。
- 本番コードが新しいエンドポイント呼び出し（今回は HEAD による CAS
  チェック）を追加したときは、それを使うテストのモックサーバーも
  追従が必要。モック側だけ古いままだと通常のタイムアウトでは検出
  できない真のデッドロックになりうる。
- `#[tokio::test]` の非同期処理に `tokio::time::timeout` を被せるのは
  良い防御策だが、それ自体はデッドロックの原因を教えてくれない
  （`Elapsed(())` としか出ない）。原因調査には該当テストだけを単独で
  ローカル実行し、ハング中のプロセスの子プロセスを `ps --forest` 等で
  観察するのが有効。
