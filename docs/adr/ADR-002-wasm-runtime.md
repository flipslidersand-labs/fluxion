# ADR-002: Wasm ランタイムに Wasmtime を選ぶ

- **日付**: 2026-06-19
- **状態**: Accepted

## 背景

Wasm コンポーネントを実行するホストランタイムとして、Wasmtime / Wasmer / wasm3 のいずれかを選ぶ必要があった。

## 決定

Wasmtime を使う。

## 理由

- Component Model（`wasm32-wasip2`）の参照実装であり、仕様への準拠度が最も高い
- Bytecode Alliance が開発・維持しており、WASI 0.2 / 0.3 への対応が最速
- Rust クレート (`wasmtime`) として提供されており、型安全な API が使える
- `WasiCtxBuilder` による Capability 制御（ファイルシステム・ネットワーク）が組み込み済み

## トレードオフ

- Wasmer に比べてプラグインシステムが複雑
- wasm3 ほど組み込み向けではない（Fluxion はデスクトップ/サーバーが主なので許容範囲）
- 起動時間はインタプリタ型ランタイムより遅いが、JIT コンパイルで実行速度は速い
