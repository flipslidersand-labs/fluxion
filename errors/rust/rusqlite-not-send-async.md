# rusqlite Connection は !Send — 非同期ブロック内で使う方法

## 症状

```
error[E0277]: `RefCell<...>` cannot be shared between threads safely
```

`tokio::spawn(async move { store.xxx() })` 内で `RunStore` を使おうとすると発生。

## 原因

`rusqlite::Connection` が `!Send` のため、`.await` をまたぐ `async` ブロックに移動できない。

## 解決策

open → collect → drop のパターンで完結させる。

```rust
// NG: store を spawn 内に移動
tokio::spawn(async move { store.due_schedules() });

// OK: 同期スコープで完結させて結果だけを async に渡す
let due = {
    let store = RunStore::open()?;
    store.due_schedules()? // drop here
};
for d in due {
    // async work
}
```

## 適用場面

`fluxion-cli` の `fire_due_schedules` など、`RunStore` を tokio タスクと組み合わせる箇所すべて。
