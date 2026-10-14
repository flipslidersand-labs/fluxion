# Architecture Decision Records

## ADR 整備 / Decision records (2026-10-11)

この一覧はこの checkout の設計判断の入口です。既存 ADR の状態・決定日・本文は保持しています。新規の遡及記録は Draft とし、当時の承認や比較経緯を推定していません。

| 記録 | 状態（既存記録は原文の表記） | 整備内容 |
| --- | --- | --- |
| [ADR-001: ホスト実装言語に Rust を選ぶ](ADR-001-language-choice.md) | 状態: Accepted | 既存記録を保持 |
| [ADR-002: Wasm ランタイムに Wasmtime を選ぶ](ADR-002-wasm-runtime.md) | 状態: Accepted | 既存記録を保持 |
| [ADR-003: WASI 0.2 から始め、0.3 を後付けする](ADR-003-wasi-version.md) | 状態: Accepted | 既存記録を保持 |
| [ADR-004: 永続化に SQLite を選ぶ](ADR-004-persistence.md) | 状態: Accepted | 既存記録を保持 |
| [ADR-005: 分散実行 — リモートワーカーへのジョブディスパッチ](ADR-005-distributed-execution.md) | Status: Accepted | 既存記録を保持 |

### 今回の補完範囲と残る確認

既存の判断への入口を整備しました。NEEDS_CONFIRMATION: 各 ADR の採用範囲・実装状況が現在も一致するか、後続判断による置換があるか。実装の進展だけを根拠に既存の Accepted / Deprecated / Superseded を変更しません。

### 更新方法

判断を追加する場合は既存の命名規約を優先し、規約がない場合は `YYYYMMDD-topic.md` とします。背景、判断、根拠・代替案、影響、再検討条件、関連資料を記録し、この一覧にリンクを追加します。未承認は Draft、承認後は承認者と実際の決定日を記録して Accepted に更新します。変更で置換する場合は旧記録を残し、Superseded と後続 ADR のリンクを記録します。

### 既存記録の補足候補

下表は本文に明示的な記載が見つからなかった項目です。比較が行われなかったことや、設計が誤っていることを意味しません。NEEDS_CONFIRMATION: 当時の資料・履歴で確認できた内容だけを補足し、既存の承認状態を維持してください。

| 記録 | 記載を確認する項目 |
| --- | --- |
| [ADR-001: ホスト実装言語に Rust を選ぶ](ADR-001-language-choice.md) | 再検討条件、根拠へのリンク |
| [ADR-002: Wasm ランタイムに Wasmtime を選ぶ](ADR-002-wasm-runtime.md) | 比較・代替案、再検討条件、根拠へのリンク |
| [ADR-003: WASI 0.2 から始め、0.3 を後付けする](ADR-003-wasi-version.md) | 比較・代替案、再検討条件、根拠へのリンク |
| [ADR-004: 永続化に SQLite を選ぶ](ADR-004-persistence.md) | 再検討条件、根拠へのリンク |
| [ADR-005: 分散実行 — リモートワーカーへのジョブディスパッチ](ADR-005-distributed-execution.md) | 比較・代替案、再検討条件、根拠へのリンク |
