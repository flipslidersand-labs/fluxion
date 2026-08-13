---
title: "serde_yaml 0.9 — 外部タグ付き enum の struct/newtype variant が map から deserialize できない"
tags: [rust, serde, serde_yaml, enum]
severity: medium
date: "2026-08-12"
---

## 症状

```
Error("jobs.aggregate.reduce: invalid type: map, expected a YAML tag starting with '!'", line: 12, column: 7)
```

YAML:

```yaml
reduce:
  custom: /path/to/reducer.wasm
```

`#[derive(Deserialize)]` + `#[serde(rename_all = "snake_case")]` の enum で
newtype variant `Custom(String)` を持つ場合、map 形式の YAML から deserialize できない。

## 原因

`serde_yaml` 0.9 は外部タグ付き enum (デフォルト) の非ユニット variant を
YAML マッピングから deserialize する際に、YAML タグ (`!!custom`) を期待してしまう。
これは serde_yaml 0.9 の既知バグ（GitHub issue #363）。

## 解決策

カスタム `Deserialize` を実装し、`deserialize_any` で string/map 両形式を手動処理する:

```rust
impl<'de> serde::Deserialize<'de> for MyEnum {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        use serde::de::{self, MapAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = MyEnum;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "string or map")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "unit_a" => Ok(MyEnum::UnitA),
                    other => Err(E::unknown_variant(other, &["unit_a", "custom"])),
                }
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
                let key: String = map.next_key()?.ok_or_else(|| de::Error::custom("empty map"))?;
                match key.as_str() {
                    "custom" => {
                        let v: String = map.next_value()?;
                        while map.next_key::<de::IgnoredAny>()?.is_some() {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                        Ok(MyEnum::Custom(v))
                    }
                    other => Err(de::Error::unknown_variant(other, &["unit_a", "custom"])),
                }
            }
        }
        de.deserialize_any(V)
    }
}
```

## 予防

- serde_yaml を使う Rust プロジェクトで enum に非ユニット variant を追加する際は
  必ずカスタム Deserialize を検討する
- serde_yaml 0.9 → 0.10 へのアップグレードで修正されている可能性があるが、
  serde_yaml 0.10 は API が大幅に変わるため移行コストが高い
