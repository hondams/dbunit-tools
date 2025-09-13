# dbunit Tool

## 機能

- dbunitのデータセットファイルの形式変換
- dbunitのデータセットファイルのマージ
- dbunitのデータセットファイルのサンプル出力
- Table定義情報の出力
- 設定変更
- データベースデータのExport
- データベースデータのImport

## データ形式

- 日付
    - yyyy-MM-dd HH:mm:ss
    - yyyy-MM-dd HH:mm:ss.SSS

## データ比較

- ファイルに出力すると、DBデータ型の情報がなくなるので、すべて、文字列、Unkown型として評価さる。


## 特殊考慮

- `\U+0000`
  - FlatXml、Xmlの場合、対応。
    - `&amp;#x0;`として、エクスポートし、インポートで、`\U+0000`に戻す。
  - Yamlの場合、対応。
    - Base64文字列になる。

- ストリーム出力
  - FlatXml、Xml、CSVの場合、対応。
  - Yaml、Excelの場合、未対応
    - 全データをメモリ上で処理する。
