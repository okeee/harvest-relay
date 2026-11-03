# harvest-relay

複数のセンサーの値を集めて、一定周期ごとに 1 リクエストにまとめて [SORACOM Harvest Data](https://users.soracom.io/ja-jp/docs/harvest/) へ送る、Python 製の小さな常駐プログラムです。

センサーごとの読み取り処理は別のコマンドとして作り、harvest-relay はそれを子プロセスとして起動・監視・集約・送信するだけです。センサーの種類を知らないので、コマンドを書き換えれば何でもつなげられます。

```
            ┌─ stream: 2jcie-bu-reader  ── 1 秒ごとに 1 行の JSON ─┐
harvest-relay┤                                                    ├→ 周期ごとに集約・マージ → POST http://uni.soracom.io
            └─ poll:   nature-remo-reader ── 周期ごとに実行 ─────┘        (SORACOM Air / Arc 経由で Harvest Data に保存)
```

対応しているセンサーのコマンド:

- [2jcie-bu-reader](https://github.com/okeee/2jcie-bu-reader) — OMRON 環境センサー 2JCIE-BU01 (BLE / USB)
- [nature-remo-reader](https://github.com/okeee/nature-remo-reader) — Nature Remo の温度・湿度・照度・人感

## 必要なもの

- Python 3.11 以上 (標準ライブラリのみで動きます)
- SORACOM Air の SIM、または SORACOM Arc で接続した端末
  - Unified Endpoint (`http://uni.soracom.io`) は SORACOM の回線内からのみ届きます
  - 端末が属するグループで Harvest Data を有効にしておきます

## センサーコマンドの作り方

| type | 動き方 | 出力 |
|---|---|---|
| `stream` | 常駐させる。終了したり `stale_sec` 秒出力がなかったりすると再起動する | 1 サンプルごとに JSON オブジェクトを 1 行 |
| `poll` | 送信周期ごとに実行する。`timeout` 秒で打ち切る | JSON オブジェクトを 1 つ |

- 標準出力にはデータだけを出し、ログは標準エラーに出してください (journald に残ります)
- stream の値は周期ごとにフィールド単位で集約します。方法は `mean` (既定) / `max` / `min` / `sum` / `last` から選べ、リストで複数指定すると 2 つ目以降は `<フィールド名>_<方法>` で送ります。数値でない値は `last` になります
- 1 つのセンサーが失敗しても、残りのセンサーの値は送ります

## セットアップ

```bash
sudo install -m 755 harvest-relay.py /usr/local/bin/harvest-relay
sudo cp harvest-relay.example.toml /etc/harvest-relay.toml
sudo editor /etc/harvest-relay.toml   # 使うセンサーコマンドを書く
```

まず送らずに動作を確かめます (周期を 15 秒にして 1 回だけ実行):

```bash
harvest-relay -c /etc/harvest-relay.toml --interval 15 --once --dry-run
```

問題なければ systemd に登録します。`User=` はセンサーにアクセスできるユーザーに書き換えてください。

```bash
sudo cp systemd/harvest-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now harvest-relay
journalctl -u harvest-relay -f
```

## 料金の目安

Harvest Data は 1 日 2000 回の書き込みまで基本料金に含まれます。センサーをいくつつないでも送信は周期ごとに 1 回なので、60 秒周期なら 1 日 1440 回です。最新の料金は [SORACOM Harvest Data の料金](https://soracom.jp/services/harvest/price/) を確認してください。

## ライセンス

MIT
