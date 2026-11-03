#!/usr/bin/env python3
"""センサーの値を集め、一定周期ごとに 1 リクエストにまとめて SORACOM Harvest Data へ送る。

センサーは設定ファイル (TOML) に書いたコマンドとして子プロセスで動かす。
  stream: 常駐し、標準出力に 1 サンプル 1 行の JSON を出し続ける。周期ごとにフィールド単位で集約する。
  poll:   周期ごとに実行し、標準出力の JSON オブジェクト 1 つをそのまま送信内容に加える。
送信先は SORACOM Unified Endpoint (http://uni.soracom.io) を想定しているが、JSON を POST できる先なら何でもよい。
"""
import argparse
import asyncio
import json
import logging
import signal
import statistics
import sys
import time
import tomllib
import urllib.error
import urllib.request

DEFAULT_ENDPOINT = "http://uni.soracom.io"
AGGREGATORS = {
    "mean": lambda v: round(statistics.fmean(v), 3),
    "max": max,
    "min": min,
    "sum": sum,
    "last": lambda v: v[-1],
}

log = logging.getLogger("harvest-relay")


class StreamSource:
    """常駐するセンサーコマンド。出力行を貯め、周期ごとに集約する。止まったら再起動する。"""

    def __init__(self, conf: dict):
        self.name = conf["name"]
        self.command = conf["command"]
        self.restart_delay = conf.get("restart_delay", 10)
        self.stale_sec = conf.get("stale_sec", 300)
        self.count_field = conf.get("count_field")
        agg = dict(conf.get("aggregate", {}))
        self.default_agg = self._as_list(agg.pop("default", "mean"))
        self.field_agg = {k: self._as_list(v) for k, v in agg.items()}
        for methods in [self.default_agg, *self.field_agg.values()]:
            unknown = set(methods) - AGGREGATORS.keys()
            if unknown:
                raise ValueError(f"{self.name}: unknown aggregate {unknown}")
        self.samples = []
        self.last_rx = time.monotonic()

    @staticmethod
    def _as_list(v):
        return [v] if isinstance(v, str) else list(v)

    async def run(self):
        while True:
            proc = await asyncio.create_subprocess_exec(*self.command, stdout=asyncio.subprocess.PIPE)
            log.info("%s: started (pid %s)", self.name, proc.pid)
            self.last_rx = time.monotonic()
            try:
                while True:
                    remaining = self.stale_sec - (time.monotonic() - self.last_rx)
                    try:
                        line = await asyncio.wait_for(proc.stdout.readline(), timeout=max(remaining, 1))
                    except asyncio.TimeoutError:
                        log.warning("%s: no output for %ss, restarting", self.name, self.stale_sec)
                        break
                    if not line:
                        log.warning("%s: exited (code %s)", self.name, await proc.wait())
                        break
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        log.warning("%s: invalid JSON line: %r", self.name, line[:200])
                        continue
                    if isinstance(record, dict):
                        self.samples.append(record)
                        self.last_rx = time.monotonic()
            finally:
                if proc.returncode is None:
                    proc.terminate()
                    try:
                        await asyncio.wait_for(proc.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        proc.kill()
            await asyncio.sleep(self.restart_delay)

    def flush(self) -> dict:
        samples, self.samples = self.samples, []
        values = {}
        for record in samples:
            for k, v in record.items():
                values.setdefault(k, []).append(v)
        out = {}
        for k, vs in values.items():
            methods = self.field_agg.get(k, self.default_agg)
            if not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vs):
                methods = ["last"]  # 数値以外は最後の値をそのまま送る
            for i, m in enumerate(methods):
                out[k if i == 0 else f"{k}_{m}"] = AGGREGATORS[m](vs)
        if self.count_field and samples:
            out[self.count_field] = len(samples)
        return out


class PollSource:
    """周期ごとに実行するセンサーコマンド。"""

    def __init__(self, conf: dict):
        self.name = conf["name"]
        self.command = conf["command"]
        self.timeout = conf.get("timeout", 15)

    async def collect(self) -> dict:
        proc = await asyncio.create_subprocess_exec(*self.command, stdout=asyncio.subprocess.PIPE)
        try:
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
            raise IOError(f"timed out after {self.timeout}s")
        if proc.returncode != 0:
            raise IOError(f"exited with code {proc.returncode}")
        record = json.loads(stdout)
        if not isinstance(record, dict):
            raise ValueError("output is not a JSON object")
        return record


def send(endpoint: str, payload: dict):
    req = urllib.request.Request(endpoint, data=json.dumps(payload).encode(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            log.info("sent: %s %s", r.status, json.dumps(payload, ensure_ascii=False))
    except (urllib.error.URLError, TimeoutError) as e:
        log.warning("send failed: %s", e)


async def collect_poll(source: PollSource) -> dict:
    try:
        return await source.collect()
    except Exception as e:  # 1 つのセンサーが失敗しても残りは送る
        log.warning("%s: %s", source.name, e)
        return {}


async def relay(conf: dict, args):
    interval = args.interval or conf.get("interval", 60)
    endpoint = conf.get("endpoint", DEFAULT_ENDPOINT)
    streams, polls = [], []
    for s in conf.get("source", []):
        kind = s.get("type")
        if kind == "stream":
            streams.append(StreamSource(s))
        elif kind == "poll":
            polls.append(PollSource(s))
        else:
            raise ValueError(f"{s.get('name')}: unknown source type {kind!r}")
    if not streams and not polls:
        raise ValueError("no source configured")

    tasks = [asyncio.create_task(s.run()) for s in streams]
    next_at = time.monotonic()
    try:
        while True:
            # poll の実行時間で周期がずれていかないよう、開始時刻からの一定間隔で送る
            next_at += interval
            await asyncio.sleep(max(next_at - time.monotonic(), 0))
            payload = {}
            for s in streams:
                payload.update(s.flush())
            for record in await asyncio.gather(*(collect_poll(p) for p in polls)):
                payload.update(record)
            if not payload:
                log.warning("no data in the last %ss", interval)
            elif args.dry_run:
                print(json.dumps(payload, ensure_ascii=False), flush=True)
            else:
                send(endpoint, payload)
            if args.once:
                return
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("-c", "--config", default="/etc/harvest-relay.toml")
    p.add_argument("--interval", type=int, help="送信周期 (秒)。設定ファイルの interval を上書きする")
    p.add_argument("--once", action="store_true", help="1 周期分だけ集めて終了する")
    p.add_argument("--dry-run", action="store_true", help="送信せず標準出力に表示する")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    with open(args.config, "rb") as f:
        conf = tomllib.load(f)
    async def run():
        task = asyncio.create_task(relay(conf, args))
        # systemctl stop でも子プロセスを止めてから終わる
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            log.info("stopped")

    try:
        asyncio.run(run())
    except (ValueError, KeyError) as e:
        log.error("config error: %s", e)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
