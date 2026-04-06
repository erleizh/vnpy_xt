from datetime import datetime, timedelta, time
from collections.abc import Callable

from pandas import DataFrame
from xtquant import (
    xtdata,
    xtdatacenter as xtdc
)
from filelock import FileLock, Timeout

from vnpy.trader.setting import SETTINGS
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import ZoneInfo, get_file_path
from vnpy.trader.datafeed import BaseDatafeed

from .xt_config import VIP_ADDRESS_LIST, LISTEN_PORT


INTERVAL_VT2XT: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.DAILY: "1d",
    Interval.TICK: "tick"
}

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.DAILY: timedelta()         # 日线无需进行调整
}

EXCHANGE_VT2XT: dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.BSE: "BJ",
    Exchange.SHFE: "SF",
    Exchange.CFFEX: "IF",
    Exchange.INE: "INE",
    Exchange.DCE: "DF",
    Exchange.CZCE: "ZF",
    Exchange.GFEX: "GF",
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")


def _epoch_to_cn_naive(ts: float) -> datetime:
    """将 epoch 秒时间转换为中国时区的 naive datetime。"""
    return datetime.fromtimestamp(ts, tz=ZoneInfo("UTC")).astimezone(CHINA_TZ).replace(tzinfo=None)


def _parse_xt_datetime(value) -> datetime:
    """
    解析 xtquant ``get_local_data`` 返回中的时间，与当前实测格式对齐：

    - K 线（1m/1d、``field_list`` 为空或仅 OHLCV）：无 ``time`` 列，**int64 索引** 为
      ``YYYYMMDD``（日线）或 ``YYYYMMDDHHMMSS``（分钟线）。
    - Tick：有 ``time`` 列（索引名常为 ``stime``）；数值多为 **毫秒级 Unix 时间**（约 13 位）。
    """
    to_py = getattr(value, "to_pydatetime", None)
    if callable(to_py):
        dt = to_py()
        return dt.replace(tzinfo=None) if dt.tzinfo else dt

    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value

    try:
        import numpy as np
        if isinstance(value, np.generic):       # int64 / float64 等
            value = value.item()
    except ImportError:
        pass

    if isinstance(value, bool):
        raise TypeError(f"unexpected bool in xt time field: {value}")

    if isinstance(value, (int, float)):
        vi = int(value)
        s = str(abs(vi))
        if len(s) == 8:
            return datetime.strptime(s, "%Y%m%d")
        if len(s) == 14:
            return datetime.strptime(s, "%Y%m%d%H%M%S")
        ts = float(vi)
        if abs(ts) >= 1_000_000_000_000:
            return _epoch_to_cn_naive(ts / 1000.0)
        return _epoch_to_cn_naive(ts)

    raise TypeError(f"unsupported xt time type: {type(value).__name__} repr={value!r}")


def _normalize_history_df(df: DataFrame) -> DataFrame:
    """统一时间来源：Tick 用 ``time`` 列；K 线用索引。返回以 naive datetime 为索引的 DataFrame。"""
    if df.empty:
        return df

    normalized: DataFrame = df.copy()
    if "time" in normalized.columns:
        normalized.index = [_parse_xt_datetime(v) for v in normalized["time"].tolist()]
        normalized.drop(columns=["time"], inplace=True)
    else:
        normalized.index = [_parse_xt_datetime(v) for v in normalized.index.tolist()]

    normalized.index.name = "stime"
    return normalized


def _xt_row_to_naive_dt(tp) -> datetime:
    """读取 ``_normalize_history_df`` 之后 itertuples 行的索引时间（已为 datetime）。"""
    idx = getattr(tp, "Index", None)
    to_py = getattr(idx, "to_pydatetime", None)
    if callable(to_py):
        dt = to_py()
        return dt.replace(tzinfo=None) if dt.tzinfo else dt
    if isinstance(idx, datetime):
        return idx.replace(tzinfo=None) if idx.tzinfo else idx
    return _parse_xt_datetime(idx)


def _safe_level_value(values, index: int) -> float:
    """安全读取盘口档位，缺失时返回0。"""
    if values is None:
        return 0.0
    if index < 0 or len(values) <= index:
        return 0.0
    return float(values[index])


class XtDatafeed(BaseDatafeed):
    """迅投研数据服务接口"""

    lock_filename = "xt_lock"
    lock_filepath = get_file_path(lock_filename)

    def __init__(self) -> None:
        """"""
        self.username: str = SETTINGS["datafeed.username"]
        self.password: str = SETTINGS["datafeed.password"]
        self.inited: bool = False

        self.lock: FileLock | None = None

        xtdata.enable_hello = False

    def init(self, output: Callable = print) -> bool:
        """初始化"""
        if self.inited:
            return True

        try:
            # 使用Token连接，无需启动客户端
            # if self.username != "client":
                # self.init_xtdc()

            # 尝试查询合约信息，确认连接成功
            xtdata.get_instrument_detail("000001.SZ")
        except Exception as ex:
            output(f"迅投研数据服务初始化失败，发生异常：{ex}")
            return False

        self.inited = True
        return True

    def get_lock(self) -> bool:
        """获取文件锁，确保单例运行"""
        self.lock = FileLock(self.lock_filepath)

        try:
            self.lock.acquire(timeout=1)
            return True
        except Timeout:
            return False

    def init_xtdc(self) -> None:
        """初始化xtdc服务进程"""
        if not self.get_lock():
            return

        # 设置token
        xtdc.set_token(self.password)

        # 设置连接池
        xtdc.set_allow_optmize_address(VIP_ADDRESS_LIST)

        # 开启使用期货真实夜盘时间
        xtdc.set_future_realtime_mode(True)

        # 执行初始化，但不启动默认58609端口监听
        xtdc.init(False)

        # 设置监听端口
        xtdc.listen(port=LISTEN_PORT)

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> list[BarData] | None:
        """查询K线数据"""
        history: list[BarData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[req.interval]

        # 遍历解析
        auction_bar: BarData = None

        for tp in df.itertuples():
            # 将迅投研时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
            dt: datetime = _xt_row_to_naive_dt(tp)
            dt = dt.replace(tzinfo=CHINA_TZ)
            dt = dt - adjustment

            # 日线，过滤尚未走完的当日数据
            if req.interval == Interval.DAILY:
                now_cn: datetime = datetime.now(CHINA_TZ)
                incomplete_bar: bool = (
                    dt.date() == now_cn.date()
                    and now_cn.time() < time(hour=15)
                )
                if incomplete_bar:
                    continue
            # 分钟线，过滤盘前集合竞价数据（合并到开盘后第1根K线中）
            else:
                if (
                    req.exchange in (Exchange.SSE, Exchange.SZSE, Exchange.BSE, Exchange.CFFEX)
                    and dt.time() == time(hour=9, minute=29)
                ) or (
                    req.exchange in (Exchange.SHFE, Exchange.INE, Exchange.DCE, Exchange.CZCE, Exchange.GFEX)
                    and dt.time() in (time(hour=8, minute=59), time(hour=20, minute=59))
                ):
                    auction_bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        open_price=float(tp.open),
                        volume=float(tp.volume),
                        turnover=float(tp.amount),
                        gateway_name="XT"
                    )
                    continue

            # 生成K线对象
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                interval=req.interval,
                volume=float(tp.volume),
                turnover=float(tp.amount),
                open_interest=float(tp.openInterest),
                open_price=float(tp.open),
                high_price=float(tp.high),
                low_price=float(tp.low),
                close_price=float(tp.close),
                gateway_name="XT"
            )

            # 合并集合竞价数据
            if auction_bar and auction_bar.volume:
                bar.open_price = auction_bar.open_price
                bar.high_price = max(bar.high_price, auction_bar.open_price)
                bar.low_price = min(bar.low_price, auction_bar.open_price)
                bar.volume += auction_bar.volume
                bar.turnover += auction_bar.turnover
                auction_bar = None

            history.append(bar)

        return history

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> list[TickData] | None:
        """查询Tick数据"""
        history: list[TickData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        # 遍历解析
        for tp in df.itertuples():
            dt: datetime = _xt_row_to_naive_dt(tp)
            dt = dt.replace(tzinfo=CHINA_TZ)

            bidPrice: list[float] = tp.bidPrice
            askPrice: list[float] = tp.askPrice
            bidVol: list[float] = tp.bidVol
            askVol: list[float] = tp.askVol

            bid_price_1: float = _safe_level_value(bidPrice, 0)
            ask_price_1: float = _safe_level_value(askPrice, 0)
            bid_volume_1: float = _safe_level_value(bidVol, 0)
            ask_volume_1: float = _safe_level_value(askVol, 0)

            tick: TickData = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                volume=float(tp.volume),
                turnover=float(tp.amount),
                open_interest=float(tp.openInt),
                open_price=float(tp.open),
                high_price=float(tp.high),
                low_price=float(tp.low),
                last_price=float(tp.lastPrice),
                pre_close=float(tp.lastClose),
                bid_price_1=bid_price_1,
                ask_price_1=ask_price_1,
                bid_volume_1=bid_volume_1,
                ask_volume_1=ask_volume_1,
                gateway_name="XT",
            )

            bid_price_2: float = _safe_level_value(bidPrice, 1)
            if bid_price_2:
                tick.bid_price_2 = bid_price_2
                tick.bid_price_3 = _safe_level_value(bidPrice, 2)
                tick.bid_price_4 = _safe_level_value(bidPrice, 3)
                tick.bid_price_5 = _safe_level_value(bidPrice, 4)

                tick.ask_price_2 = _safe_level_value(askPrice, 1)
                tick.ask_price_3 = _safe_level_value(askPrice, 2)
                tick.ask_price_4 = _safe_level_value(askPrice, 3)
                tick.ask_price_5 = _safe_level_value(askPrice, 4)

                tick.bid_volume_2 = _safe_level_value(bidVol, 1)
                tick.bid_volume_3 = _safe_level_value(bidVol, 2)
                tick.bid_volume_4 = _safe_level_value(bidVol, 3)
                tick.bid_volume_5 = _safe_level_value(bidVol, 4)

                tick.ask_volume_2 = _safe_level_value(askVol, 1)
                tick.ask_volume_3 = _safe_level_value(askVol, 2)
                tick.ask_volume_4 = _safe_level_value(askVol, 3)
                tick.ask_volume_5 = _safe_level_value(askVol, 4)

            history.append(tick)

        return history


def get_history_df(req: HistoryRequest, output: Callable = print) -> DataFrame:
    """获取历史数据DataFrame"""
    symbol: str = req.symbol
    exchange: Exchange = req.exchange
    start_dt: datetime = req.start
    end_dt: datetime = req.end
    interval: Interval = req.interval

    if not interval:
        interval = Interval.TICK

    xt_interval: str | None = INTERVAL_VT2XT.get(interval, None)
    if not xt_interval:
        output(f"迅投研查询历史数据失败：不支持的时间周期{interval.value}")
        return DataFrame()

    # 为了查询夜盘数据
    end_dt += timedelta(1)

    # 从服务器下载获取
    xt_symbol: str = symbol + "." + EXCHANGE_VT2XT[exchange]
    start: str = start_dt.strftime("%Y%m%d%H%M%S")
    end: str = end_dt.strftime("%Y%m%d%H%M%S")

    if exchange in (Exchange.SSE, Exchange.SZSE) and len(symbol) > 6:
        xt_symbol += "O"

    xtdata.download_history_data(xt_symbol, xt_interval, start, end)
    data: dict = xtdata.get_local_data([], [xt_symbol], xt_interval, start, end, -1, "front_ratio", False)      # 默认等比前复权

    df: DataFrame | None = data.get(xt_symbol)
    if df is None:
        output(f"迅投研查询历史数据为空：{xt_symbol} {xt_interval} {start}~{end}（symbol不存在于返回字典）")
        return DataFrame()
    if df.empty:
        output(f"迅投研查询历史数据为空：{xt_symbol} {xt_interval} {start}~{end}")
        return DataFrame()
    return _normalize_history_df(df)
