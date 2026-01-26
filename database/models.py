"""
データモデル定義
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class WeatherData:
    """気象データモデル"""
    id: Optional[int]
    location_code: str        # 地点コード (prec_no-block_no)
    location_name: str        # 地点名
    date: str                 # 日付 (YYYY-MM-DD)
    avg_temp: Optional[float]          # 平均気温(℃)
    max_temp: Optional[float]          # 最高気温(℃)
    min_temp: Optional[float]          # 最低気温(℃)
    precipitation: Optional[float]     # 降水量(mm)
    max_wind_speed: Optional[float]    # 最大風速(m/s)
    sunshine_hours: Optional[float]    # 日照時間(h)
    avg_humidity: Optional[float]      # 平均湿度(%)


@dataclass
class TrafficData:
    """交通量データモデル"""
    id: Optional[int]
    location_code: str        # 観測地点コード
    location_name: str        # 観測地点名
    prefecture: str           # 都道府県
    road_name: str            # 路線名
    date: str                 # 観測日 (YYYY-MM-DD)
    time_period: str          # 時間帯 (例: 7-8, 8-9)
    vehicle_count_large: Optional[int]  # 大型車台数
    vehicle_count_small: Optional[int]  # 小型車台数
    total_count: Optional[int]          # 合計台数
    travel_speed: Optional[float]       # 旅行速度(km/h)