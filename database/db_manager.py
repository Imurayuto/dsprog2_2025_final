"""
データベース管理クラス
気象データと交通量データを管理
"""
import sqlite3
import pandas as pd
from typing import List, Dict, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class WeatherTrafficDatabase:
    """
    気象データと交通量データを管理するデータベースクラス
    """
    
    def __init__(self, db_path: str = 'data/weather_traffic.db'):
        """
        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = db_path
        self.conn = None
        self.init_database()
        logger.info(f"Database initialized at {db_path}")
    
    def init_database(self):
        """データベースとテーブルを作成"""
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        
        # 気象データテーブル
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_code TEXT NOT NULL,
            location_name TEXT NOT NULL,
            date TEXT NOT NULL,
            avg_temp REAL,
            max_temp REAL,
            min_temp REAL,
            precipitation REAL,
            max_wind_speed REAL,
            sunshine_hours REAL,
            avg_humidity REAL,
            UNIQUE(location_code, date)
        )
        ''')
        
        # 交通量データテーブル
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS traffic (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_code TEXT NOT NULL,
            location_name TEXT NOT NULL,
            prefecture TEXT NOT NULL,
            road_name TEXT NOT NULL,
            date TEXT NOT NULL,
            time_period TEXT NOT NULL,
            vehicle_count_large INTEGER,
            vehicle_count_small INTEGER,
            total_count INTEGER,
            travel_speed REAL,
            UNIQUE(location_code, date, time_period)
        )
        ''')
        
        # インデックス作成
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_weather_date 
        ON weather(date)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_weather_location 
        ON weather(location_name)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_traffic_date 
        ON traffic(date)
        ''')
        
        cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_traffic_location 
        ON traffic(location_name)
        ''')
        
        self.conn.commit()
    
    # ========== 気象データ関連メソッド ==========
    
    def insert_weather_data(self, data_list: List[Dict]) -> int:
        """
        気象データを挿入
        
        Args:
            data_list: 気象データのリスト
        
        Returns:
            挿入された行数
        """
        if not data_list:
            return 0
        
        cursor = self.conn.cursor()
        inserted = 0
        
        for data in data_list:
            try:
                cursor.execute('''
                INSERT OR REPLACE INTO weather 
                (location_code, location_name, date, avg_temp, max_temp, 
                 min_temp, precipitation, max_wind_speed, sunshine_hours, avg_humidity)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['location_code'],
                    data['location_name'],
                    data['date'],
                    data.get('avg_temp'),
                    data.get('max_temp'),
                    data.get('min_temp'),
                    data.get('precipitation'),
                    data.get('max_wind_speed'),
                    data.get('sunshine_hours'),
                    data.get('avg_humidity')
                ))
                inserted += 1
            except sqlite3.Error as e:
                logger.error(f"Insert error: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Inserted {inserted} weather records")
        return inserted
    
    def query_weather_by_date_range(self, 
                                   start_date: str, 
                                   end_date: str,
                                   location_name: Optional[str] = None) -> pd.DataFrame:
        """
        日付範囲で気象データを検索
        
        Args:
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            location_name: 地点名（Noneの場合は全地点）
        
        Returns:
            DataFrame
        """
        if location_name:
            query = '''
            SELECT * FROM weather
            WHERE date BETWEEN ? AND ?
            AND location_name = ?
            ORDER BY date
            '''
            params = (start_date, end_date, location_name)
        else:
            query = '''
            SELECT * FROM weather
            WHERE date BETWEEN ? AND ?
            ORDER BY date
            '''
            params = (start_date, end_date)
        
        return pd.read_sql_query(query, self.conn, params=params)
    
    def query_weather_by_condition(self, 
                                   condition: str,
                                   threshold: float,
                                   operator: str = '>=') -> pd.DataFrame:
        """
        条件を指定して気象データを検索
        
        Args:
            condition: 条件列名 ('precipitation', 'avg_temp'など)
            threshold: 閾値
            operator: 比較演算子 ('>=', '<=', '=', '>', '<')
        
        Returns:
            DataFrame
        """
        valid_operators = ['>=', '<=', '=', '>', '<']
        if operator not in valid_operators:
            raise ValueError(f"Invalid operator. Use one of {valid_operators}")
        
        query = f'''
        SELECT * FROM weather
        WHERE {condition} {operator} ?
        ORDER BY date
        '''
        
        return pd.read_sql_query(query, self.conn, params=(threshold,))
    
    # ========== 交通量データ関連メソッド ==========
    
    def insert_traffic_data(self, data_list: List[Dict]) -> int:
        """
        交通量データを挿入
        
        Args:
            data_list: 交通量データのリスト
        
        Returns:
            挿入された行数
        """
        if not data_list:
            return 0
        
        cursor = self.conn.cursor()
        inserted = 0
        
        for data in data_list:
            try:
                cursor.execute('''
                INSERT OR REPLACE INTO traffic 
                (location_code, location_name, prefecture, road_name, date, 
                 time_period, vehicle_count_large, vehicle_count_small, 
                 total_count, travel_speed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    data['location_code'],
                    data['location_name'],
                    data['prefecture'],
                    data['road_name'],
                    data['date'],
                    data['time_period'],
                    data.get('vehicle_count_large'),
                    data.get('vehicle_count_small'),
                    data.get('total_count'),
                    data.get('travel_speed')
                ))
                inserted += 1
            except sqlite3.Error as e:
                logger.error(f"Insert error: {e}")
                continue
        
        self.conn.commit()
        logger.info(f"Inserted {inserted} traffic records")
        return inserted
    
    def query_traffic_by_date_range(self, 
                                   start_date: str, 
                                   end_date: str) -> pd.DataFrame:
        """
        日付範囲で交通量データを検索
        
        Args:
            start_date: 開始日
            end_date: 終了日
        
        Returns:
            DataFrame
        """
        query = '''
        SELECT * FROM traffic
        WHERE date BETWEEN ? AND ?
        ORDER BY date, time_period
        '''
        return pd.read_sql_query(query, self.conn, params=(start_date, end_date))
    
    def aggregate_traffic_by_date(self) -> pd.DataFrame:
        """
        日別の交通量を集計（時間帯を統合）
        
        Returns:
            日別集計DataFrame
        """
        query = '''
        SELECT 
            date,
            location_name,
            prefecture,
            SUM(total_count) as daily_total_count,
            AVG(travel_speed) as avg_travel_speed,
            COUNT(*) as time_periods
        FROM traffic
        GROUP BY date, location_name
        ORDER BY date
        '''
        return pd.read_sql_query(query, self.conn)
    
    # ========== 統合分析用メソッド ==========
    
    def join_weather_traffic(self, 
                           start_date: str, 
                           end_date: str,
                           location_name: Optional[str] = None) -> pd.DataFrame:
        """
        気象データと交通量データを結合
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名（Noneの場合は全地点）
        
        Returns:
            結合されたDataFrame
        """
        if location_name:
            query = '''
            SELECT 
                w.date,
                w.location_name,
                w.avg_temp,
                w.max_temp,
                w.min_temp,
                w.precipitation,
                w.max_wind_speed,
                w.sunshine_hours,
                w.avg_humidity,
                t.daily_total_count,
                t.avg_travel_speed
            FROM weather w
            LEFT JOIN (
                SELECT 
                    date,
                    location_name,
                    SUM(total_count) as daily_total_count,
                    AVG(travel_speed) as avg_travel_speed
                FROM traffic
                GROUP BY date, location_name
            ) t ON w.date = t.date AND w.location_name = t.location_name
            WHERE w.date BETWEEN ? AND ?
            AND w.location_name = ?
            ORDER BY w.date
            '''
            params = (start_date, end_date, location_name)
        else:
            query = '''
            SELECT 
                w.date,
                w.location_name,
                w.avg_temp,
                w.max_temp,
                w.min_temp,
                w.precipitation,
                w.max_wind_speed,
                w.sunshine_hours,
                w.avg_humidity,
                t.daily_total_count,
                t.avg_travel_speed
            FROM weather w
            LEFT JOIN (
                SELECT 
                    date,
                    location_name,
                    SUM(total_count) as daily_total_count,
                    AVG(travel_speed) as avg_travel_speed
                FROM traffic
                GROUP BY date, location_name
            ) t ON w.date = t.date AND w.location_name = t.location_name
            WHERE w.date BETWEEN ? AND ?
            ORDER BY w.date
            '''
            params = (start_date, end_date)
        
        return pd.read_sql_query(query, self.conn, params=params)
    
    def get_statistics(self) -> Dict[str, int]:
        """
        データベースの統計情報を取得
        
        Returns:
            統計情報の辞書
        """
        cursor = self.conn.cursor()
        
        # 気象データ件数
        cursor.execute('SELECT COUNT(*) FROM weather')
        weather_count = cursor.fetchone()[0]
        
        # 交通量データ件数
        cursor.execute('SELECT COUNT(*) FROM traffic')
        traffic_count = cursor.fetchone()[0]
        
        # 気象データの日付範囲
        cursor.execute('SELECT MIN(date), MAX(date) FROM weather')
        weather_range = cursor.fetchone()
        
        # 交通量データの日付範囲
        cursor.execute('SELECT MIN(date), MAX(date) FROM traffic')
        traffic_range = cursor.fetchone()
        
        return {
            'weather_records': weather_count,
            'traffic_records': traffic_count,
            'weather_date_range': weather_range,
            'traffic_date_range': traffic_range
        }
    
    def close(self):
        """データベース接続を閉じる"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """コンテキストマネージャーのサポート"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャーのサポート"""
        self.close()