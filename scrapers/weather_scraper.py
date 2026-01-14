"""
気象庁の過去の気象データをスクレイピング
サーバー負荷に配慮した実装
"""
import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherScraper:
    """
    気象庁の過去の気象データをスクレイピングするクラス
    サーバー負荷に配慮して適切な遅延を設定
    """
    
    BASE_URL = "https://www.data.jma.go.jp/obd/stats/etrn/view/daily_s1.php"
    
    # 主要都市の地点コード
    LOCATIONS = {
        '東京': {'prec_no': 44, 'block_no': 47662},
        '大阪': {'prec_no': 62, 'block_no': 47772},
        '名古屋': {'prec_no': 51, 'block_no': 47636},
        '札幌': {'prec_no': 14, 'block_no': 47412},
        '福岡': {'prec_no': 82, 'block_no': 47807},
    }
    
    def __init__(self, delay: float = 2.0):
        """
        Args:
            delay: リクエスト間隔（秒）デフォルトは2秒
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        logger.info(f"WeatherScraper initialized with {delay}s delay")
    
    def scrape_daily_data(self, 
                         location_name: str,
                         year: int, 
                         month: int) -> List[Dict]:
        """
        指定した地点・年月の日別データを取得
        
        Args:
            location_name: 地点名 ('東京', '大阪'など)
            year: 年
            month: 月
        
        Returns:
            日別気象データのリスト
        """
        if location_name not in self.LOCATIONS:
            logger.error(f"Unknown location: {location_name}")
            return []
        
        location = self.LOCATIONS[location_name]
        prec_no = location['prec_no']
        block_no = location['block_no']
        
        params = {
            'prec_no': prec_no,
            'block_no': block_no,
            'year': year,
            'month': month,
            'day': '',
            'view': ''
        }
        
        try:
            logger.info(f"Scraping {location_name} - {year}/{month:02d}")
            response = self.session.get(self.BASE_URL, params=params, timeout=10)
            response.encoding = 'utf-8'
            
            if response.status_code != 200:
                logger.error(f"HTTP Error {response.status_code}")
                return []
            
            soup = BeautifulSoup(response.content, 'html.parser')
            data = self._parse_daily_table(soup, location_name, year, month)
            
            logger.info(f"Successfully scraped {len(data)} records")
            
            # サーバー負荷に配慮して遅延
            time.sleep(self.delay)
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"Timeout error for {location_name} {year}/{month}")
            return []
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return []
    
    def _parse_daily_table(self, 
                          soup: BeautifulSoup, 
                          location_name: str,
                          year: int, 
                          month: int) -> List[Dict]:
        """
        HTMLテーブルから気象データを抽出
        
        Args:
            soup: BeautifulSoupオブジェクト
            location_name: 地点名
            year: 年
            month: 月
        
        Returns:
            パースされたデータのリスト
        """
        data_list = []
        
        # データテーブルを取得
        table = soup.find('table', {'class': 'data2_s'})
        if not table:
            logger.warning("Data table not found")
            return data_list
        
        rows = table.find_all('tr')[2:]  # ヘッダー行をスキップ
        
        for row in rows:
            cols = row.find_all(['td', 'th'])
            if len(cols) < 20:  # 十分な列数がない場合はスキップ
                continue
            
            try:
                day_text = cols[0].get_text(strip=True)
                if not day_text.isdigit():
                    continue
                
                day = int(day_text)
                
                # 各気象要素を抽出
                data = {
                    'location_code': f"{self.LOCATIONS[location_name]['prec_no']}-{self.LOCATIONS[location_name]['block_no']}",
                    'location_name': location_name,
                    'date': f"{year}-{month:02d}-{day:02d}",
                    'avg_temp': self._parse_value(cols[6]),
                    'max_temp': self._parse_value(cols[7]),
                    'min_temp': self._parse_value(cols[8]),
                    'precipitation': self._parse_value(cols[11]),
                    'max_wind_speed': self._parse_value(cols[15]),
                    'sunshine_hours': self._parse_value(cols[18]),
                    'avg_humidity': self._parse_value(cols[20])
                }
                
                data_list.append(data)
                
            except (ValueError, IndexError) as e:
                logger.debug(f"Parse error for row: {e}")
                continue
        
        return data_list
    
    def _parse_value(self, cell) -> Optional[float]:
        """
        セルの値を解析（欠測値などに対応）
        
        Args:
            cell: BeautifulSoupのセル要素
        
        Returns:
            数値またはNone
        """
        text = cell.get_text(strip=True)
        
        # 欠測値のパターン
        if text in ['--', '×', '///', '']:
            return None
        
        # 数値に変換
        try:
            # 記号を除去（］や）など）
            text = text.replace(']', '').replace(')', '').replace('#', '')
            return float(text)
        except ValueError:
            return None
    
    def scrape_date_range(self, 
                         location_name: str,
                         start_date: str,
                         end_date: str) -> List[Dict]:
        """
        期間を指定してデータを取得
        
        Args:
            location_name: 地点名
            start_date: 開始日 'YYYY-MM-DD'
            end_date: 終了日 'YYYY-MM-DD'
        
        Returns:
            期間内の全データ
        """
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        all_data = []
        current = start.replace(day=1)  # 月初に設定
        
        while current <= end:
            monthly_data = self.scrape_daily_data(
                location_name,
                current.year, 
                current.month
            )
            all_data.extend(monthly_data)
            
            # 次の月へ
            if current.month == 12:
                current = current.replace(year=current.year+1, month=1)
            else:
                current = current.replace(month=current.month+1)
        
        logger.info(f"Total scraped records: {len(all_data)}")
        return all_data
    
    def scrape_multiple_locations(self,
                                  location_names: List[str],
                                  year: int,
                                  month: int) -> Dict[str, List[Dict]]:
        """
        複数地点のデータを一度に取得
        
        Args:
            location_names: 地点名のリスト
            year: 年
            month: 月
        
        Returns:
            地点名をキーとした辞書
        """
        results = {}
        
        for location in location_names:
            data = self.scrape_daily_data(location, year, month)
            results[location] = data
        
        return results