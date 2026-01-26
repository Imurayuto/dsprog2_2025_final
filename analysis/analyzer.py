"""
気象データと交通量データの分析クラス
動的に入力に応じて出力を変化させる
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from typing import Optional, Dict, List, Tuple
import logging

logger = logging.getLogger(__name__)

# 日本語フォント設定
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'MS Gothic']
plt.rcParams['axes.unicode_minus'] = False


class WeatherTrafficAnalyzer:
    """
    気象データと交通量データの相関分析クラス
    入力パラメータに応じて動的に分析を実行
    """
    
    def __init__(self, db_manager):
        """
        Args:
            db_manager: WeatherTrafficDatabaseインスタンス
        """
        self.db = db_manager
        logger.info("Analyzer initialized")
    
    def analyze_correlation(self,
                          start_date: str,
                          end_date: str,
                          location_name: Optional[str] = None,
                          weather_variable: str = 'precipitation',
                          traffic_variable: str = 'daily_total_count') -> Dict:
        """
        気象要素と交通量の相関分析
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名
            weather_variable: 分析する気象要素
            traffic_variable: 分析する交通量要素
        
        Returns:
            分析結果の辞書
        """
        # データ取得
        data = self.db.join_weather_traffic(start_date, end_date, location_name)
        
        if data.empty:
            logger.warning("No data found for specified parameters")
            return {'error': 'No data available'}
        
        # 欠損値を除外
        valid_data = data[[weather_variable, traffic_variable]].dropna()
        
        if len(valid_data) < 10:
            logger.warning("Insufficient data for correlation analysis")
            return {'error': 'Insufficient data'}
        
        # 相関係数計算
        correlation, p_value = stats.pearsonr(
            valid_data[weather_variable],
            valid_data[traffic_variable]
        )
        
        # スピアマンの順位相関も計算
        spearman_corr, spearman_p = stats.spearmanr(
            valid_data[weather_variable],
            valid_data[traffic_variable]
        )
        
        results = {
            'data': valid_data,
            'pearson_correlation': correlation,
            'pearson_p_value': p_value,
            'spearman_correlation': spearman_corr,
            'spearman_p_value': spearman_p,
            'sample_size': len(valid_data),
            'weather_mean': valid_data[weather_variable].mean(),
            'weather_std': valid_data[weather_variable].std(),
            'traffic_mean': valid_data[traffic_variable].mean(),
            'traffic_std': valid_data[traffic_variable].std()
        }
        
        logger.info(f"Correlation: {correlation:.3f} (p={p_value:.4f})")
        return results
    
    def plot_correlation(self,
                        start_date: str,
                        end_date: str,
                        location_name: Optional[str] = None,
                        weather_variable: str = 'precipitation',
                        traffic_variable: str = 'daily_total_count',
                        figsize: Tuple[int, int] = (12, 5)) -> plt.Figure:
        """
        相関関係を可視化
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名
            weather_variable: 気象要素
            traffic_variable: 交通量要素
            figsize: 図のサイズ
        
        Returns:
            Matplotlibのfigureオブジェクト
        """
        results = self.analyze_correlation(
            start_date, end_date, location_name,
            weather_variable, traffic_variable
        )
        
        if 'error' in results:
            logger.error(f"Cannot plot: {results['error']}")
            return None
        
        data = results['data']
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize)
        
        # 散布図
        ax1.scatter(data[weather_variable], data[traffic_variable], alpha=0.5)
        ax1.set_xlabel(self._get_label(weather_variable))
        ax1.set_ylabel(self._get_label(traffic_variable))
        ax1.set_title(f'Correlation Analysis\nr={results["pearson_correlation"]:.3f}, p={results["pearson_p_value"]:.4f}')
        ax1.grid(True, alpha=0.3)
        
        # 回帰直線を追加
        z = np.polyfit(data[weather_variable], data[traffic_variable], 1)
        p = np.poly1d(z)
        ax1.plot(data[weather_variable], p(data[weather_variable]), "r--", alpha=0.8)
        
        # 時系列プロット
        data_with_date = self.db.join_weather_traffic(start_date, end_date, location_name)
        data_with_date['date'] = pd.to_datetime(data_with_date['date'])
        
        ax2_twin = ax2.twinx()
        
        line1 = ax2.plot(data_with_date['date'], data_with_date[weather_variable], 
                        'b-', label=self._get_label(weather_variable), alpha=0.7)
        line2 = ax2_twin.plot(data_with_date['date'], data_with_date[traffic_variable], 
                             'r-', label=self._get_label(traffic_variable), alpha=0.7)
        
        ax2.set_xlabel('Date')
        ax2.set_ylabel(self._get_label(weather_variable), color='b')
        ax2_twin.set_ylabel(self._get_label(traffic_variable), color='r')
        ax2.tick_params(axis='y', labelcolor='b')
        ax2_twin.tick_params(axis='y', labelcolor='r')
        ax2.set_title('Time Series')
        ax2.grid(True, alpha=0.3)
        
        # 凡例を統合
        lines = line1 + line2
        labels = [l.get_label() for l in lines]
        ax2.legend(lines, labels, loc='upper left')
        
        plt.tight_layout()
        return fig
    
    def categorize_weather(self,
                          start_date: str,
                          end_date: str,
                          location_name: Optional[str] = None,
                          weather_variable: str = 'precipitation',
                          thresholds: Optional[List[float]] = None) -> pd.DataFrame:
        """
        気象条件をカテゴリ化して交通量を比較
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名
            weather_variable: 気象要素
            thresholds: カテゴリ分けの閾値リスト
        
        Returns:
            カテゴリ別統計のDataFrame
        """
        data = self.db.join_weather_traffic(start_date, end_date, location_name)
        
        if thresholds is None:
            # デフォルトの閾値（降水量の場合）
            if weather_variable == 'precipitation':
                thresholds = [0.5, 10, 30]
            elif weather_variable == 'avg_temp':
                thresholds = [10, 20, 30]
            else:
                thresholds = data[weather_variable].quantile([0.33, 0.67]).tolist()
        
        # カテゴリ化
        labels = [f'Low (< {thresholds[0]})']
        for i in range(len(thresholds) - 1):
            labels.append(f'Mid ({thresholds[i]}-{thresholds[i+1]})')
        labels.append(f'High (>= {thresholds[-1]})')
        
        data['category'] = pd.cut(
            data[weather_variable],
            bins=[-np.inf] + thresholds + [np.inf],
            labels=labels
        )
        
        # カテゴリ別統計
        stats_df = data.groupby('category', observed=True).agg({
            'daily_total_count': ['count', 'mean', 'std', 'median'],
            'avg_travel_speed': ['mean', 'std']
        }).round(2)
        
        return stats_df
    
    def plot_categorical_comparison(self,
                                   start_date: str,
                                   end_date: str,
                                   location_name: Optional[str] = None,
                                   weather_variable: str = 'precipitation',
                                   thresholds: Optional[List[float]] = None,
                                   figsize: Tuple[int, int] = (14, 5)) -> plt.Figure:
        """
        カテゴリ別の交通量比較を可視化
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名
            weather_variable: 気象要素
            thresholds: 閾値
            figsize: 図のサイズ
        
        Returns:
            Figure
        """
        data = self.db.join_weather_traffic(start_date, end_date, location_name)
        
        if thresholds is None:
            if weather_variable == 'precipitation':
                thresholds = [0.5, 10, 30]
            elif weather_variable == 'avg_temp':
                thresholds = [10, 20, 30]
            else:
                thresholds = data[weather_variable].quantile([0.33, 0.67]).tolist()
        
        # カテゴリ化
        labels = [f'Low\n(< {thresholds[0]})']
        for i in range(len(thresholds) - 1):
            labels.append(f'Mid\n({thresholds[i]}-{thresholds[i+1]})')
        labels.append(f'High\n(>= {thresholds[-1]})')
        
        data['category'] = pd.cut(
            data[weather_variable],
            bins=[-np.inf] + thresholds + [np.inf],
            labels=labels
        )
        
        fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=figsize)
        
        # 箱ひげ図（交通量）
        data.boxplot(column='daily_total_count', by='category', ax=ax1)
        ax1.set_title(f'Traffic Volume by {self._get_label(weather_variable)}')
        ax1.set_xlabel('Category')
        ax1.set_ylabel('Daily Total Count')
        plt.sca(ax1)
        plt.xticks(rotation=0)
        
        # 箱ひげ図（速度）
        data.boxplot(column='avg_travel_speed', by='category', ax=ax2)
        ax2.set_title(f'Travel Speed by {self._get_label(weather_variable)}')
        ax2.set_xlabel('Category')
        ax2.set_ylabel('Avg Travel Speed (km/h)')
        plt.sca(ax2)
        plt.xticks(rotation=0)
        
        # カテゴリ別の日数
        category_counts = data['category'].value_counts().sort_index()
        ax3.bar(range(len(category_counts)), category_counts.values)
        ax3.set_xticks(range(len(category_counts)))
        ax3.set_xticklabels(category_counts.index, rotation=0)
        ax3.set_title('Number of Days per Category')
        ax3.set_xlabel('Category')
        ax3.set_ylabel('Days')
        
        plt.suptitle('')  # 自動タイトルを削除
        plt.tight_layout()
        return fig
    
    def compare_multiple_locations(self,
                                  start_date: str,
                                  end_date: str,
                                  location_names: List[str],
                                  weather_variable: str = 'precipitation') -> pd.DataFrame:
        """
        複数地点の気象と交通量の関係を比較
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_names: 地点名のリスト
            weather_variable: 気象要素
        
        Returns:
            比較結果のDataFrame
        """
        results = []
        
        for location in location_names:
            corr_results = self.analyze_correlation(
                start_date, end_date, location,
                weather_variable, 'daily_total_count'
            )
            
            if 'error' not in corr_results:
                results.append({
                    'location': location,
                    'correlation': corr_results['pearson_correlation'],
                    'p_value': corr_results['pearson_p_value'],
                    'sample_size': corr_results['sample_size'],
                    'avg_weather': corr_results['weather_mean'],
                    'avg_traffic': corr_results['traffic_mean']
                })
        
        return pd.DataFrame(results)
    
    def _get_label(self, variable: str) -> str:
        """
        変数名から表示用ラベルを取得
        
        Args:
            variable: 変数名
        
        Returns:
            ラベル文字列
        """
        labels = {
            'precipitation': 'Precipitation (mm)',
            'avg_temp': 'Avg Temperature (C)',
            'max_temp': 'Max Temperature (C)',
            'min_temp': 'Min Temperature (C)',
            'max_wind_speed': 'Max Wind Speed (m/s)',
            'sunshine_hours': 'Sunshine Hours (h)',
            'avg_humidity': 'Avg Humidity (%)',
            'daily_total_count': 'Daily Traffic Volume',
            'avg_travel_speed': 'Avg Travel Speed (km/h)'
        }
        return labels.get(variable, variable)
    
    def generate_summary_report(self,
                               start_date: str,
                               end_date: str,
                               location_name: Optional[str] = None) -> str:
        """
        分析結果のサマリーレポートを生成
        
        Args:
            start_date: 開始日
            end_date: 終了日
            location_name: 地点名
        
        Returns:
            レポート文字列
        """
        # 降水量と交通量の相関
        precip_corr = self.analyze_correlation(
            start_date, end_date, location_name,
            'precipitation', 'daily_total_count'
        )
        
        # 気温と交通量の相関
        temp_corr = self.analyze_correlation(
            start_date, end_date, location_name,
            'avg_temp', 'daily_total_count'
        )
        
        report = f"""
========================================
Weather-Traffic Correlation Analysis
========================================
Period: {start_date} to {end_date}
Location: {location_name or 'All locations'}

[Precipitation vs Traffic Volume]
Correlation: {precip_corr.get('pearson_correlation', 'N/A'):.3f}
P-value: {precip_corr.get('pearson_p_value', 'N/A'):.4f}
Sample size: {precip_corr.get('sample_size', 'N/A')}

[Temperature vs Traffic Volume]
Correlation: {temp_corr.get('pearson_correlation', 'N/A'):.3f}
P-value: {temp_corr.get('pearson_p_value', 'N/A'):.4f}
Sample size: {temp_corr.get('sample_size', 'N/A')}

========================================
"""
        return report