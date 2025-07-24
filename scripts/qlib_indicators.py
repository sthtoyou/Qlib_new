#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import talib
import struct
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set
from loguru import logger
import warnings
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
from functools import partial
import multiprocessing
import csv
import gc
import logging
import json
import hashlib
from datetime import datetime, timedelta
import shutil

warnings.filterwarnings('ignore', category=RuntimeWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


class QlibIndicatorsEnhancedCalculator:
    """
    增强版Qlib指标计算器
    集成Alpha158、Alpha360指标体系和多种技术分析指标
    支持多线程并行计算、指标去重功能和增量计算
    """
    
    # 字段中文标签映射
    FIELD_LABELS = {
        # 基础字段
        'Date': '日期',
        'Symbol': '股票代码',
        
        # OHLCV基础数据
        'Open': '开盘价',
        'High': '最高价', 
        'Low': '最低价',
        'Close': '收盘价',
        'Volume': '成交量',
        
        # 波动率指标
        'RealizedVolatility_20': '20日已实现波动率',
        'NegativeSemiDeviation_20': '20日负半偏差',
        'ContinuousVolatility_20': '20日连续波动率',
        'PositiveSemiDeviation_20': '20日正半偏差',
        'Volatility_10': '10日波动率',
        'Volatility_30': '30日波动率',
        'Volatility_60': '60日波动率',
        
        # 蜡烛图模式
        'CDL2CROWS': '两只乌鸦',
        'CDL3BLACKCROWS': '三只乌鸦',
        'CDL3INSIDE': '三内部上涨和下跌',
        'CDL3LINESTRIKE': '三线打击',
        'CDL3OUTSIDE': '三外部上涨和下跌',
        'CDL3STARSINSOUTH': '南方三星',
        'CDL3WHITESOLDIERS': '三白兵',
        'CDLABANDONEDBABY': '弃婴',
        'CDLADVANCEBLOCK': '大敌当前',
        'CDLBELTHOLD': '捉腰带线',
        'CDLBREAKAWAY': '脱离',
        'CDLCLOSINGMARUBOZU': '收盘缺影线',
        'CDLCONCEALBABYSWALL': '藏婴吞没',
        'CDLCOUNTERATTACK': '反击线',
        'CDLDARKCLOUDCOVER': '乌云压顶',
        'CDLDOJI': '十字',
        'CDLDOJISTAR': '十字星',
        'CDLDRAGONFLYDOJI': '蜻蜓十字',
        'CDLENGULFING': '吞噬模式',
        'CDLEVENINGDOJISTAR': '十字暮星',
        'CDLEVENINGSTAR': '暮星',
        'CDLGAPSIDESIDEWHITE': '向上跳空并列阳线',
        'CDLGRAVESTONEDOJI': '墓碑十字',
        'CDLHAMMER': '锤子线',
        'CDLHANGINGMAN': '上吊线',
        'CDLHARAMI': '母子线',
        'CDLHARAMICROSS': '十字孕线',
        'CDLHIGHWAVE': '风高浪大线',
        'CDLHIKKAKE': '陷阱',
        'CDLHIKKAKEMOD': '修正陷阱',
        'CDLHOMINGPIGEON': '家鸽',
        'CDLIDENTICAL3CROWS': '三胞胎乌鸦',
        'CDLINNECK': '颈内线',
        'CDLINVERTEDHAMMER': '倒锤子线',
        'CDLKICKING': '反冲形态',
        'CDLKICKINGBYLENGTH': '由较长缺影线决定的反冲形态',
        'CDLLADDERBOTTOM': '梯底',
        'CDLLONGLEGGEDDOJI': '长脚十字',
        'CDLLONGLINE': '长蜡烛线',
        'CDLMARUBOZU': '光头光脚/缺影线',
        'CDLMATCHINGLOW': '相同低价',
        'CDLMATHOLD': '铺垫',
        'CDLMORNINGDOJISTAR': '十字晨星',
        'CDLMORNINGSTAR': '晨星',
        'CDLONNECK': '颈上线',
        'CDLPIERCING': '刺穿形态',
        'CDLRICKSHAWMAN': '黄包车夫',
        'CDLRISEFALL3METHODS': '上升和下降三法',
        'CDLSEPARATINGLINES': '分离线',
        'CDLSHOOTINGSTAR': '射击之星',
        'CDLSHORTLINE': '短蜡烛线',
        'CDLSPINNINGTOP': '陀螺',
        'CDLSTALLEDPATTERN': '停顿形态',
        'CDLSTICKSANDWICH': '条形三明治',
        'CDLTAKURI': '探水竿',
        'CDLTASUKIGAP': '跳空并列阴阳线',
        'CDLTHRUSTING': '插入',
        'CDLTRISTAR': '三星',
        'CDLUNIQUE3RIVER': '奇特三河床',
        'CDLUPSIDEGAP2CROWS': '上升跳空两乌鸦',
        'CDLXSIDEGAP3METHODS': '上升/下降跳空三法',
        
        # 移动平均线
        'SMA_5': '5日简单移动平均',
        'SMA_10': '10日简单移动平均',
        'SMA_20': '20日简单移动平均',
        'SMA_50': '50日简单移动平均',
        'EMA_5': '5日指数移动平均',
        'EMA_10': '10日指数移动平均',
        'EMA_20': '20日指数移动平均',
        'EMA_50': '50日指数移动平均',
        'DEMA_20': '20日双重指数移动平均',
        'TEMA_20': '20日三重指数移动平均',
        'KAMA_30': '30日自适应移动平均',
        'WMA_20': '20日加权移动平均',
        
        # MACD指标
        'MACD': 'MACD主线',
        'MACD_Signal': 'MACD信号线',
        'MACD_Histogram': 'MACD柱状图',
        'MACDEXT': 'MACD扩展',
        'MACDFIX': 'MACD固定',
        
        # 动量指标
        'RSI_14': '14日相对强弱指数',
        'CCI_14': '14日商品通道指数',
        'CMO_14': '14日钱德动量摆动指标',
        'MFI_14': '14日资金流量指数',
        'WILLR_14': '14日威廉指标',
        'ULTOSC': '终极摆动指标',
        
        # 趋势指标
        'ADX_14': '14日平均趋向指数',
        'ADXR_14': '14日平均趋向指数评级',
        'APO': '价格震荡指标',
        'AROON_DOWN': '阿隆下降',
        'AROON_UP': '阿隆上升',
        'AROONOSC_14': '14日阿隆震荡指标',
        'BOP': '平衡震荡指标',
        'DX_14': '14日方向运动指数',
        'MINUS_DI_14': '14日负向动态指标',
        'MINUS_DM_14': '14日负向动态动量',
        'PLUS_DI_14': '14日正向动态指标',
        'PLUS_DM_14': '14日正向动态动量',
        'PPO': '价格震荡百分比',
        'TRIX_30': '30日三重指数平滑移动平均',
        
        # 价格动量
        'MOM_10': '10日动量',
        'ROC_10': '10日变动率',
        'ROCP_10': '10日变动率百分比',
        'ROCR_10': '10日变动率比率',
        'ROCR100_10': '10日变动率比率100',
        
        # 布林带
        'BB_Upper': '布林带上轨',
        'BB_Middle': '布林带中轨',
        'BB_Lower': '布林带下轨',
        
        # 随机指标
        'STOCH_K': '随机指标K值',
        'STOCH_D': '随机指标D值',
        'STOCHF_K': '快速随机指标K值',
        'STOCHF_D': '快速随机指标D值',
        'STOCHRSI_K': '随机RSI指标K值',
        'STOCHRSI_D': '随机RSI指标D值',
        
        # 波动率指标
        'ATR_14': '14日平均真实波幅',
        'NATR_14': '14日归一化平均真实波幅',
        'TRANGE': '真实波幅',
        
        # 成交量指标
        'OBV': '能量潮',
        'AD': '累积/派发线',
        'ADOSC': '累积/派发震荡指标',
        
        # 希尔伯特变换指标
        'HT_DCPERIOD': '希尔伯特变换主导周期',
        'HT_DCPHASE': '希尔伯特变换主导周期相位',
        'HT_INPHASE': '希尔伯特变换同相分量',
        'HT_QUADRATURE': '希尔伯特变换正交分量',
        'HT_SINE': '希尔伯特变换正弦波',
        'HT_LEADSINE': '希尔伯特变换领先正弦波',
        'HT_TRENDMODE': '希尔伯特变换趋势模式',
        'HT_TRENDLINE': '希尔伯特变换趋势线',
        
        # 价格指标
        'AVGPRICE': '平均价格',
        'MEDPRICE': '中间价格',
        'TYPPRICE': '典型价格',
        'WCLPRICE': '加权收盘价',
        'MIDPOINT': '中点',
        'MIDPRICE': '中间价格',
        'MAMA': 'MESA自适应移动平均',
        'FAMA': '跟随自适应移动平均',
        
        # 统计指标
        'LINEARREG': '线性回归',
        'LINEARREG_ANGLE': '线性回归角度',
        'LINEARREG_INTERCEPT': '线性回归截距',
        'LINEARREG_SLOPE': '线性回归斜率',
        'STDDEV': '标准差',
        'TSF': '时间序列预测',
        'VAR': '方差',
        'MAXINDEX': '最大值索引',
        'MININDEX': '最小值索引',
        
        # 财务指标
        'PriceToBookRatio': '市净率',
        'MarketCap': '市值',
        'PERatio': '市盈率',
        'PriceToSalesRatio': '市销率',
        'ROE': '净资产收益率',
        'ROA': '总资产收益率',
        'ProfitMargins': '利润率',
        'QuickRatio': '速动比率',
        'DebtToEquity': '资产负债率',
        'TobinsQ': '托宾Q值',
        'DailyTurnover': '日换手率',
        'turnover_c1d': '1日累计换手率',
        'turnover_c5d': '5日累计换手率',
        'turnover_m5d': '5日移动换手率',
        'turnover_c10d': '10日累计换手率',
        'turnover_m10d': '10日移动换手率',
        'turnover_c20d': '20日累计换手率',
        'turnover_m20d': '20日移动换手率',
        'turnover_c30d': '30日累计换手率',
        'turnover_m30d': '30日移动换手率',
        'CurrentRatio': '流动比率',
    }
    
    @classmethod
    def _generate_alpha360_labels(cls):
        """生成Alpha360指标的中文标签"""
        alpha360_labels = {}
        
        # 为Alpha360指标生成标签
        for field in ['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VWAP', 'VOLUME']:
            for i in range(60):
                field_name = f'ALPHA360_{field}{i}'
                if field == 'CLOSE':
                    alpha360_labels[field_name] = f'Alpha360收盘价{i}日前'
                elif field == 'OPEN':
                    alpha360_labels[field_name] = f'Alpha360开盘价{i}日前'
                elif field == 'HIGH':
                    alpha360_labels[field_name] = f'Alpha360最高价{i}日前'
                elif field == 'LOW':
                    alpha360_labels[field_name] = f'Alpha360最低价{i}日前'
                elif field == 'VWAP':
                    alpha360_labels[field_name] = f'Alpha360成交量加权平均价{i}日前'
                elif field == 'VOLUME':
                    alpha360_labels[field_name] = f'Alpha360成交量{i}日前'
        
        return alpha360_labels
    
    @classmethod
    def _generate_alpha158_labels(cls):
        """生成Alpha158指标的中文标签"""
        alpha158_labels = {}
        
        # 基础K线指标
        alpha158_labels.update({
            'ALPHA158_KMID': 'K线中点价格',
            'ALPHA158_KLEN': 'K线长度',
            'ALPHA158_KMID2': 'K线中点价格平方',
            'ALPHA158_KUP': 'K线上影线',
            'ALPHA158_KUP2': 'K线上影线平方',
            'ALPHA158_KLOW': 'K线下影线',
            'ALPHA158_KLOW2': 'K线下影线平方',
            'ALPHA158_KSFT': 'K线偏移',
            'ALPHA158_KSFT2': 'K线偏移平方',
            'ALPHA158_OPEN0': '当日开盘价',
            'ALPHA158_HIGH0': '当日最高价',
            'ALPHA158_LOW0': '当日最低价',
            'ALPHA158_VWAP0': '当日成交量加权平均价',
            'ALPHA158_VOLUME0': '当日成交量',
        })
        
        # 收益率指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_ROC{period}'] = f'{period}日收益率'
        
        # 移动平均指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_MA{period}'] = f'{period}日移动平均'
        
        # 标准差指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_STD{period}'] = f'{period}日标准差'
        
        # 贝塔值指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_BETA{period}'] = f'{period}日贝塔值'
        
        # R平方指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_RSQR{period}'] = f'{period}日R平方'
        
        # 残差指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_RESI{period}'] = f'{period}日线性回归残差'
        
        # 最大值指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_MAX{period}'] = f'{period}日最大值'
        
        # 最小值指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_MIN{period}'] = f'{period}日最小值'
        
        # 四分位数指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_QTLU{period}'] = f'{period}日上四分位数'
            alpha158_labels[f'ALPHA158_QTLD{period}'] = f'{period}日下四分位数'
        
        # 排名指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_RANK{period}'] = f'{period}日排名'
        
        # RSV指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_RSV{period}'] = f'{period}日RSV'
        
        # 位置指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_IMAX{period}'] = f'{period}日最高价位置指数'
            alpha158_labels[f'ALPHA158_IMIN{period}'] = f'{period}日最低价位置指数'
            alpha158_labels[f'ALPHA158_IMXD{period}'] = f'{period}日最高最低价位置差'
        
        # 相关性指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_CORR{period}'] = f'{period}日相关性'
            alpha158_labels[f'ALPHA158_CORD{period}'] = f'{period}日相关性差分'
        
        # 计数指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_CNTP{period}'] = f'{period}日上涨天数比例'
            alpha158_labels[f'ALPHA158_CNTN{period}'] = f'{period}日下跌天数比例'
            alpha158_labels[f'ALPHA158_CNTD{period}'] = f'{period}日涨跌天数差'
        
        # 求和指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_SUMP{period}'] = f'{period}日总收益比例'
            alpha158_labels[f'ALPHA158_SUMN{period}'] = f'{period}日总损失比例'
            alpha158_labels[f'ALPHA158_SUMD{period}'] = f'{period}日收益损失差比例'
        
        # 成交量指标
        for period in [5, 10, 20, 30, 60]:
            alpha158_labels[f'ALPHA158_VMA{period}'] = f'{period}日成交量移动平均'
            alpha158_labels[f'ALPHA158_VSTD{period}'] = f'{period}日成交量标准差'
            alpha158_labels[f'ALPHA158_WVMA{period}'] = f'{period}日成交量加权价格波动率'
            alpha158_labels[f'ALPHA158_VSUMP{period}'] = f'{period}日成交量上升比例'
            alpha158_labels[f'ALPHA158_VSUMN{period}'] = f'{period}日成交量下降比例'
            alpha158_labels[f'ALPHA158_VSUMD{period}'] = f'{period}日成交量涨跌差比例'
        
        return alpha158_labels
    
    def get_field_labels(self, columns):
        """获取字段的中文标签"""
        # 合并所有标签
        all_labels = self.FIELD_LABELS.copy()
        all_labels.update(self._generate_alpha360_labels())
        all_labels.update(self._generate_alpha158_labels())
        
        # 返回指定列的中文标签
        labels = []
        for col in columns:
            labels.append(all_labels.get(col, col))
        return labels
    
    def __init__(self, data_dir: str = r"D:\stk_data\trd\us_data", financial_data_dir: str = None, 
                 max_workers: int = None, enable_parallel: bool = True,
                 cache_dir: str = "indicator_cache", enable_incremental: bool = False,
                 start_date: str = None, end_date: str = None, recent_days: int = None):
        """
        初始化增强版指标计算器
        
        Parameters:
        -----------
        data_dir : str
            数据目录路径
        financial_data_dir : str
            财务数据目录路径
        max_workers : int
            最大工作线程数
        enable_parallel : bool
            是否启用并行计算
        cache_dir : str
            缓存目录，用于增量计算
        enable_incremental : bool
            是否启用增量计算模式
        start_date : str
            计算开始日期 (YYYY-MM-DD格式)
        end_date : str
            计算结束日期 (YYYY-MM-DD格式)
        recent_days : int
            计算最近N天的数据
        """
        self.data_dir = Path(data_dir)
        self.features_dir = self.data_dir / "features"
        self.output_dir = self.data_dir
        self.financial_data_dir = Path(financial_data_dir) if financial_data_dir else None
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.enable_parallel = enable_parallel
        self.enable_incremental = enable_incremental
        
        # 时间窗口设置
        self.start_date = None
        self.end_date = None
        self._setup_time_window(start_date, end_date, recent_days)
        
        # 增量计算相关
        if self.enable_incremental:
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(exist_ok=True)
            
            # 缓存文件路径
            self.metadata_file = self.cache_dir / "metadata.json"
            self.stock_status_file = self.cache_dir / "stock_status.json"
            self.data_hashes_file = self.cache_dir / "data_hashes.json"
            self.date_ranges_file = self.cache_dir / "date_ranges.json"
            self.output_backup_dir = self.cache_dir / "output_backups"
            self.output_backup_dir.mkdir(exist_ok=True)
            
            # 加载或初始化元数据
            self.metadata = self._load_metadata()
            self.stock_status = self._load_stock_status()
            self.data_hashes = self._load_data_hashes()
            self.date_ranges = self._load_date_ranges()
            
            logger.info(f"增量计算模式已启用，缓存目录: {self.cache_dir}")
        
        # 指标缓存
        self._indicators_cache = {}
        self._indicators_cache_lock = threading.Lock()
        
        # 财务数据缓存
        self._financial_data_cache = {}
        self._financial_data_cache_lock = threading.Lock()
        
        # 线程本地存储
        self._local = threading.local()
        
        # 加载财务数据
        if self.financial_data_dir:
            self._load_financial_data()
        
        logger.info(f"增强版指标计算器初始化完成 (并行: {enable_parallel}, 增量: {enable_incremental})")
        
    def _setup_time_window(self, start_date: str, end_date: str, recent_days: int):
        """设置时间窗口"""
        try:
            if recent_days is not None:
                # 使用最近N天
                if start_date or end_date:
                    logger.warning("同时指定了recent_days和start_date/end_date，优先使用recent_days")
                
                self.end_date = pd.Timestamp.now().normalize()
                self.start_date = self.end_date - pd.Timedelta(days=recent_days)
                logger.info(f"📅 时间窗口设置: 最近{recent_days}天 ({self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')})")
                
            elif start_date or end_date:
                # 使用指定的开始和结束日期
                if start_date:
                    self.start_date = pd.to_datetime(start_date)
                    logger.info(f"📅 开始日期: {self.start_date.strftime('%Y-%m-%d')}")
                    
                if end_date:
                    self.end_date = pd.to_datetime(end_date)
                    logger.info(f"📅 结束日期: {self.end_date.strftime('%Y-%m-%d')}")
                    
                if self.start_date and self.end_date:
                    if self.start_date >= self.end_date:
                        raise ValueError("开始日期必须早于结束日期")
                    days_span = (self.end_date - self.start_date).days
                    logger.info(f"📅 时间窗口: {days_span}天 ({self.start_date.strftime('%Y-%m-%d')} 至 {self.end_date.strftime('%Y-%m-%d')})")
                    
            else:
                # 没有指定时间窗口，使用全部数据
                logger.info("📅 时间窗口: 全部数据")
                
        except Exception as e:
            logger.error(f"时间窗口设置失败: {e}")
            raise ValueError(f"无效的时间窗口参数: {e}")
    
    def _apply_time_window_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据时间窗口过滤数据"""
        if df.empty:
            return df
            
        if self.start_date is None and self.end_date is None:
            return df
            
        try:
            # 确保DataFrame有日期索引或Date列
            if 'Date' in df.columns:
                # 如果有Date列，使用Date列进行过滤
                df_filtered = df.copy()
                date_series = pd.to_datetime(df_filtered['Date'])
                
                if self.start_date is not None:
                    mask_start = date_series >= self.start_date
                    df_filtered = df_filtered[mask_start]
                    
                if self.end_date is not None:
                    mask_end = date_series <= self.end_date
                    df_filtered = df_filtered[mask_end]
                    
            elif isinstance(df.index, pd.DatetimeIndex):
                # 如果索引是日期索引，直接使用索引过滤
                df_filtered = df.copy()
                
                if self.start_date is not None:
                    df_filtered = df_filtered[df_filtered.index >= self.start_date]
                    
                if self.end_date is not None:
                    df_filtered = df_filtered[df_filtered.index <= self.end_date]
                    
            else:
                logger.warning("数据没有日期索引或Date列，无法应用时间窗口过滤")
                return df
                
            if not df_filtered.empty:
                original_len = len(df)
                filtered_len = len(df_filtered)
                logger.debug(f"时间窗口过滤: {original_len} -> {filtered_len} 行")
                
            return df_filtered
            
        except Exception as e:
            logger.error(f"时间窗口过滤失败: {e}")
            return df

    # 增量计算相关方法
    def _load_metadata(self) -> Dict:
        """加载元数据"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载元数据失败: {e}")
        
        return {
            "last_update": None,
            "total_stocks": 0,
            "processed_stocks": 0,
            "version": "2.0",
            "output_file": None,
            "last_output_backup": None
        }
    
    def _save_metadata(self):
        """保存元数据"""
        try:
            with open(self.metadata_file, 'w', encoding='utf-8') as f:
                json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存元数据失败: {e}")
    
    def _load_stock_status(self) -> Dict:
        """加载股票状态"""
        if self.stock_status_file.exists():
            try:
                with open(self.stock_status_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载股票状态失败: {e}")
        
        return {}
    
    def _save_stock_status(self):
        """保存股票状态"""
        try:
            with open(self.stock_status_file, 'w', encoding='utf-8') as f:
                json.dump(self.stock_status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存股票状态失败: {e}")
    
    def _load_data_hashes(self) -> Dict:
        """加载数据哈希值"""
        if self.data_hashes_file.exists():
            try:
                with open(self.data_hashes_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载数据哈希失败: {e}")
        
        return {}
    
    def _save_data_hashes(self):
        """保存数据哈希值"""
        try:
            with open(self.data_hashes_file, 'w', encoding='utf-8') as f:
                json.dump(self.data_hashes, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存数据哈希失败: {e}")
    
    def _load_date_ranges(self) -> Dict:
        """加载日期范围信息"""
        if self.date_ranges_file.exists():
            try:
                with open(self.date_ranges_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"加载日期范围失败: {e}")
        
        return {}
    
    def _save_date_ranges(self):
        """保存日期范围信息"""
        try:
            with open(self.date_ranges_file, 'w', encoding='utf-8') as f:
                json.dump(self.date_ranges, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"保存日期范围失败: {e}")
    
    def _calculate_data_hash(self, symbol: str, date_range: Optional[Tuple[str, str]] = None) -> str:
        """计算股票数据的哈希值"""
        try:
            data = self.read_qlib_binary_data(symbol)
            if data is None or data.empty:
                return ""
            
            # 如果指定了日期范围，只计算该范围内的数据哈希
            if date_range:
                start_date, end_date = date_range
                data = data[(data.index >= start_date) & (data.index <= end_date)]
                if data.empty:
                    return ""
            
            # 计算关键字段的哈希值
            key_fields = ['Open', 'High', 'Low', 'Close', 'Volume']
            hash_data = data[key_fields].fillna(0).values.tobytes()
            return hashlib.md5(hash_data).hexdigest()
            
        except Exception as e:
            logger.warning(f"计算 {symbol} 数据哈希失败: {e}")
            return ""
    
    def _get_stock_date_range(self, symbol: str) -> Tuple[Optional[str], Optional[str]]:
        """获取股票的数据日期范围"""
        try:
            data = self.read_qlib_binary_data(symbol)
            if data is None or data.empty:
                return None, None
            
            return str(data.index.min()), str(data.index.max())
            
        except Exception as e:
            logger.warning(f"获取 {symbol} 日期范围失败: {e}")
            return None, None
    
    def _is_data_changed(self, symbol: str, date_range: Optional[Tuple[str, str]] = None) -> bool:
        """检查股票数据是否发生变化"""
        current_hash = self._calculate_data_hash(symbol, date_range)
        
        # 构建哈希键
        if date_range:
            hash_key = f"{symbol}_{date_range[0]}_{date_range[1]}"
        else:
            hash_key = symbol
        
        previous_hash = self.data_hashes.get(hash_key, "")
        
        if current_hash != previous_hash:
            self.data_hashes[hash_key] = current_hash
            return True
        
        return False
    
    def _get_stock_last_update(self, symbol: str) -> Optional[str]:
        """获取股票最后更新时间"""
        return self.stock_status.get(symbol, {}).get('last_update')
    
    def _update_stock_status(self, symbol: str, success: bool, rows: int = 0, 
                           date_range: Optional[Tuple[str, str]] = None):
        """
        更新股票状态
        
        Parameters:
        -----------
        symbol : str
            股票代码
        success : bool
            是否成功
        rows : int
            处理的行数
        date_range : Optional[Tuple[str, str]]
            日期范围 (start_date, end_date)
        """
        if symbol not in self.stock_status:
            self.stock_status[symbol] = {}
        
        self.stock_status[symbol].update({
            'last_update': datetime.now().isoformat(),
            'success': success,
            'rows': rows,
            'indicators_count': 695  # 预期的指标数量
        })
        
        # 更新日期范围信息
        if date_range:
            self.date_ranges[symbol] = {
                'start_date': date_range[0],
                'end_date': date_range[1],
                'last_update': datetime.now().isoformat()
            }
    
    def _needs_update(self, symbol: str, force_update: bool = False, 
                     date_range: Optional[Tuple[str, str]] = None) -> Tuple[bool, str]:
        """
        判断股票是否需要更新
        基于"Stock X Date X Indicator"维度进行增量判断
        
        Parameters:
        -----------
        symbol : str
            股票代码
        force_update : bool
            是否强制更新
        date_range : Optional[Tuple[str, str]]
            日期范围 (start_date, end_date)
            
        Returns:
        --------
        Tuple[bool, str]: (是否需要更新, 原因)
        """
        if force_update:
            return True, "强制更新"
        
        # 检查数据是否发生变化（基于数据哈希）
        if self._is_data_changed(symbol, date_range):
            return True, "数据发生变化"
        
        # 检查是否有新的日期范围
        if date_range:
            current_range = self.date_ranges.get(symbol, {})
            current_start = current_range.get('start_date')
            current_end = current_range.get('end_date')
            
            # 检查日期范围是否扩展
            if current_start is None or current_end is None:
                return True, "首次获取日期范围"
            
            # 检查是否有新数据（日期范围扩展）
            if date_range[0] < current_start:
                return True, f"日期范围扩展（开始日期: {date_range[0]} < {current_start}）"
            
            if date_range[1] > current_end:
                return True, f"日期范围扩展（结束日期: {date_range[1]} > {current_end}）"
            
            # 检查日期范围是否收缩（可能是数据修正）
            if date_range[0] > current_start or date_range[1] < current_end:
                return True, f"日期范围变化（可能是数据修正）"
        
        # 检查是否从未处理过
        if symbol not in self.stock_status:
            return True, "首次处理"
        
        # 检查上次处理是否成功
        if not self.stock_status[symbol].get('success', False):
            return True, "上次处理失败"
        
        # 检查是否有指标数量变化（可能算法更新）
        current_indicators_count = self.stock_status[symbol].get('indicators_count', 0)
        expected_indicators_count = 695  # 预期的指标数量
        if current_indicators_count != expected_indicators_count:
            return True, f"指标数量变化（当前: {current_indicators_count}, 预期: {expected_indicators_count}）"
        
        # 检查最后更新时间（可选：基于时间间隔的更新）
        last_update = self.stock_status[symbol].get('last_update')
        if last_update:
            try:
                last_update_time = datetime.fromisoformat(last_update)
                days_since_update = (datetime.now() - last_update_time).days
                # 如果超过30天没有更新，建议重新计算（可选策略）
                if days_since_update > 30:
                    return True, f"长时间未更新（{days_since_update}天）"
            except Exception:
                pass
        
        return False, "无需更新"
    
    def _backup_output_file(self, output_file: str) -> str:
        """备份输出文件"""
        if not os.path.exists(output_file):
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = self.output_backup_dir / f"backup_{timestamp}_{Path(output_file).name}"
        
        try:
            shutil.copy2(output_file, backup_file)
            logger.info(f"✅ 备份文件: {backup_file}")
            return str(backup_file)
        except Exception as e:
            logger.error(f"❌ 备份文件失败: {e}")
            return None
    
    def _merge_with_existing_output(self, new_data: pd.DataFrame, output_file: str) -> pd.DataFrame:
        """
        与现有输出文件合并
        基于"Stock X Date X Indicator"维度进行智能合并
        
        Parameters:
        -----------
        new_data : pd.DataFrame
            新计算的数据
        output_file : str
            现有输出文件路径
            
        Returns:
        --------
        pd.DataFrame: 合并后的数据
        """
        if not os.path.exists(output_file):
            return new_data
        
        try:
            # 读取现有数据：先读取字段名，然后跳过中文标签行读取数据
            # 第一步：读取字段名
            with open(output_file, 'r', encoding='utf-8-sig') as f:
                header_line = f.readline().strip()
                column_names = header_line.split(',')
            
            # 第二步：读取数据（跳过字段名和中文标签行）
            existing_data = pd.read_csv(output_file, skiprows=2, names=column_names, low_memory=False)
            
            logger.info(f"📋 现有数据: {len(existing_data)} 行")
            logger.info(f"📋 新数据: {len(new_data)} 行")
            
            # 确保必要的列存在
            required_columns = ['Symbol', 'Date']
            missing_columns = []
            for col in required_columns:
                if col not in existing_data.columns:
                    missing_columns.append(f"现有数据缺少: {col}")
                if col not in new_data.columns:
                    missing_columns.append(f"新数据缺少: {col}")
            
            if missing_columns:
                logger.error(f"❌ 缺少必要列: {', '.join(missing_columns)}")
                logger.info(f"📋 现有数据列: {list(existing_data.columns[:10])}...")
                logger.info(f"📋 新数据列: {list(new_data.columns[:10])}...")
                # 如果合并失败，保留现有数据而不是覆盖
                logger.warning("🔄 合并失败，保留现有数据")
                return existing_data
            
            # 创建复合键：股票+日期（基于"Stock X Date X Indicator"维度）
            existing_data['composite_key'] = existing_data['Symbol'].astype(str) + '_' + existing_data['Date'].astype(str)
            new_data['composite_key'] = new_data['Symbol'].astype(str) + '_' + new_data['Date'].astype(str)
            
            # 统计重复记录
            duplicate_keys = set(existing_data['composite_key']) & set(new_data['composite_key'])
            logger.info(f"🔄 发现重复记录: {len(duplicate_keys)} 条")
            
            # 移除现有数据中的重复记录（新数据优先）
            existing_data = existing_data[~existing_data['composite_key'].isin(new_data['composite_key'])]
            
            # 合并数据
            combined_data = pd.concat([existing_data.drop('composite_key', axis=1), 
                                     new_data.drop('composite_key', axis=1)], 
                                    ignore_index=True, sort=False)
            
            # 按日期和股票代码排序
            if 'Date' in combined_data.columns and 'Symbol' in combined_data.columns:
                combined_data = combined_data.sort_values(['Date', 'Symbol']).reset_index(drop=True)
            
            logger.info(f"✅ 合并完成: 新增 {len(new_data)} 行，保留 {len(existing_data)} 行，总计 {len(combined_data)} 行")
            
            return combined_data
            
        except Exception as e:
            logger.error(f"❌ 合并数据失败: {e}")
            logger.warning("🔄 合并失败，尝试读取现有数据")
            try:
                if os.path.exists(output_file):
                    # 正确读取带有列名的现有数据
                    with open(output_file, 'r', encoding='utf-8-sig') as f:
                        header_line = f.readline().strip()
                        column_names = header_line.split(',')
                    existing_data = pd.read_csv(output_file, skiprows=2, names=column_names, low_memory=False)
                    if not existing_data.empty:
                        logger.info(f"✅ 成功读取现有数据: {len(existing_data)} 行")
                        return existing_data
                    else:
                        logger.warning("⚠️ 现有文件为空")
                        return new_data if not new_data.empty else pd.DataFrame()
                else:
                    logger.warning("⚠️ 现有文件不存在")
                    return new_data if not new_data.empty else pd.DataFrame()
            except Exception as read_error:
                logger.error(f"❌ 读取现有数据也失败: {read_error}")
                return new_data if not new_data.empty else pd.DataFrame()
    
    def _load_financial_data(self):
        """加载财务数据到内存缓存"""
        try:
            if self.financial_data_dir is None:
                logger.warning("未指定财务数据目录，将使用估算值")
                return
                
            logger.info(f"正在加载财务数据从: {self.financial_data_dir}")
            
            # 加载各种财务数据
            data_types = ['info', 'financials', 'balance_sheet', 'cashflow', 'dividends', 'financial_ratios']
            
            for data_type in data_types:
                data_path = self.financial_data_dir / data_type
                if data_path.exists():
                    self._financial_data_cache[data_type] = {}
                    
                    # 读取CSV文件
                    csv_files = list(data_path.glob("*.csv"))
                    if csv_files:
                        for csv_file in csv_files:
                            symbol = csv_file.stem.upper()
                            try:
                                df = pd.read_csv(csv_file, index_col=0)
                                self._financial_data_cache[data_type][symbol] = df
                            except Exception as e:
                                logger.warning(f"Failed to load {data_type} for {symbol}: {e}")
                        
                        logger.info(f"✅ 加载 {data_type} 数据: {len(self._financial_data_cache[data_type])} 只股票")
                    else:
                        logger.warning(f"📁 {data_type} 目录为空")
                else:
                    logger.warning(f"📁 财务数据目录不存在: {data_path}")
                    
        except Exception as e:
            logger.error(f"加载财务数据失败: {e}")
            self._financial_data_cache = {}
    
    def read_qlib_binary_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """读取Qlib二进制数据"""
        symbol_dir = self.features_dir / symbol.lower()
        
        if not symbol_dir.exists():
            return None
        
        features = ['open', 'high', 'low', 'close', 'volume']
        data_dict = {}
        
        try:
            for feature in features:
                bin_file = symbol_dir / f"{feature}.day.bin"
                if bin_file.exists():
                    with open(bin_file, 'rb') as f:
                        values = []
                        while True:
                            data = f.read(4)
                            if not data:
                                break
                            value = struct.unpack('<f', data)[0]
                            if not np.isnan(value) and not np.isinf(value):
                                values.append(value)
                            else:
                                values.append(0.0)
                        data_dict[feature.title()] = values
            
            if not data_dict:
                return None
            
            # Ensure all arrays have the same length
            lengths = [len(v) for v in data_dict.values()]
            if len(set(lengths)) > 1:
                min_length = min(lengths)
                for key in data_dict:
                    data_dict[key] = data_dict[key][:min_length]
            
            df = pd.DataFrame(data_dict)
            
            # Read actual calendar dates from qlib
            calendar_file = self.data_dir / "calendars" / "day.txt"
            if calendar_file.exists():
                with open(calendar_file, 'r') as f:
                    calendar_dates = [line.strip() for line in f.readlines()]
                calendar_dates = [pd.to_datetime(date) for date in calendar_dates if date.strip()]
                
                # The data in qlib is typically aligned with the calendar in reverse order
                if len(df) <= len(calendar_dates):
                    dates = calendar_dates[-len(df):]
                else:
                    # If data is longer than calendar, extend backwards
                    latest_date = calendar_dates[-1] if calendar_dates else pd.to_datetime('2025-06-27')
                    all_dates = pd.bdate_range(end=latest_date, periods=len(df), freq='B')
                    dates = all_dates.tolist()
            else:
                # Fallback: generate business days ending at a reasonable date
                end_date = pd.to_datetime('2025-06-27')
                dates = pd.bdate_range(end=end_date, periods=len(df), freq='B')
            
            df.index = pd.DatetimeIndex(dates)
            
            # 应用时间窗口过滤
            df = self._apply_time_window_filter(df)
            
            return df
            
        except Exception as e:
            logger.warning(f"Failed to read binary data {symbol}: {e}")
            return None
    
    def get_available_stocks(self) -> List[str]:
        """获取可用股票列表"""
        stocks = []
        
        if not self.features_dir.exists():
            logger.error(f"Features directory does not exist: {self.features_dir}")
            return stocks
        
        for stock_dir in self.features_dir.iterdir():
            if stock_dir.is_dir():
                required_files = ['open.day.bin', 'high.day.bin', 'low.day.bin', 'close.day.bin', 'volume.day.bin']
                if all((stock_dir / file).exists() for file in required_files):
                    symbol = stock_dir.name.upper()
                    stocks.append(symbol)
        
        logger.info(f"Found {len(stocks)} stocks data")
        return sorted(stocks)
    
    def get_financial_data(self, symbol: str, data_type: str) -> Optional[pd.DataFrame]:
        """获取财务数据"""
        try:
            # 尝试多种符号格式
            symbol_variants = [
                symbol,                        # 原始符号
                symbol.replace('_', '.'),      # 下划线转点号 (0002_HK -> 0002.HK)
                symbol.replace('.', '_'),      # 点号转下划线 (0002.HK -> 0002_HK)
                symbol.upper(),                # 大写
                symbol.replace('_HK', '.HK'),  # 特定转换
                symbol.replace('.HK', '_HK')   # 特定转换
            ]
            
            if data_type in self._financial_data_cache:
                for variant in symbol_variants:
                    if variant in self._financial_data_cache[data_type]:
                        return self._financial_data_cache[data_type][variant]
            return None
        except Exception as e:
            logger.warning(f"Failed to get financial data for {symbol}, {data_type}: {e}")
            return None
    
    def _safe_divide(self, a, b, fill_value=0.0):
        """安全除法操作，避免除零错误"""
        return np.where(np.abs(b) > 1e-12, a / b, fill_value)
    
    def _get_calculated_indicators(self):
        """获取线程本地的指标集合"""
        if not hasattr(self._local, 'calculated_indicators'):
            self._local.calculated_indicators = set()
        return self._local.calculated_indicators
    
    def _add_indicator(self, indicators: dict, name: str, values):
        """添加指标到字典中，避免重复（线程安全）"""
        calculated_indicators = self._get_calculated_indicators()
        if name not in calculated_indicators:
            indicators[name] = values
            calculated_indicators.add(name)
        else:
            logger.debug(f"指标 {name} 已存在，跳过重复计算")
    
    def _reset_indicators_cache(self):
        """重置线程本地的指标缓存"""
        if hasattr(self._local, 'calculated_indicators'):
            self._local.calculated_indicators.clear()
    
    def calculate_all_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算所有技术指标（共约60个）"""
        if data.empty or len(data) < 50:
            logger.warning("Insufficient data for calculating technical indicators")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # Clean data and convert to numpy arrays for talib
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 1. Moving Averages (移动平均线类) - 12个
            self._add_indicator(indicators, 'SMA_5', talib.SMA(close, timeperiod=5))
            self._add_indicator(indicators, 'SMA_10', talib.SMA(close, timeperiod=10))
            self._add_indicator(indicators, 'SMA_20', talib.SMA(close, timeperiod=20))
            self._add_indicator(indicators, 'SMA_50', talib.SMA(close, timeperiod=50))
            
            self._add_indicator(indicators, 'EMA_5', talib.EMA(close, timeperiod=5))
            self._add_indicator(indicators, 'EMA_10', talib.EMA(close, timeperiod=10))
            self._add_indicator(indicators, 'EMA_20', talib.EMA(close, timeperiod=20))
            self._add_indicator(indicators, 'EMA_50', talib.EMA(close, timeperiod=50))
            
            self._add_indicator(indicators, 'DEMA_20', talib.DEMA(close, timeperiod=20))
            self._add_indicator(indicators, 'TEMA_20', talib.TEMA(close, timeperiod=20))
            self._add_indicator(indicators, 'KAMA_30', talib.KAMA(close, timeperiod=30))
            self._add_indicator(indicators, 'WMA_20', talib.WMA(close, timeperiod=20))
            
            # 2. MACD Family - 3个
            indicators['MACD'], indicators['MACD_Signal'], indicators['MACD_Histogram'] = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
            indicators['MACDEXT'], _, _ = talib.MACDEXT(close, fastperiod=12, slowperiod=26, signalperiod=9)
            indicators['MACDFIX'], _, _ = talib.MACDFIX(close, signalperiod=9)
            
            # 3. Momentum Oscillators (动量振荡器) - 6个
            indicators['RSI_14'] = talib.RSI(close, timeperiod=14)
            indicators['CCI_14'] = talib.CCI(high, low, close, timeperiod=14)
            indicators['CMO_14'] = talib.CMO(close, timeperiod=14)
            indicators['MFI_14'] = talib.MFI(high, low, close, volume, timeperiod=14)
            indicators['WILLR_14'] = talib.WILLR(high, low, close, timeperiod=14)
            indicators['ULTOSC'] = talib.ULTOSC(high, low, close, timeperiod1=7, timeperiod2=14, timeperiod3=28)
            
            # 4. Trend Indicators (趋势指标) - 13个
            indicators['ADX_14'] = talib.ADX(high, low, close, timeperiod=14)
            indicators['ADXR_14'] = talib.ADXR(high, low, close, timeperiod=14)
            indicators['APO'] = talib.APO(close, fastperiod=12, slowperiod=26)
            indicators['AROON_DOWN'], indicators['AROON_UP'] = talib.AROON(high, low, timeperiod=14)
            indicators['AROONOSC_14'] = talib.AROONOSC(high, low, timeperiod=14)
            indicators['BOP'] = talib.BOP(open_price, high, low, close)
            indicators['DX_14'] = talib.DX(high, low, close, timeperiod=14)
            indicators['MINUS_DI_14'] = talib.MINUS_DI(high, low, close, timeperiod=14)
            indicators['MINUS_DM_14'] = talib.MINUS_DM(high, low, timeperiod=14)
            indicators['PLUS_DI_14'] = talib.PLUS_DI(high, low, close, timeperiod=14)
            indicators['PLUS_DM_14'] = talib.PLUS_DM(high, low, timeperiod=14)
            indicators['PPO'] = talib.PPO(close, fastperiod=12, slowperiod=26)
            indicators['TRIX_30'] = talib.TRIX(close, timeperiod=30)
            
            # 5. Momentum Indicators (动量指标) - 5个
            indicators['MOM_10'] = talib.MOM(close, timeperiod=10)
            indicators['ROC_10'] = talib.ROC(close, timeperiod=10)
            indicators['ROCP_10'] = talib.ROCP(close, timeperiod=10)
            indicators['ROCR_10'] = talib.ROCR(close, timeperiod=10)
            indicators['ROCR100_10'] = talib.ROCR100(close, timeperiod=10)
            
            # 6. Bollinger Bands - 3个
            indicators['BB_Upper'], indicators['BB_Middle'], indicators['BB_Lower'] = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
            
            # 7. Stochastic (随机指标) - 6个
            indicators['STOCH_K'], indicators['STOCH_D'] = talib.STOCH(high, low, close, fastk_period=14, slowk_period=3, slowd_period=3)
            indicators['STOCHF_K'], indicators['STOCHF_D'] = talib.STOCHF(high, low, close, fastk_period=14, fastd_period=3)
            indicators['STOCHRSI_K'], indicators['STOCHRSI_D'] = talib.STOCHRSI(close, timeperiod=14, fastk_period=5, fastd_period=3)
            
            # 8. Volatility Indicators (波动率指标) - 3个
            indicators['ATR_14'] = talib.ATR(high, low, close, timeperiod=14)
            indicators['NATR_14'] = talib.NATR(high, low, close, timeperiod=14)
            indicators['TRANGE'] = talib.TRANGE(high, low, close)
            
            # 9. Volume Indicators (成交量指标) - 3个
            indicators['OBV'] = talib.OBV(close, volume)
            indicators['AD'] = talib.AD(high, low, close, volume)
            indicators['ADOSC'] = talib.ADOSC(high, low, close, volume, fastperiod=3, slowperiod=10)
            
            # 10. Hilbert Transform (希尔伯特变换) - 7个
            indicators['HT_DCPERIOD'] = talib.HT_DCPERIOD(close)
            indicators['HT_DCPHASE'] = talib.HT_DCPHASE(close)
            indicators['HT_INPHASE'], indicators['HT_QUADRATURE'] = talib.HT_PHASOR(close)
            indicators['HT_SINE'], indicators['HT_LEADSINE'] = talib.HT_SINE(close)
            indicators['HT_TRENDMODE'] = talib.HT_TRENDMODE(close)
            indicators['HT_TRENDLINE'] = talib.HT_TRENDLINE(close)
            
            # 11. Math Transform (数学变换) - 8个
            indicators['AVGPRICE'] = talib.AVGPRICE(open_price, high, low, close)
            indicators['MEDPRICE'] = talib.MEDPRICE(high, low)
            indicators['TYPPRICE'] = talib.TYPPRICE(high, low, close)
            indicators['WCLPRICE'] = talib.WCLPRICE(high, low, close)
            indicators['MIDPOINT'] = talib.MIDPOINT(close, timeperiod=14)
            indicators['MIDPRICE'] = talib.MIDPRICE(high, low, timeperiod=14)
            indicators['MAMA'], indicators['FAMA'] = talib.MAMA(close)
            
            # 12. Statistical Functions (统计函数) - 7个
            indicators['LINEARREG'] = talib.LINEARREG(close, timeperiod=14)
            indicators['LINEARREG_ANGLE'] = talib.LINEARREG_ANGLE(close, timeperiod=14)
            indicators['LINEARREG_INTERCEPT'] = talib.LINEARREG_INTERCEPT(close, timeperiod=14)
            indicators['LINEARREG_SLOPE'] = talib.LINEARREG_SLOPE(close, timeperiod=14)
            indicators['STDDEV'] = talib.STDDEV(close, timeperiod=30)
            indicators['TSF'] = talib.TSF(close, timeperiod=14)
            indicators['VAR'] = talib.VAR(close, timeperiod=30)
            
            # 13. Min/Max Functions - 2个
            indicators['MAXINDEX'] = talib.MAXINDEX(close, timeperiod=30)
            indicators['MININDEX'] = talib.MININDEX(close, timeperiod=30)
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了 {len(indicators)} 个技术指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算技术指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_alpha158_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算Alpha158指标体系 (158个指标)
        包括KBAR指标、价格指标、成交量指标、滚动技术指标
        """
        if data.empty or len(data) < 60:
            logger.warning("数据不足以计算Alpha158指标")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # 清理数据
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 计算VWAP
            vwap = self._safe_divide(
                np.convolve(close * volume, np.ones(1), 'same'),
                np.convolve(volume, np.ones(1), 'same')
            )
            
            # 1. KBAR指标 (9个)
            self._add_indicator(indicators, 'ALPHA158_KMID', self._safe_divide(close - open_price, open_price))
            self._add_indicator(indicators, 'ALPHA158_KLEN', self._safe_divide(high - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KMID2', self._safe_divide(close - open_price, high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KUP', self._safe_divide(high - np.maximum(open_price, close), open_price))
            self._add_indicator(indicators, 'ALPHA158_KUP2', self._safe_divide(high - np.maximum(open_price, close), high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KLOW', self._safe_divide(np.minimum(open_price, close) - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KLOW2', self._safe_divide(np.minimum(open_price, close) - low, high - low + 1e-12))
            self._add_indicator(indicators, 'ALPHA158_KSFT', self._safe_divide(2 * close - high - low, open_price))
            self._add_indicator(indicators, 'ALPHA158_KSFT2', self._safe_divide(2 * close - high - low, high - low + 1e-12))
            
            # 2. 价格指标 (标准化到收盘价)
            features = ['OPEN', 'HIGH', 'LOW', 'VWAP']
            for feature in features:
                if feature == 'OPEN':
                    price_values = open_price
                elif feature == 'HIGH':
                    price_values = high
                elif feature == 'LOW':
                    price_values = low
                elif feature == 'VWAP':
                    price_values = vwap
                
                self._add_indicator(indicators, f'ALPHA158_{feature}0', self._safe_divide(price_values, close))
            
            # 3. 成交量指标
            self._add_indicator(indicators, 'ALPHA158_VOLUME0', self._safe_divide(volume, volume + 1e-12))
            
            # 4. 滚动技术指标
            windows = [5, 10, 20, 30, 60]
            
            # ROC - Rate of Change
            for d in windows:
                ref_close = np.roll(close, d)
                self._add_indicator(indicators, f'ALPHA158_ROC{d}', self._safe_divide(ref_close, close))
            
            # MA - Simple Moving Average
            for d in windows:
                ma_values = pd.Series(close).rolling(window=d, min_periods=1).mean().values
                self._add_indicator(indicators, f'ALPHA158_MA{d}', self._safe_divide(ma_values, close))
            
            # STD - Standard Deviation
            for d in windows:
                std_values = pd.Series(close).rolling(window=d, min_periods=1).std().fillna(0).values
                self._add_indicator(indicators, f'ALPHA158_STD{d}', self._safe_divide(std_values, close))
            
            # BETA - Slope
            for d in windows:
                beta_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            slope = np.polyfit(x, y, 1)[0]
                            beta_values[i] = slope
                        except:
                            beta_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_BETA{d}', self._safe_divide(beta_values, close))
            
            # RSQR - R-square
            for d in windows:
                rsqr_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            correlation_matrix = np.corrcoef(x, y)
                            rsqr_values[i] = correlation_matrix[0, 1] ** 2 if not np.isnan(correlation_matrix[0, 1]) else 0
                        except:
                            rsqr_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_RSQR{d}', rsqr_values)
            
            # MAX/MIN
            for d in windows:
                max_values = pd.Series(high).rolling(window=d, min_periods=1).max().values
                min_values = pd.Series(low).rolling(window=d, min_periods=1).min().values
                self._add_indicator(indicators, f'ALPHA158_MAX{d}', self._safe_divide(max_values, close))
                self._add_indicator(indicators, f'ALPHA158_MIN{d}', self._safe_divide(min_values, close))
            
            # QTLU/QTLD - Quantiles
            for d in windows:
                qtlu_values = pd.Series(close).rolling(window=d, min_periods=1).quantile(0.8).values
                qtld_values = pd.Series(close).rolling(window=d, min_periods=1).quantile(0.2).values
                self._add_indicator(indicators, f'ALPHA158_QTLU{d}', self._safe_divide(qtlu_values, close))
                self._add_indicator(indicators, f'ALPHA158_QTLD{d}', self._safe_divide(qtld_values, close))
            
            # RANK - Percentile rank
            for d in windows:
                rank_values = pd.Series(close).rolling(window=d, min_periods=1).rank(pct=True).values
                self._add_indicator(indicators, f'ALPHA158_RANK{d}', rank_values)
            
            # RSV - Relative Strength Value
            for d in windows:
                min_low = pd.Series(low).rolling(window=d, min_periods=1).min().values
                max_high = pd.Series(high).rolling(window=d, min_periods=1).max().values
                self._add_indicator(indicators, f'ALPHA158_RSV{d}', self._safe_divide(close - min_low, max_high - min_low + 1e-12))
            
            # RESI - Linear Regression Residual
            for d in windows:
                resi_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    x = np.arange(d)
                    y = close[i-d+1:i+1]
                    if len(y) > 1:
                        try:
                            slope, intercept = np.polyfit(x, y, 1)
                            predicted = slope * (d-1) + intercept
                            resi_values[i] = y[-1] - predicted
                        except:
                            resi_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_RESI{d}', self._safe_divide(resi_values, close))
            
            # IMAX - Index of Maximum
            for d in windows:
                imax_values = np.zeros_like(close)
                for i in range(d, len(high)):
                    window_high = high[i-d+1:i+1]
                    if len(window_high) > 0:
                        imax_values[i] = (len(window_high) - 1 - np.argmax(window_high)) / d
                self._add_indicator(indicators, f'ALPHA158_IMAX{d}', imax_values)
            
            # IMIN - Index of Minimum  
            for d in windows:
                imin_values = np.zeros_like(close)
                for i in range(d, len(low)):
                    window_low = low[i-d+1:i+1]
                    if len(window_low) > 0:
                        imin_values[i] = (len(window_low) - 1 - np.argmin(window_low)) / d
                self._add_indicator(indicators, f'ALPHA158_IMIN{d}', imin_values)
            
            # IMXD - Index Max - Index Min Difference
            for d in windows:
                imxd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_high = high[i-d+1:i+1] 
                    window_low = low[i-d+1:i+1]
                    if len(window_high) > 0 and len(window_low) > 0:
                        idx_max = len(window_high) - 1 - np.argmax(window_high)
                        idx_min = len(window_low) - 1 - np.argmin(window_low)
                        imxd_values[i] = (idx_max - idx_min) / d
                self._add_indicator(indicators, f'ALPHA158_IMXD{d}', imxd_values)
            
            # CORR - Correlation between close and log(volume)
            for d in windows:
                corr_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_close = close[i-d+1:i+1]
                    window_volume = volume[i-d+1:i+1]
                    if len(window_close) > 1:
                        try:
                            log_volume = np.log(window_volume + 1)
                            if np.std(window_close) > 1e-8 and np.std(log_volume) > 1e-8:
                                corr_values[i] = np.corrcoef(window_close, log_volume)[0, 1]
                        except:
                            corr_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_CORR{d}', corr_values)
            
            # CORD - Correlation between price change and volume change
            for d in windows:
                cord_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    if i >= d:
                        close_change = close[i-d+2:i+1] / close[i-d+1:i]
                        volume_change = np.log((volume[i-d+2:i+1] / (volume[i-d+1:i] + 1e-12)) + 1)
                        if len(close_change) > 1:
                            try:
                                if np.std(close_change) > 1e-8 and np.std(volume_change) > 1e-8:
                                    cord_values[i] = np.corrcoef(close_change, volume_change)[0, 1]
                            except:
                                cord_values[i] = 0
                self._add_indicator(indicators, f'ALPHA158_CORD{d}', cord_values)
            
            # CNTP - Count of Positive returns
            for d in windows:
                cntp_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_returns = close[i-d+2:i+1] > close[i-d+1:i]
                    if len(window_returns) > 0:
                        cntp_values[i] = np.mean(window_returns)
                self._add_indicator(indicators, f'ALPHA158_CNTP{d}', cntp_values)
            
            # CNTN - Count of Negative returns
            for d in windows:
                cntn_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_returns = close[i-d+2:i+1] < close[i-d+1:i]
                    if len(window_returns) > 0:
                        cntn_values[i] = np.mean(window_returns)
                self._add_indicator(indicators, f'ALPHA158_CNTN{d}', cntn_values)
            
            # CNTD - Count Difference (CNTP - CNTN)
            for d in windows:
                cntd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    window_pos = close[i-d+2:i+1] > close[i-d+1:i]
                    window_neg = close[i-d+2:i+1] < close[i-d+1:i]
                    if len(window_pos) > 0:
                        cntd_values[i] = np.mean(window_pos) - np.mean(window_neg)
                self._add_indicator(indicators, f'ALPHA158_CNTD{d}', cntd_values)
            
            # SUMP - Sum of Positive returns ratio
            for d in windows:
                sump_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        positive_sum = np.sum(np.maximum(changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sump_values[i] = self._safe_divide(positive_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMP{d}', sump_values)
            
            # SUMN - Sum of Negative returns ratio  
            for d in windows:
                sumn_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        negative_sum = np.sum(np.maximum(-changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sumn_values[i] = self._safe_divide(negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMN{d}', sumn_values)
            
            # SUMD - Sum Difference (SUMP - SUMN)
            for d in windows:
                sumd_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    changes = close[i-d+2:i+1] - close[i-d+1:i]
                    if len(changes) > 0:
                        positive_sum = np.sum(np.maximum(changes, 0))
                        negative_sum = np.sum(np.maximum(-changes, 0))
                        total_abs_sum = np.sum(np.abs(changes))
                        sumd_values[i] = self._safe_divide(positive_sum - negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_SUMD{d}', sumd_values)
            
            # VMA - Volume Moving Average
            for d in windows:
                vma_values = pd.Series(volume).rolling(window=d, min_periods=1).mean().values
                self._add_indicator(indicators, f'ALPHA158_VMA{d}', self._safe_divide(vma_values, volume + 1e-12))
            
            # VSTD - Volume Standard Deviation
            for d in windows:
                vstd_values = pd.Series(volume).rolling(window=d, min_periods=1).std().fillna(0).values
                self._add_indicator(indicators, f'ALPHA158_VSTD{d}', self._safe_divide(vstd_values, volume + 1e-12))
            
            # WVMA - Weighted Volume Moving Average (price change volatility weighted by volume)
            for d in windows:
                wvma_values = np.zeros_like(close)
                for i in range(d, len(close)):
                    price_changes = np.abs(close[i-d+2:i+1] / close[i-d+1:i] - 1)
                    weights = volume[i-d+2:i+1]
                    if len(price_changes) > 0:
                        weighted_changes = price_changes * weights
                        mean_weighted = np.mean(weighted_changes)
                        std_weighted = np.std(weighted_changes)
                        wvma_values[i] = self._safe_divide(std_weighted, mean_weighted + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_WVMA{d}', wvma_values)
            
            # VSUMP - Volume Sum Positive ratio
            for d in windows:
                vsump_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i] 
                    if len(vol_changes) > 0:
                        positive_sum = np.sum(np.maximum(vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsump_values[i] = self._safe_divide(positive_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMP{d}', vsump_values)
            
            # VSUMN - Volume Sum Negative ratio
            for d in windows:
                vsumn_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i]
                    if len(vol_changes) > 0:
                        negative_sum = np.sum(np.maximum(-vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsumn_values[i] = self._safe_divide(negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMN{d}', vsumn_values)
            
            # VSUMD - Volume Sum Difference (VSUMP - VSUMN)
            for d in windows:
                vsumd_values = np.zeros_like(close)
                for i in range(d, len(volume)):
                    vol_changes = volume[i-d+2:i+1] - volume[i-d+1:i]
                    if len(vol_changes) > 0:
                        positive_sum = np.sum(np.maximum(vol_changes, 0))
                        negative_sum = np.sum(np.maximum(-vol_changes, 0))
                        total_abs_sum = np.sum(np.abs(vol_changes))
                        vsumd_values[i] = self._safe_divide(positive_sum - negative_sum, total_abs_sum + 1e-12)
                self._add_indicator(indicators, f'ALPHA158_VSUMD{d}', vsumd_values)
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了Alpha158指标体系: {len(indicators)} 个指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算Alpha158指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_alpha360_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        计算Alpha360指标体系 (360个指标)
        包括过去60天的标准化价格和成交量数据
        """
        if data.empty or len(data) < 60:
            logger.warning("数据不足以计算Alpha360指标")
            return pd.DataFrame()
        
        try:
            indicators = {}
            
            # 清理数据
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            volume = data['Volume'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(0).values
            
            # 计算VWAP
            vwap = self._safe_divide(
                np.convolve(close * volume, np.ones(1), 'same'),
                np.convolve(volume, np.ones(1), 'same')
            )
            
            # Alpha360: 过去60天的价格和成交量数据，除以当前收盘价标准化
            # 1. CLOSE 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_CLOSE{i}', self._safe_divide(close, close))
                else:
                    ref_close = np.roll(close, i)
                    self._add_indicator(indicators, f'ALPHA360_CLOSE{i}', self._safe_divide(ref_close, close))
            
            # 2. OPEN 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_OPEN{i}', self._safe_divide(open_price, close))
                else:
                    ref_open = np.roll(open_price, i)
                    self._add_indicator(indicators, f'ALPHA360_OPEN{i}', self._safe_divide(ref_open, close))
            
            # 3. HIGH 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_HIGH{i}', self._safe_divide(high, close))
                else:
                    ref_high = np.roll(high, i)
                    self._add_indicator(indicators, f'ALPHA360_HIGH{i}', self._safe_divide(ref_high, close))
            
            # 4. LOW 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_LOW{i}', self._safe_divide(low, close))
                else:
                    ref_low = np.roll(low, i)
                    self._add_indicator(indicators, f'ALPHA360_LOW{i}', self._safe_divide(ref_low, close))
            
            # 5. VWAP 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_VWAP{i}', self._safe_divide(vwap, close))
                else:
                    ref_vwap = np.roll(vwap, i)
                    self._add_indicator(indicators, f'ALPHA360_VWAP{i}', self._safe_divide(ref_vwap, close))
            
            # 6. VOLUME 指标 (60个)
            for i in range(59, -1, -1):
                if i == 0:
                    self._add_indicator(indicators, f'ALPHA360_VOLUME{i}', self._safe_divide(volume, volume + 1e-12))
                else:
                    ref_volume = np.roll(volume, i)
                    self._add_indicator(indicators, f'ALPHA360_VOLUME{i}', self._safe_divide(ref_volume, volume + 1e-12))
            
            # 转换为DataFrame
            indicators_df = pd.DataFrame(indicators, index=data.index)
            
            logger.info(f"计算了Alpha360指标体系: {len(indicators)} 个指标")
            return indicators_df
            
        except Exception as e:
            logger.error(f"计算Alpha360指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_candlestick_patterns(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算蜡烛图形态指标（共61个）"""
        if data.empty or len(data) < 10:
            logger.warning("Insufficient data for calculating candlestick patterns")
            return pd.DataFrame()
        
        try:
            patterns = {}
            
            # Clean data
            open_price = data['Open'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            high = data['High'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            low = data['Low'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            close = data['Close'].astype(float).replace([np.inf, -np.inf], np.nan).fillna(method='ffill').values
            
            # 所有61个蜡烛图形态
            candle_patterns = [
                'CDL2CROWS', 'CDL3BLACKCROWS', 'CDL3INSIDE', 'CDL3LINESTRIKE', 'CDL3OUTSIDE',
                'CDL3STARSINSOUTH', 'CDL3WHITESOLDIERS', 'CDLABANDONEDBABY', 'CDLADVANCEBLOCK',
                'CDLBELTHOLD', 'CDLBREAKAWAY', 'CDLCLOSINGMARUBOZU', 'CDLCONCEALBABYSWALL',
                'CDLCOUNTERATTACK', 'CDLDARKCLOUDCOVER', 'CDLDOJI', 'CDLDOJISTAR', 'CDLDRAGONFLYDOJI',
                'CDLENGULFING', 'CDLEVENINGDOJISTAR', 'CDLEVENINGSTAR', 'CDLGAPSIDESIDEWHITE',
                'CDLGRAVESTONEDOJI', 'CDLHAMMER', 'CDLHANGINGMAN', 'CDLHARAMI', 'CDLHARAMICROSS',
                'CDLHIGHWAVE', 'CDLHIKKAKE', 'CDLHIKKAKEMOD', 'CDLHOMINGPIGEON', 'CDLIDENTICAL3CROWS',
                'CDLINNECK', 'CDLINVERTEDHAMMER', 'CDLKICKING', 'CDLKICKINGBYLENGTH', 'CDLLADDERBOTTOM',
                'CDLLONGLEGGEDDOJI', 'CDLLONGLINE', 'CDLMARUBOZU', 'CDLMATCHINGLOW', 'CDLMATHOLD',
                'CDLMORNINGDOJISTAR', 'CDLMORNINGSTAR', 'CDLONNECK', 'CDLPIERCING', 'CDLRICKSHAWMAN',
                'CDLRISEFALL3METHODS', 'CDLSEPARATINGLINES', 'CDLSHOOTINGSTAR', 'CDLSHORTLINE',
                'CDLSPINNINGTOP', 'CDLSTALLEDPATTERN', 'CDLSTICKSANDWICH', 'CDLTAKURI', 'CDLTASUKIGAP',
                'CDLTHRUSTING', 'CDLTRISTAR', 'CDLUNIQUE3RIVER', 'CDLUPSIDEGAP2CROWS', 'CDLXSIDEGAP3METHODS'
            ]
            
            for pattern in candle_patterns:
                try:
                    patterns[pattern] = getattr(talib, pattern)(open_price, high, low, close)
                except Exception as e:
                    logger.warning(f"Failed to calculate {pattern}: {e}")
                    patterns[pattern] = np.zeros(len(data))
            
            patterns_df = pd.DataFrame(patterns, index=data.index)
            
            logger.info(f"计算了 {len(patterns)} 个蜡烛图形态指标")
            return patterns_df
            
        except Exception as e:
            logger.error(f"计算蜡烛图形态失败: {e}")
            return pd.DataFrame()
    
    def calculate_financial_indicators(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """计算财务指标和换手率（约15个）- 使用估算值替代缺失数据"""
        try:
            result_data = data.copy()
            
            # 预先初始化所有财务指标列
            financial_columns = [
                'PriceToBookRatio', 'MarketCap', 'PERatio', 'PriceToSalesRatio',
                'ROE', 'ROA', 'ProfitMargins', 'CurrentRatio', 'QuickRatio', 
                'DebtToEquity', 'TobinsQ', 'DailyTurnover', 
                'turnover_c1d', 'turnover_c5d', 'turnover_c10d', 'turnover_c20d', 'turnover_c30d',
                'turnover_m5d', 'turnover_m10d', 'turnover_m20d', 'turnover_m30d'
            ]
            
            # 获取基本信息数据
            info_data = self.get_financial_data(symbol, 'info')
            balance_sheet_data = self.get_financial_data(symbol, 'balance_sheet')
            
            # 如果有真实财务数据，使用真实数据
            if info_data is not None and not info_data.empty:
                result_data = self._calculate_real_financial_indicators(result_data, info_data, balance_sheet_data)
            else:
                # 否则使用基于价格和成交量的估算指标
                result_data = self._calculate_estimated_financial_indicators(result_data, symbol)
            
            # 确保所有财务指标列都存在且有默认值
            result_data = self._ensure_financial_columns_exist(result_data, symbol)
            
            logger.info(f"✅ 完成财务指标计算 (包含估算值)")
            return result_data
            
        except Exception as e:
            logger.error(f"计算财务指标失败: {e}")
            # 即使失败也要确保列存在
            for col in financial_columns:
                if col not in data.columns:
                    data[col] = np.nan
            return data
    
    def _calculate_real_financial_indicators(self, data: pd.DataFrame, info_data: pd.DataFrame, balance_sheet_data: pd.DataFrame) -> pd.DataFrame:
        """使用真实财务数据计算指标"""
        result_data = data.copy()
        
        try:
            # 获取财务数据的第一行（通常包含最新数据）
            info_row = info_data.iloc[0] if not info_data.empty else pd.Series()
            
            # 1. 市净率 (Price to Book Ratio)
            if 'priceToBook' in info_row.index and not pd.isna(info_row['priceToBook']):
                # 直接使用已计算的市净率
                result_data['PriceToBookRatio'] = float(info_row['priceToBook'])
            elif 'bookValue' in info_row.index and not pd.isna(info_row['bookValue']):
                book_value = float(info_row['bookValue'])
                if book_value > 0:
                    result_data['PriceToBookRatio'] = result_data['Close'] / book_value
            
            # 2. 市值 (Market Cap)
            if 'marketCap' in info_row.index and not pd.isna(info_row['marketCap']):
                result_data['MarketCap'] = float(info_row['marketCap'])
            elif 'sharesOutstanding' in info_row.index and not pd.isna(info_row['sharesOutstanding']):
                shares_outstanding = float(info_row['sharesOutstanding'])
                if shares_outstanding > 0:
                    result_data['MarketCap'] = result_data['Close'] * shares_outstanding
            
            # 3. 市盈率 (PE Ratio)
            if 'trailingPE' in info_row.index and not pd.isna(info_row['trailingPE']):
                result_data['PERatio'] = float(info_row['trailingPE'])
            elif 'forwardPE' in info_row.index and not pd.isna(info_row['forwardPE']):
                result_data['PERatio'] = float(info_row['forwardPE'])
            
            # 4. 市销率 (Price to Sales Ratio)
            if 'priceToSalesTrailing12Months' in info_row.index and not pd.isna(info_row['priceToSalesTrailing12Months']):
                result_data['PriceToSalesRatio'] = float(info_row['priceToSalesTrailing12Months'])
            
            # 5. 净资产收益率 (ROE)
            if 'returnOnEquity' in info_row.index and not pd.isna(info_row['returnOnEquity']):
                result_data['ROE'] = float(info_row['returnOnEquity'])
            
            # 6. 资产收益率 (ROA)
            if 'returnOnAssets' in info_row.index and not pd.isna(info_row['returnOnAssets']):
                result_data['ROA'] = float(info_row['returnOnAssets'])
            
            # 7. 利润率
            if 'profitMargins' in info_row.index and not pd.isna(info_row['profitMargins']):
                result_data['ProfitMargins'] = float(info_row['profitMargins'])
            
            # 8. 流动比率 (如果有资产负债表数据)
            if balance_sheet_data is not None and not balance_sheet_data.empty:
                balance_row = balance_sheet_data.iloc[0]
                if 'currentRatio' in balance_row.index and not pd.isna(balance_row['currentRatio']):
                    result_data['CurrentRatio'] = float(balance_row['currentRatio'])
                elif 'Total Current Assets' in balance_row.index and 'Total Current Liabilities' in balance_row.index:
                    current_assets = balance_row.get('Total Current Assets', 0)
                    current_liabilities = balance_row.get('Total Current Liabilities', 1)
                    if current_liabilities > 0:
                        result_data['CurrentRatio'] = current_assets / current_liabilities
            
            # 9. 速动比率
            if 'quickRatio' in info_row.index and not pd.isna(info_row['quickRatio']):
                result_data['QuickRatio'] = float(info_row['quickRatio'])
            else:
                # 估算为流动比率的80%
                if 'CurrentRatio' in result_data.columns:
                    result_data['QuickRatio'] = result_data['CurrentRatio'] * 0.8
            
            # 10. 资产负债率
            if 'debtToEquity' in info_row.index and not pd.isna(info_row['debtToEquity']):
                result_data['DebtToEquity'] = float(info_row['debtToEquity'])
            elif 'totalDebt' in info_row.index and 'marketCap' in info_row.index:
                total_debt = info_row.get('totalDebt', 0)
                market_cap = info_row.get('marketCap', 1)
                if market_cap > 0:
                    result_data['DebtToEquity'] = total_debt / market_cap
            
            # 11. 托宾Q值
            if 'enterpriseValue' in info_row.index and balance_sheet_data is not None:
                enterprise_value = info_row.get('enterpriseValue', None)
                if enterprise_value and not balance_sheet_data.empty:
                    balance_row = balance_sheet_data.iloc[0]
                    total_assets = balance_row.get('Total Assets', balance_row.get('totalAssets', None))
                    if total_assets and total_assets > 0:
                        result_data['TobinsQ'] = enterprise_value / total_assets
            
            # 12. 换手率计算
            if 'floatShares' in info_row.index and not pd.isna(info_row['floatShares']):
                float_shares = float(info_row['floatShares'])
                if float_shares > 0:
                    result_data = self._calculate_real_turnover_indicators(result_data, float_shares)
            elif 'sharesOutstanding' in info_row.index and not pd.isna(info_row['sharesOutstanding']):
                # 使用总股本作为流通股的替代
                shares_outstanding = float(info_row['sharesOutstanding'])
                if shares_outstanding > 0:
                    result_data = self._calculate_real_turnover_indicators(result_data, shares_outstanding)
            
            logger.info("✅ 使用真实财务数据计算完成")
            
        except Exception as e:
            logger.error(f"使用真实财务数据计算失败: {e}")
            # 如果真实数据计算失败，回退到估算方法
            result_data = self._calculate_estimated_financial_indicators(result_data, "UNKNOWN")
        
        return result_data
    
    def _calculate_real_turnover_indicators(self, data: pd.DataFrame, shares_count: float) -> pd.DataFrame:
        """基于真实流通股数计算换手率指标"""
        try:
            result_data = data.copy()
            
            # 计算日换手率
            result_data['DailyTurnover'] = result_data['Volume'] / shares_count
            
            # 计算不同窗口的累计和平均换手率
            windows = [1, 5, 10, 20, 30]
            for window in windows:
                result_data[f'turnover_c{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                if window > 1:
                    result_data[f'turnover_m{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
            
            return result_data
            
        except Exception as e:
            logger.error(f"计算真实换手率指标失败: {e}")
            return data
    
    def _calculate_estimated_financial_indicators(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """基于价格和成交量数据估算财务指标"""
        result_data = data.copy()
        
        # 获取基础数据
        close = result_data['Close'].values
        volume = result_data['Volume'].values
        high = result_data['High'].values
        low = result_data['Low'].values
        
        # 1. 估算市值 (假设流通股为平均成交量的某个倍数)
        avg_volume = np.mean(volume[volume > 0]) if len(volume[volume > 0]) > 0 else 1000000
        estimated_shares = avg_volume * 50  # 假设平均成交量是流通股的1/50
        result_data['MarketCap'] = close * estimated_shares
        
        # 2. 估算市净率 (基于价格波动性，高波动性通常对应高PB)
        price_volatility = pd.Series(close).rolling(20).std().fillna(0) / pd.Series(close).rolling(20).mean().fillna(1)
        result_data['PriceToBookRatio'] = 1.0 + price_volatility * 3  # 基准1倍，波动性每增加1，PB增加3
        
        # 3. 估算市盈率 (基于价格趋势，上涨趋势对应高PE)
        price_trend = pd.Series(close).rolling(20).apply(lambda x: np.polyfit(range(len(x)), x, 1)[0] if len(x) > 1 else 0).fillna(0)
        base_pe = 15  # 基准PE
        result_data['PERatio'] = base_pe + (price_trend / np.mean(close) * 1000)
        result_data['PERatio'] = np.clip(result_data['PERatio'], 5, 50)  # 限制在合理范围
        
        # 4. 估算市销率 (基于成交量活跃度)
        volume_series = pd.Series(volume, index=result_data.index)
        volume_activity = volume_series.rolling(20).mean().fillna(avg_volume) / avg_volume
        result_data['PriceToSalesRatio'] = 1.0 + volume_activity * 2  # 成交活跃对应高PS
        
        # 5. 估算ROE (基于收益率)
        returns = pd.Series(close).pct_change(20).fillna(0)
        result_data['ROE'] = np.clip(returns * 4, -0.3, 0.5)  # 年化收益率作为ROE的代理
        
        # 6. 估算ROA (通常比ROE低)
        result_data['ROA'] = result_data['ROE'] * 0.6
        
        # 7. 估算利润率 (基于价格稳定性)
        price_stability = 1 / (1 + price_volatility)
        result_data['ProfitMargins'] = price_stability * 0.1  # 稳定的股票假设有更好的利润率
        
        # 8. 估算流动比率 (基于成交量流动性)
        volume_series = pd.Series(volume, index=result_data.index)
        liquidity = volume_series.rolling(5).mean() / volume_series.rolling(20).mean()
        result_data['CurrentRatio'] = 1.0 + liquidity.fillna(1) * 0.5
        
        # 9. 估算速动比率 (通常比流动比率低)
        result_data['QuickRatio'] = result_data['CurrentRatio'] * 0.8
        
        # 10. 估算资产负债率 (基于波动性，高波动可能意味着高杠杆)
        result_data['DebtToEquity'] = price_volatility * 2
        
        # 11. 估算托宾Q值
        market_value_ratio = (high + low) / (2 * close)  # 相对估值
        market_value_ratio = pd.Series(market_value_ratio, index=result_data.index).fillna(1)
        result_data['TobinsQ'] = market_value_ratio
        
        # 12. 估算换手率指标
        result_data = self._calculate_estimated_turnover_indicators(result_data, estimated_shares)
        
        logger.info(f"🔮 使用估算方法计算财务指标 (基于价格和成交量)")
        return result_data
    
    def _calculate_estimated_turnover_indicators(self, data: pd.DataFrame, estimated_shares: float) -> pd.DataFrame:
        """基于估算流通股计算换手率指标"""
        try:
            result_data = data.copy()
            
            # 计算日换手率
            result_data['DailyTurnover'] = result_data['Volume'] / estimated_shares
            
            # 计算不同窗口的累计和平均换手率
            windows = [1, 5, 10, 20, 30]
            for window in windows:
                result_data[f'turnover_c{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                if window > 1:
                    result_data[f'turnover_m{window}d'] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
            
            logger.info("✅ 完成换手率指标估算")
            return result_data
            
        except Exception as e:
            logger.error(f"估算换手率指标失败: {e}")
            return data
    
    def _ensure_financial_columns_exist(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """确保所有财务指标列都存在且有合理的默认值"""
        result_data = data.copy()
        
        # 定义所有必需的财务指标列和其默认值
        required_columns = {
            'PriceToBookRatio': 1.5,    # 默认市净率
            'MarketCap': None,          # 市值需要计算
            'PERatio': 15.0,            # 默认市盈率
            'PriceToSalesRatio': 2.0,   # 默认市销率
            'ROE': 0.1,                 # 默认10%的ROE
            'ROA': 0.05,                # 默认5%的ROA
            'ProfitMargins': 0.08,      # 默认8%的利润率
            'CurrentRatio': 1.2,        # 默认流动比率
            'QuickRatio': 1.0,          # 默认速动比率
            'DebtToEquity': 0.5,        # 默认资产负债率
            'TobinsQ': 1.0,             # 默认托宾Q值
            'DailyTurnover': None,      # 需要计算
            'turnover_c1d': None,       # 需要计算
            'turnover_c5d': None,       # 需要计算
            'turnover_c10d': None,      # 需要计算
            'turnover_c20d': None,      # 需要计算
            'turnover_c30d': None,      # 需要计算
            'turnover_m5d': None,       # 需要计算
            'turnover_m10d': None,      # 需要计算
            'turnover_m20d': None,      # 需要计算
            'turnover_m30d': None       # 需要计算
        }
        
        # 为缺失的列添加默认值
        for col_name, default_value in required_columns.items():
            # 检查列是否不存在或全部为NaN
            col_missing = col_name not in result_data.columns 
            col_all_nan = not col_missing and result_data[col_name].isna().all()
            col_all_zero = not col_missing and (result_data[col_name] == 0).all()
            
            # 对于换手率指标，额外检查是否全部为0（这通常表示计算有问题）
            needs_calculation = col_missing or col_all_nan or (col_name.startswith('turnover') and col_all_zero)
            
            if needs_calculation:
                if default_value is not None:
                    result_data[col_name] = default_value
                elif col_name == 'MarketCap':
                    # 估算市值：假设平均股价为当前股价，流通股为成交量的50倍
                    avg_volume = result_data['Volume'].mean() if 'Volume' in result_data.columns else 1000000
                    estimated_shares = avg_volume * 50
                    result_data['MarketCap'] = result_data['Close'] * estimated_shares
                elif col_name.startswith('turnover'):
                    # 只有在换手率指标确实缺失或有问题时才重新计算
                    logger.warning(f"换手率指标 {col_name} 缺失或异常，使用估算方法")
                    
                    # 估算换手率相关指标
                    if 'DailyTurnover' not in result_data.columns or result_data['DailyTurnover'].isna().all() or (result_data['DailyTurnover'] == 0).all():
                        avg_volume = result_data['Volume'].mean() if 'Volume' in result_data.columns else 1000000
                        estimated_shares = avg_volume * 50
                        result_data['DailyTurnover'] = result_data['Volume'] / estimated_shares
                    
                    # 计算累计和平均换手率
                    if col_name.startswith('turnover_c'):
                        window = int(col_name.split('d')[0].split('_c')[1])
                        result_data[col_name] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).sum()
                    elif col_name.startswith('turnover_m'):
                        window = int(col_name.split('d')[0].split('_m')[1])
                        result_data[col_name] = result_data['DailyTurnover'].rolling(window=window, min_periods=1).mean()
        
        return result_data
    

    
    def calculate_volatility_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算波动率指标（约8个）"""
        try:
            volatility_data = {}
            
            # 计算价格变化
            price_diff = data['Close'].diff()
            log_returns = np.log(data['Close'] / data['Close'].shift(1))
            
            # 1. 已实现波动率 (20天窗口)
            volatility_data['RealizedVolatility_20'] = price_diff.rolling(window=20).std() * np.sqrt(252)
            
            # 2. 已实现负半变差
            negative_returns = price_diff[price_diff < 0]
            volatility_data['NegativeSemiDeviation_20'] = negative_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 3. 已实现连续波动率
            volatility_data['ContinuousVolatility_20'] = log_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 4. 已实现正半变差
            positive_returns = price_diff[price_diff > 0]
            volatility_data['PositiveSemiDeviation_20'] = positive_returns.rolling(window=20).std() * np.sqrt(252)
            
            # 5. 不同窗口的波动率
            for window in [10, 30, 60]:
                volatility_data[f'Volatility_{window}'] = price_diff.rolling(window=window).std() * np.sqrt(252)
            
            volatility_df = pd.DataFrame(volatility_data, index=data.index)
            
            logger.info("计算了波动率指标")
            return volatility_df
            
        except Exception as e:
            logger.error(f"计算波动率指标失败: {e}")
            return pd.DataFrame()
    
    def calculate_all_indicators_for_stock(self, symbol: str, incremental_start_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        为单只股票计算所有指标（支持增量计算）
        
        Parameters:
        -----------
        symbol : str
            股票代码
        incremental_start_date : Optional[str]
            增量计算的开始日期，如果提供则只计算该日期之后的数据
        """
        try:
            # 读取历史价格数据
            price_data = self.read_qlib_binary_data(symbol)
            if price_data is None or price_data.empty:
                logger.warning(f"No price data found for {symbol}")
                return None
            
            # 如果指定了增量开始日期，只计算该日期之后的数据
            if incremental_start_date:
                start_date = pd.to_datetime(incremental_start_date)
                price_data = price_data[price_data.index >= start_date]
                if price_data.empty:
                    logger.info(f"{symbol}: 增量日期 {incremental_start_date} 之后没有新数据")
                    return None
                logger.info(f"{symbol}: 增量计算 {incremental_start_date} 之后的数据 ({len(price_data)} 行)")
            
            # 使用并行计算或顺序计算
            if self.enable_parallel:
                return self._calculate_indicators_parallel(symbol, price_data)
            else:
                return self._calculate_indicators_sequential(symbol, price_data)
                
        except Exception as e:
            logger.error(f"❌ {symbol}: 计算指标失败 - {e}")
            return None
    
    def _calculate_indicators_parallel(self, symbol: str, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        并行计算单只股票的所有指标类型
        """
        if not self.enable_parallel:
            return self._calculate_indicators_sequential(symbol, price_data)
        
        try:
            logger.info(f"开始并行计算 {symbol} 的所有指标...")
            start_time = time.time()
            
            # 重置指标缓存
            self._reset_indicators_cache()
            
            # 保存原始日期信息
            original_dates = price_data.index
            
            # 定义各类指标计算任务
            indicator_tasks = [
                ('Alpha158', partial(self.calculate_alpha158_indicators, price_data)),
                ('Alpha360', partial(self.calculate_alpha360_indicators, price_data)),
                ('Technical', partial(self.calculate_all_technical_indicators, price_data)),
                ('Candlestick', partial(self.calculate_candlestick_patterns, price_data)),
                ('Financial', partial(self.calculate_financial_indicators, price_data, symbol)),
                ('Volatility', partial(self.calculate_volatility_indicators, price_data))
            ]
            
            # 使用线程池并行计算
            results = {}
            failed_tasks = []
            
            with ThreadPoolExecutor(max_workers=min(6, self.max_workers)) as executor:
                # 提交所有任务
                future_to_task = {
                    executor.submit(task_func): task_name 
                    for task_name, task_func in indicator_tasks
                }
                
                # 收集结果
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        result = future.result(timeout=300)  # 5分钟超时
                        if result is not None and not result.empty:
                            results[task_name] = result
                            logger.debug(f"✅ {symbol} - {task_name}: {result.shape[1]} 个指标")
                        else:
                            failed_tasks.append(task_name)
                            logger.warning(f"⚠️ {symbol} - {task_name}: 计算结果为空")
                    except Exception as e:
                        failed_tasks.append(task_name)
                        logger.error(f"❌ {symbol} - {task_name}: 计算失败 - {e}")
            
            if failed_tasks:
                logger.warning(f"{symbol}: 以下指标类型计算失败: {failed_tasks}")
            
            # 合并所有指标
            try:
                all_indicators = [price_data]
                all_indicators.extend(results.values())
                
                if all_indicators:
                    # 确保所有DataFrame都有一致的索引，但保留日期信息
                    aligned_indicators = []
                    base_length = len(price_data)
                    
                    for df in all_indicators:
                        if df is not None and not df.empty:
                            # 重置索引但保留原始索引作为Date列（如果还没有Date列的话）
                            df_reset = df.reset_index()
                            if 'index' in df_reset.columns and 'Date' not in df_reset.columns:
                                df_reset = df_reset.rename(columns={'index': 'Date'})
                            elif 'Date' not in df_reset.columns:
                                # 如果没有日期信息，使用原始日期
                                df_reset['Date'] = original_dates[:len(df_reset)]
                            
                            # 确保长度一致
                            if len(df_reset) != base_length:
                                # 重新采样或截断以匹配基准长度
                                if len(df_reset) > base_length:
                                    df_reset = df_reset.iloc[:base_length]
                                else:
                                    # 用NaN填充不足的行
                                    missing_rows = base_length - len(df_reset)
                                    padding_data = {}
                                    for col in df_reset.columns:
                                        if col == 'Date':
                                            # 为日期列生成合适的日期
                                            last_date = df_reset['Date'].iloc[-1] if len(df_reset) > 0 else original_dates[-1]
                                            if isinstance(last_date, str):
                                                last_date = pd.to_datetime(last_date)
                                            additional_dates = pd.bdate_range(start=last_date + pd.Timedelta(days=1), periods=missing_rows)
                                            padding_data[col] = additional_dates
                                        else:
                                            padding_data[col] = [np.nan] * missing_rows
                                    
                                    padding_df = pd.DataFrame(padding_data)
                                    df_reset = pd.concat([df_reset, padding_df], ignore_index=True)
                            aligned_indicators.append(df_reset)
                    
                    # 安全合并 - 保留Date列
                    combined_df = pd.concat(aligned_indicators, axis=1)
                    
                    # 处理重复的Date列
                    if 'Date' in combined_df.columns:
                        date_cols = [col for col in combined_df.columns if col == 'Date']
                        if len(date_cols) > 1:
                            # 保留第一个Date列，删除其余的
                            combined_df = combined_df.loc[:, ~combined_df.columns.duplicated(keep='first')]
                    
                    # 从财务数据中提取新增的列
                    if 'Financial' in results:
                        financial_data = results['Financial']
                        financial_cols = [col for col in financial_data.columns if col not in price_data.columns and col != 'Date']
                        if financial_cols:
                            for col in financial_cols:
                                if col not in combined_df.columns:
                                    # 重新索引财务数据以匹配基准索引
                                    financial_col_data = financial_data[col].reindex(range(base_length), method='ffill')
                                    combined_df[col] = financial_col_data
                    
                    # 添加股票代码
                    combined_df['Symbol'] = symbol
                    logger.debug(f"✅ {symbol}: 已添加Symbol列，当前列数: {len(combined_df.columns)}")
                    
                    # 重新排列列顺序：Date, Symbol, 然后是其他列
                    cols = []
                    if 'Date' in combined_df.columns:
                        cols.append('Date')
                    cols.append('Symbol')
                    cols.extend([col for col in combined_df.columns if col not in ['Date', 'Symbol']])
                    combined_df = combined_df[cols]
                    
                    # 验证Symbol列是否存在
                    if 'Symbol' not in combined_df.columns:
                        logger.error(f"❌ {symbol}: Symbol列添加失败！当前列: {list(combined_df.columns[:10])}...")
                        # 强制添加Symbol列
                        combined_df['Symbol'] = symbol
                    
                    # 重置数字索引，但保留Date列
                    combined_df = combined_df.reset_index(drop=True)
                    
                    elapsed_time = time.time() - start_time
                    logger.info(f"✅ {symbol}: 并行计算完成 {len(combined_df.columns)-2} 个指标 (耗时: {elapsed_time:.2f}s)")
                    return combined_df
                    
            except Exception as e:
                logger.error(f"❌ {symbol}: 合并指标时发生错误 - {e}")
                # 降级到顺序计算方法
                return self._calculate_indicators_sequential(symbol, price_data)
            else:
                logger.error(f"❌ {symbol}: 所有指标计算都失败了")
                return None
                
        except Exception as e:
            logger.error(f"❌ {symbol}: 并行计算失败 - {e}")
            return self._calculate_indicators_sequential(symbol, price_data)
    
    def _calculate_indicators_sequential(self, symbol: str, price_data: pd.DataFrame) -> Optional[pd.DataFrame]:
        """
        顺序计算单只股票的所有指标（备用方法）
        """
        try:
            logger.info(f"开始顺序计算 {symbol} 的所有指标...")
            
            # 重置指标跟踪器
            self._reset_indicators_cache()
            
            # 保存原始日期信息
            original_dates = price_data.index
            
            # 1. 计算Alpha158指标体系 (~158个)
            alpha158_indicators = self.calculate_alpha158_indicators(price_data)
            
            # 2. 计算Alpha360指标体系 (~360个)
            alpha360_indicators = self.calculate_alpha360_indicators(price_data)
            
            # 3. 计算技术指标 (~60个)
            technical_indicators = self.calculate_all_technical_indicators(price_data)
            
            # 4. 计算蜡烛图形态 (61个)
            candlestick_patterns = self.calculate_candlestick_patterns(price_data)
            
            # 5. 计算财务指标 (~15个)
            financial_data = self.calculate_financial_indicators(price_data, symbol)
            
            # 6. 计算波动率指标 (~8个)
            volatility_indicators = self.calculate_volatility_indicators(price_data)
            
            # 合并所有指标（确保索引一致性并保留日期信息）
            base_index = price_data.index
            indicator_dfs = [
                price_data,
                alpha158_indicators,
                alpha360_indicators,
                technical_indicators,
                candlestick_patterns,
                financial_data,  # 添加财务数据
                volatility_indicators
            ]
            
            # 重新索引所有DataFrame以确保一致性，并保留日期信息
            aligned_dfs = []
            for df in indicator_dfs:
                if df is not None and not df.empty:
                    if not df.index.equals(base_index):
                        df = df.reindex(base_index, method='ffill')
                    
                    # 将索引转换为Date列（如果还不是列的话）
                    df_with_date = df.reset_index()
                    if 'index' in df_with_date.columns and 'Date' not in df_with_date.columns:
                        df_with_date = df_with_date.rename(columns={'index': 'Date'})
                    elif 'Date' not in df_with_date.columns:
                        df_with_date['Date'] = original_dates
                    
                    aligned_dfs.append(df_with_date)
            
            all_indicators = pd.concat(aligned_dfs, axis=1)
            
            # 处理重复的Date列
            if 'Date' in all_indicators.columns:
                date_cols = [col for col in all_indicators.columns if col == 'Date']
                if len(date_cols) > 1:
                    # 保留第一个Date列，删除其余的
                    all_indicators = all_indicators.loc[:, ~all_indicators.columns.duplicated(keep='first')]
            
            # 财务数据已经在主合并中处理，无需额外操作
            
            # 添加股票代码
            all_indicators['Symbol'] = symbol
            logger.debug(f"✅ {symbol}: 顺序计算已添加Symbol列，当前列数: {len(all_indicators.columns)}")
            
            # 重新排列列顺序：Date, Symbol, 然后是其他列
            cols = []
            if 'Date' in all_indicators.columns:
                cols.append('Date')
            cols.append('Symbol')
            cols.extend([col for col in all_indicators.columns if col not in ['Date', 'Symbol']])
            all_indicators = all_indicators[cols]
            
            # 验证Symbol列是否存在
            if 'Symbol' not in all_indicators.columns:
                logger.error(f"❌ {symbol}: 顺序计算Symbol列添加失败！当前列: {list(all_indicators.columns[:10])}...")
                # 强制添加Symbol列
                all_indicators['Symbol'] = symbol
            
            # 重置数字索引，但保留Date列
            all_indicators = all_indicators.reset_index(drop=True)
            
            logger.info(f"✅ {symbol}: 顺序计算完成 {len(all_indicators.columns)-2} 个指标")
            return all_indicators
            
        except Exception as e:
            logger.error(f"❌ {symbol}: 顺序计算失败 - {e}")
            return None
    
    def calculate_all_indicators(self, max_stocks: Optional[int] = None) -> pd.DataFrame:
        """计算所有股票的所有指标（支持并行处理）"""
        stocks = self.get_available_stocks()
        
        if max_stocks:
            stocks = stocks[:max_stocks]
        
        if not stocks:
            logger.error("没有找到可用的股票数据")
            return pd.DataFrame()
        
        logger.info(f"开始计算 {len(stocks)} 只股票的指标...")
        start_time = time.time()
        
        if self.enable_parallel and len(stocks) > 1:
            return self._calculate_all_stocks_parallel(stocks)
        else:
            return self._calculate_all_stocks_sequential(stocks)
    
    def _calculate_all_stocks_parallel(self, stocks: List[str]) -> pd.DataFrame:
        """并行计算多只股票的指标"""
        logger.info(f"使用并行模式计算 {len(stocks)} 只股票 (最大线程数: {self.max_workers})")
        
        all_results = []
        success_count = 0
        failed_stocks = []
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有股票的计算任务
            future_to_symbol = {
                executor.submit(self.calculate_all_indicators_for_stock, symbol): symbol 
                for symbol in stocks
            }
            
            # 收集结果（带进度显示）
            completed = 0
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                completed += 1
                
                try:
                    result = future.result(timeout=600)  # 10分钟超时
                    if result is not None:
                        all_results.append(result)
                        success_count += 1
                        logger.info(f"✅ 进度 {completed}/{len(stocks)}: {symbol} 计算完成 ({len(result.columns)-1} 个指标)")
                    else:
                        failed_stocks.append(symbol)
                        logger.warning(f"⚠️ 进度 {completed}/{len(stocks)}: {symbol} 计算结果为空")
                        
                except Exception as e:
                    failed_stocks.append(symbol)
                    logger.error(f"❌ 进度 {completed}/{len(stocks)}: {symbol} 计算失败 - {e}")
        
        elapsed_time = time.time() - start_time
        
        if failed_stocks:
            logger.warning(f"计算失败的股票 ({len(failed_stocks)}): {failed_stocks[:5]}{'...' if len(failed_stocks) > 5 else ''}")
        
        if all_results:
            try:
                # 数据合并前的预处理和检查
                logger.info("开始合并多只股票的计算结果...")
                
                # 强化的DataFrame清理和标准化
                logger.info("开始强化的DataFrame清理和标准化...")
                
                # 第一步：初步清理和验证
                valid_dfs = []
                for i, df in enumerate(all_results):
                    if df is None or df.empty:
                        logger.warning(f"跳过空的DataFrame (索引: {i})")
                        continue
                    
                    # 完全重置索引，确保为连续数字索引
                    df_clean = df.copy()
                    df_clean = df_clean.reset_index(drop=True)
                    
                    # 检查索引唯一性
                    if df_clean.index.has_duplicates:
                        logger.warning(f"DataFrame {i} 存在重复索引，进行去重")
                        df_clean = df_clean.loc[~df_clean.index.duplicated(keep='first')]
                        df_clean = df_clean.reset_index(drop=True)
                    
                    valid_dfs.append((i, df_clean))
                
                if not valid_dfs:
                    logger.error("❌ 没有有效的计算结果可以处理")
                    return pd.DataFrame()
                
                # 第二步：统一列结构
                logger.info(f"统一 {len(valid_dfs)} 个DataFrame的列结构...")
                
                # 获取所有唯一的列名
                all_columns = set()
                for _, df in valid_dfs:
                    all_columns.update(df.columns)
                
                all_columns = sorted(list(all_columns))
                logger.info(f"发现 {len(all_columns)} 个唯一列")
                
                # 使用简单安全的合并方法
                cleaned_results = []
                for i, df in valid_dfs:
                    try:
                        # 简单复制和重置索引
                        clean_df = df.copy()
                        clean_df = clean_df.reset_index(drop=True)
                        
                        # 确保所有列都是数值类型或字符串，但保护重要的字符串列和日期列
                        # 需要保护的字符串列和日期列
                        protected_cols = ['Symbol', 'Ticker', 'Name', 'ENName', 'Code', 'Date']
                        
                        for col in clean_df.columns:
                            try:
                                # 跳过重要的字符串列和日期列，不进行数值转换
                                if col in protected_cols:
                                    continue
                                    
                                # 尝试转换为数值，如果失败就保持原样
                                if clean_df[col].dtype == 'object':
                                    try:
                                        clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
                                    except:
                                        pass
                            except:
                                pass
                        
                        cleaned_results.append(clean_df)
                        logger.debug(f"✅ 成功清理DataFrame {i}")
                        
                    except Exception as e:
                        logger.error(f"❌ 清理DataFrame {i} 时发生错误: {e}")
                        continue
                
                if not cleaned_results:
                    logger.error("❌ 没有有效的计算结果可以合并")
                    return pd.DataFrame()
                
                # 安全合并DataFrame - 使用更保守的方法
                logger.info(f"正在合并 {len(cleaned_results)} 个有效结果...")
                try:
                    # 逐个合并，避免列不匹配的问题
                    combined_df = None
                    for i, df in enumerate(cleaned_results):
                        if combined_df is None:
                            combined_df = df.copy()
                        else:
                            # 使用outer join确保所有列都被保留
                            combined_df = pd.concat([combined_df, df], ignore_index=True, sort=False)
                        logger.debug(f"合并第 {i+1}/{len(cleaned_results)} 个DataFrame")
                    
                    if combined_df is None or combined_df.empty:
                        logger.error("❌ 合并后的DataFrame为空")
                        return pd.DataFrame()
                    
                except Exception as merge_error:
                    logger.error(f"❌ DataFrame合并失败: {merge_error}")
                    # 降级到最简单的方法
                    logger.info("尝试使用最简单的合并方法...")
                    try:
                        # 只保留列数最少的DataFrame的列
                        min_cols = min(len(df.columns) for df in cleaned_results)
                        logger.info(f"使用最小列数: {min_cols}")
                        
                        # 找到有最小列数的第一个DataFrame作为模板
                        template_df = next(df for df in cleaned_results if len(df.columns) == min_cols)
                        template_cols = template_df.columns.tolist()
                        
                        # 只保留公共列进行合并
                        aligned_dfs = []
                        for df in cleaned_results:
                            aligned_df = df[template_cols].copy()
                            aligned_dfs.append(aligned_df)
                        
                        combined_df = pd.concat(aligned_dfs, ignore_index=True)
                        logger.info(f"✅ 简化合并成功，保留 {len(template_cols)} 列")
                        
                    except Exception as e2:
                        logger.error(f"❌ 简化合并也失败: {e2}")
                        return pd.DataFrame()
                
                # 验证合并结果
                if combined_df.empty:
                    logger.error("❌ 合并后的DataFrame为空")
                    return pd.DataFrame()
                
                # 检查重复行
                initial_rows = len(combined_df)
                combined_df = combined_df.drop_duplicates()
                if len(combined_df) < initial_rows:
                    logger.info(f"移除了 {initial_rows - len(combined_df)} 行重复数据")
                
                logger.info(f"✅ 并行计算完成: {success_count}/{len(stocks)} 只股票成功 (耗时: {elapsed_time:.2f}s)")
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                logger.info(f"📊 总指标数量: {indicator_count}")
                logger.info(f"📈 总数据行数: {len(combined_df)}")
                logger.info(f"⚡ 平均每只股票耗时: {elapsed_time/len(stocks):.2f}s")
                return combined_df
                
            except Exception as e:
                logger.error(f"❌ 合并计算结果时发生错误: {e}")
                logger.error(f"尝试降级为简单合并...")
                
                # 降级方案：最安全的逐个合并
                try:
                    logger.info("使用最安全的逐个合并方案...")
                    
                    # 只保留非空的结果
                    valid_results = [df for df in all_results if df is not None and not df.empty]
                    
                    if not valid_results:
                        logger.error("❌ 没有有效的计算结果")
                        return pd.DataFrame()
                    
                    logger.info(f"开始逐个合并 {len(valid_results)} 个DataFrame...")
                    
                    # 逐个安全合并
                    combined_df = None
                    successful_merges = 0
                    
                    for i, df in enumerate(valid_results):
                        try:
                            # 彻底清理单个DataFrame
                            clean_df = df.copy()
                            clean_df = clean_df.reset_index(drop=True)
                            
                            # 确保列名为字符串
                            clean_df.columns = [str(col) for col in clean_df.columns]
                            
                            # 去除任何可能的重复索引
                            if clean_df.index.has_duplicates:
                                clean_df = clean_df.loc[~clean_df.index.duplicated(keep='first')]
                                clean_df = clean_df.reset_index(drop=True)
                            
                            # 转换为字典再转回DataFrame（最彻底的清理）
                            # 保护重要的字符串列和日期列
                            protected_cols = ['Symbol', 'Ticker', 'Name', 'ENName', 'Code', 'Date']
                            data_dict = {}
                            for col in clean_df.columns:
                                try:
                                    if col in protected_cols:
                                        # 对于重要的字符串列和日期列，直接保留原值
                                        data_dict[col] = clean_df[col].tolist()
                                    else:
                                        data_dict[col] = clean_df[col].values.tolist()
                                except:
                                    if col in protected_cols:
                                        # 对于重要列，尝试保留原值而不是设为NaN
                                        try:
                                            data_dict[col] = clean_df[col].astype(str).tolist()
                                        except:
                                            data_dict[col] = [''] * len(clean_df)
                                    else:
                                        data_dict[col] = [np.nan] * len(clean_df)
                            
                            clean_df = pd.DataFrame(data_dict)
                            
                            if combined_df is None:
                                combined_df = clean_df
                            else:
                                # 逐个添加行
                                combined_df = pd.concat([combined_df, clean_df], ignore_index=True, sort=False)
                            
                            successful_merges += 1
                            logger.debug(f"成功合并第 {i+1} 个DataFrame")
                            
                        except Exception as merge_error:
                            logger.warning(f"跳过第 {i+1} 个DataFrame，合并失败: {merge_error}")
                            continue
                    
                    if combined_df is not None and not combined_df.empty:
                        logger.info(f"✅ 降级合并成功: {successful_merges}/{len(valid_results)} 个结果")
                        # 计算指标数量：总列数减去Date和Symbol列
                        indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                        logger.info(f"📊 总指标数量: {indicator_count}")
                        logger.info(f"📈 总数据行数: {len(combined_df)}")
                        return combined_df
                    else:
                        logger.error("❌ 降级合并后结果为空")
                        return pd.DataFrame()
                    
                except Exception as e2:
                    logger.error(f"❌ 降级合并也失败: {e2}")
                    # 最后的备用方案：保存单个最大的结果
                    try:
                        if all_results:
                            largest_df = max(all_results, key=lambda x: len(x) if x is not None else 0)
                            if largest_df is not None and not largest_df.empty:
                                result_df = largest_df.copy().reset_index(drop=True)
                                logger.warning(f"⚠️ 使用最大的单个结果: {len(result_df)} 行")
                                return result_df
                    except:
                        pass
                    return pd.DataFrame()
        else:
            logger.error("❌ 没有成功计算任何股票的指标")
            return pd.DataFrame()
    
    def _calculate_all_stocks_sequential(self, stocks: List[str]) -> pd.DataFrame:
        """顺序计算多只股票的指标"""
        logger.info(f"使用顺序模式计算 {len(stocks)} 只股票")
        
        all_results = []
        success_count = 0
        start_time = time.time()
        
        for i, symbol in enumerate(stocks, 1):
            stock_start_time = time.time()
            logger.info(f"📈 处理第 {i}/{len(stocks)} 只股票: {symbol}")
            
            result = self.calculate_all_indicators_for_stock(symbol)
            if result is not None:
                all_results.append(result)
                success_count += 1
                stock_elapsed = time.time() - stock_start_time
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(result.columns) - (2 if 'Date' in result.columns else 1)
                logger.info(f"✅ {symbol}: 完成 {indicator_count} 个指标 (耗时: {stock_elapsed:.2f}s)")
            else:
                logger.warning(f"⚠️ {symbol}: 计算失败")
        
        elapsed_time = time.time() - start_time
        
        if all_results:
            try:
                # 数据合并前的预处理和检查
                logger.info("开始合并多只股票的计算结果...")
                
                # 重置所有DataFrame的索引以避免冲突
                cleaned_results = []
                for i, df in enumerate(all_results):
                    if df is None or df.empty:
                        logger.warning(f"跳过空的DataFrame (索引: {i})")
                        continue
                    cleaned_results.append(df.reset_index(drop=True))
                
                if not cleaned_results:
                    logger.error("❌ 没有有效的计算结果可以合并")
                    return pd.DataFrame()
                
                # 安全合并DataFrame
                combined_df = pd.concat(cleaned_results, ignore_index=True, sort=False)
                
                # 检查重复行
                initial_rows = len(combined_df)
                combined_df = combined_df.drop_duplicates()
                if len(combined_df) < initial_rows:
                    logger.info(f"移除了 {initial_rows - len(combined_df)} 行重复数据")
                
                logger.info(f"✅ 顺序计算完成: {success_count}/{len(stocks)} 只股票成功 (总耗时: {elapsed_time:.2f}s)")
                # 计算指标数量：总列数减去Date和Symbol列
                indicator_count = len(combined_df.columns) - (2 if 'Date' in combined_df.columns else 1)
                logger.info(f"📊 总指标数量: {indicator_count}")
                logger.info(f"📈 总数据行数: {len(combined_df)}")
                logger.info(f"⏱️ 平均每只股票耗时: {elapsed_time/len(stocks):.2f}s")
                return combined_df
                
            except Exception as e:
                logger.error(f"❌ 顺序计算合并结果时发生错误: {e}")
                return pd.DataFrame()
        else:
            logger.error("❌ 没有成功计算任何股票的指标")
            return pd.DataFrame()
    
    def save_results(self, df: pd.DataFrame, filename: str = "enhanced_quantitative_indicators.csv") -> str:
        """保存结果到CSV文件，包含中文标签行，空值使用空字符串（兼容SAS）"""
        if df.empty:
            logger.warning("DataFrame为空，无法保存")
            return ""
        
        try:
            output_path = self.output_dir / filename
            
            # 获取标准字段顺序并重新排列DataFrame
            standard_columns = self._get_standard_column_order()
            available_columns = [col for col in standard_columns if col in df.columns]
            
            # 重新排列DataFrame列顺序
            df_reordered = df[available_columns]
            
            # 获取列名和中文标签
            columns = df_reordered.columns.tolist()
            chinese_labels = self.get_field_labels(columns)
            
            # 处理空值：将NaN替换为空字符串，以兼容SAS
            df_clean = df_reordered.copy()
            df_clean = df_clean.fillna('')  # 将NaN值替换为空字符串
            
            logger.info("📝 空值处理: 将NaN值替换为空字符串以兼容SAS")
            
            # 使用手动方式写入CSV文件以包含中文标签行
            with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
                import csv
                writer = csv.writer(f)
                
                # 第一行：字段名（英文列名）
                writer.writerow(columns)
                
                # 第二行：中文标签
                writer.writerow(chinese_labels)
                
                # 第三行开始：具体数据
                for _, row in df_clean.iterrows():
                    writer.writerow(row.values)
            
            logger.info(f"结果已保存到: {output_path}")
            logger.info(f"数据形状: {df_reordered.shape}")
            logger.info(f"包含中文标签行的CSV格式:")
            logger.info(f"  第一行: 字段名 ({len(columns)} 个字段)")
            logger.info(f"  第二行: 中文标签")
            logger.info(f"  第三行开始: 具体数据 ({len(df_reordered)} 行数据)")
            logger.info(f"  空值处理: NaN → '' (空字符串，兼容SAS)")
            
            # 计算指标数量：总列数减去Date和Symbol列
            indicator_count = len(df_reordered.columns) - (2 if 'Date' in df_reordered.columns else 1)
            logger.info(f"指标数量: {indicator_count}")
            
            return str(output_path)
            
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            return ""
    
    def run(self, max_stocks: Optional[int] = None, output_filename: str = "enhanced_quantitative_indicators.csv"):
        """运行完整的指标计算流程"""
        logger.info("=" * 80)
        logger.info("🚀 开始运行增强版Qlib指标计算器")
        logger.info(f"⚙️ 多线程模式: {'启用' if self.enable_parallel else '禁用'}")
        if self.enable_parallel:
            logger.info(f"🧵 最大线程数: {self.max_workers}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        # 计算指标
        results_df = self.calculate_all_indicators(max_stocks=max_stocks)
        
        total_elapsed = time.time() - start_time
        
        if not results_df.empty:
            # 保存结果
            output_path = self.save_results(results_df, output_filename)
            
            logger.info("=" * 80)
            logger.info("✅ 指标计算完成！")
            # 计算指标数量：总列数减去Date和Symbol列
            indicator_count = len(results_df.columns) - (2 if 'Date' in results_df.columns else 1)
            logger.info(f"📊 总共计算了 {indicator_count} 个指标")
            logger.info(f"📈 包含 {results_df['Symbol'].nunique()} 只股票")
            logger.info(f"⏱️ 总耗时: {total_elapsed:.2f} 秒")
            logger.info(f"💾 结果保存至: {output_path}")
            logger.info("=" * 80)
            
            # 显示指标统计
            self._show_indicators_summary(results_df)
        else:
            logger.error(f"❌ 指标计算失败，没有生成任何结果 (耗时: {total_elapsed:.2f}s)")
    
    def _show_indicators_summary(self, df: pd.DataFrame):
        """显示指标统计摘要"""
        logger.info("📊 指标分类统计:")
        logger.info("-" * 50)
        
        # 统计各类指标数量
        alpha158_count = len([col for col in df.columns if col.startswith('ALPHA158_')])
        alpha360_count = len([col for col in df.columns if col.startswith('ALPHA360_')])
        technical_count = len([col for col in df.columns if any(prefix in col for prefix in ['SMA', 'EMA', 'RSI', 'MACD', 'ADX', 'ATR', 'BB_', 'STOCH']) and not col.startswith(('ALPHA158_', 'ALPHA360_'))])
        candlestick_count = len([col for col in df.columns if col.startswith('CDL')])
        financial_count = len([col for col in df.columns if any(prefix in col for prefix in ['PriceToBook', 'MarketCap', 'PE', 'ROE', 'ROA', 'turnover', 'TobinsQ'])])
        volatility_count = len([col for col in df.columns if 'Volatility' in col or 'SemiDeviation' in col])
        
        logger.info(f"Alpha158指标: {alpha158_count} 个")
        logger.info(f"Alpha360指标: {alpha360_count} 个")
        logger.info(f"技术指标: {technical_count} 个")
        logger.info(f"蜡烛图形态: {candlestick_count} 个")
        logger.info(f"财务指标: {financial_count} 个")
        logger.info(f"波动率指标: {volatility_count} 个")
        logger.info(f"总计: {alpha158_count + alpha360_count + technical_count + candlestick_count + financial_count + volatility_count} 个")

    def calculate_all_indicators_streaming(self, output_file: str, max_stocks: Optional[int] = None, batch_size: int = 20):
        """
        流式计算所有股票的指标，逐行写入CSV，极大节省内存
        """
        stocks = self.get_available_stocks()
        if max_stocks:
            stocks = stocks[:max_stocks]
        logger = logging.getLogger(__name__)
        logger.info(f"[流式模式] 开始流式计算 {len(stocks)} 只股票的指标 (批次大小: {batch_size})")
        
        # 预定义标准字段顺序，确保一致性
        standard_columns = self._get_standard_column_order()
        logger.info(f"[流式模式] 使用预定义字段顺序，共 {len(standard_columns)} 个字段")
        
        # 第一步：收集实际存在的列名（用于验证）
        actual_columns = set()
        for i, symbol in enumerate(stocks):
            try:
                result = self.calculate_all_indicators_for_stock(symbol)
                if result is not None and not result.empty:
                    actual_columns.update(result.columns)
                if i % 10 == 0:
                    gc.collect()
            except Exception as e:
                logger.warning(f"收集列名时跳过 {symbol}: {e}")
        
        # 过滤出实际存在的标准列
        available_columns = [col for col in standard_columns if col in actual_columns]
        logger.info(f"[流式模式] 实际可用字段: {len(available_columns)} 个")
        
        # 第二步：写入CSV头部
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(available_columns)
        
        # 第三步：分批处理并逐行写入
        current_batch = []
        batch_num = 0
        total_rows = 0
        for i, symbol in enumerate(stocks):
            try:
                result = self.calculate_all_indicators_for_stock(symbol)
                if result is not None and not result.empty:
                    current_batch.append(result)
                if len(current_batch) >= batch_size or i == len(stocks) - 1:
                    batch_num += 1
                    batch_rows = 0
                    with open(output_file, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        for df in current_batch:
                            for _, row in df.iterrows():
                                row_data = [row[col] if col in row and not pd.isna(row[col]) else '' for col in available_columns]
                                writer.writerow(row_data)
                                batch_rows += 1
                    total_rows += batch_rows
                    logger.info(f"[流式模式] 第 {batch_num} 批写入完成: {batch_rows} 行")
                    current_batch.clear()
                    gc.collect()
            except Exception as e:
                logger.warning(f"写入数据时跳过 {symbol}: {e}")
        logger.info(f"[流式模式] 流式计算完成，总行数: {total_rows}")

    def calculate_indicators_incremental(self, output_file: str, 
                                       max_stocks: Optional[int] = None,
                                       force_update: bool = False,
                                       batch_size: int = 20,
                                       backup_output: bool = True) -> bool:
        """
        增强版增量计算指标，只计算需要更新的股票
        基于"Stock X Date X Indicator"维度进行增量判断
        
        Parameters:
        -----------
        output_file : str
            输出文件路径
        max_stocks : Optional[int]
            最大股票数量限制
        force_update : bool
            是否强制更新所有股票
        batch_size : int
            批次大小
        backup_output : bool
            是否备份输出文件
            
        Returns:
        --------
        bool : 是否成功
        """
        if not self.enable_incremental:
            logger.error("增量计算模式未启用")
            return False
        
        # 获取股票列表
        stocks = self.get_available_stocks()
        if max_stocks:
            stocks = stocks[:max_stocks]
        
        # 显示计算覆盖范围
        logger.info("=" * 80)
        logger.info("📊 增强版增量计算覆盖范围")
        logger.info("=" * 80)
        logger.info(f"📁 数据目录: {self.data_dir}")
        logger.info(f"📄 输出文件: {output_file}")
        logger.info(f"📈 总股票数: {len(stocks)} 只")
        logger.info(f"🔧 强制更新: {'是' if force_update else '否'}")
        logger.info(f"📦 批次大小: {batch_size}")
        logger.info(f"💾 自动备份: {'是' if backup_output else '否'}")
        if max_stocks:
            logger.info(f"🎯 股票限制: 最多 {max_stocks} 只")
        
        # 分析数据时间范围
        data_start_date = None
        data_end_date = None
        if stocks:
            try:
                sample_stock = stocks[0]
                sample_data = self.read_qlib_binary_data(sample_stock)
                if sample_data is not None and not sample_data.empty:
                    data_start_date = sample_data.index.min()
                    data_end_date = sample_data.index.max()
                    logger.info(f"📅 数据时间范围: {data_start_date} 至 {data_end_date}")
                    logger.info(f"📊 数据天数: {(data_end_date - data_start_date).days + 1} 天")
            except Exception as e:
                logger.warning(f"无法获取数据时间范围: {e}")
        
        # 检查现有输出文件的日期范围
        output_start_date = None
        output_end_date = None
        if os.path.exists(output_file):
            try:
                existing_data = pd.read_csv(output_file, low_memory=False)
                logger.info(f"📋 现有输出文件: {len(existing_data)} 行")
                if 'Date' in existing_data.columns:
                    # 跳过第一行（字段名）和第二行（中文标签）
                    date_data = existing_data['Date'].iloc[2:] if len(existing_data) > 2 else existing_data['Date']
                    # 处理可能包含时间的日期格式
                    output_start_date = pd.to_datetime(date_data, format='mixed').min()
                    output_end_date = pd.to_datetime(date_data, format='mixed').max()
                    logger.info(f"📅 已计算时间范围: {output_start_date} 至 {output_end_date}")
                    if data_start_date and data_end_date:
                        # 计算覆盖率
                        total_days = (data_end_date - data_start_date).days + 1
                        calculated_days = (output_end_date - output_start_date).days + 1
                        coverage = (calculated_days / total_days) * 100
                        logger.info(f"📊 数据覆盖率: {coverage:.1f}% ({calculated_days}/{total_days} 天)")
            except Exception as e:
                logger.warning(f"无法读取现有输出文件: {e}")
        else:
            logger.info("📋 现有输出文件: 不存在（将创建新文件）")
        
        # 分析需要更新的股票和日期范围
        needs_update = []
        skip_count = 0
        update_date_ranges = []
        
        for symbol in stocks:
            # 获取股票的日期范围
            start_date, end_date = self._get_stock_date_range(symbol)
            if start_date is None or end_date is None:
                logger.warning(f"跳过 {symbol}: 无法获取日期范围")
                continue
            
            date_range = (start_date, end_date)
            should_update, reason = self._needs_update(symbol, force_update, date_range)
            
            if should_update:
                # 计算增量开始日期
                incremental_start_date = None
                if not force_update and output_end_date:
                    # 如果现有数据到2025-05-30，增量计算应该从2025-05-31开始
                    incremental_start_date = (output_end_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    logger.info(f"{symbol}: 增量计算从 {incremental_start_date} 开始")
                
                needs_update.append((symbol, reason, date_range, incremental_start_date))
                update_date_ranges.append(date_range)
            else:
                skip_count += 1
                logger.debug(f"跳过 {symbol}: {reason}")
        
        logger.info(f"需要更新: {len(needs_update)} 只股票")
        logger.info(f"跳过更新: {skip_count} 只股票")
        
        # 显示本次计算范围
        if needs_update and update_date_ranges:
            if force_update:
                logger.info(f"🎯 本次计算范围: 全量计算 ({data_start_date} 至 {data_end_date})")
            else:
                # 计算需要更新的日期范围
                update_start = min([r[0] for r in update_date_ranges])
                update_end = max([r[1] for r in update_date_ranges])
                logger.info(f"🎯 本次计算范围: 增量计算 ({len(needs_update)} 只股票)")
                
                # 如果有现有输出文件，显示真正的增量范围
                if output_start_date and output_end_date:
                    if pd.to_datetime(update_end) > output_end_date:
                        # 真正的增量范围：从现有数据结束日期后一天开始
                        true_incremental_start = (output_end_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                        logger.info(f"📈 增量计算范围: {true_incremental_start} 至 {update_end}")
                        logger.info(f"📊 预计新增天数: {(pd.to_datetime(update_end) - output_end_date).days} 天")
                    elif pd.to_datetime(update_start) < output_start_date:
                        logger.info(f"📈 补充历史数据: {update_start} 至 {output_start_date}")
                else:
                    logger.info(f"📅 全量计算范围: {update_start} 至 {update_end}")
        
        logger.info("=" * 80)
        
        if not needs_update:
            logger.info("✅ 所有股票都是最新的，无需更新")
            return True
        
        # 备份现有输出文件
        backup_path = ""
        if backup_output and os.path.exists(output_file):
            backup_path = self._backup_output_file(output_file)
            if backup_path:
                self.metadata['last_output_backup'] = backup_path
        
        # 分批处理需要更新的股票
        success_count = 0
        failed_count = 0
        all_new_data = []
        
        for i in range(0, len(needs_update), batch_size):
            batch = needs_update[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(needs_update) + batch_size - 1) // batch_size
            
            logger.info(f"处理第 {batch_num}/{total_batches} 批 ({len(batch)} 只股票)")
            
            batch_results = []
            
            for symbol, reason, date_range, incremental_start_date in batch:
                try:
                    if incremental_start_date:
                        logger.info(f"计算 {symbol} ({reason}) - 增量范围: {incremental_start_date} 至 {date_range[1]}")
                    else:
                        logger.info(f"计算 {symbol} ({reason}) - 日期范围: {date_range}")
                    
                    result = self.calculate_all_indicators_for_stock(symbol, incremental_start_date)
                    
                    if result is not None and not result.empty:
                        batch_results.append(result)
                        self._update_stock_status(symbol, True, len(result), date_range)
                        success_count += 1
                        logger.info(f"✅ {symbol}: 完成 ({len(result)} 行)")
                    else:
                        self._update_stock_status(symbol, False, 0, date_range)
                        failed_count += 1
                        logger.warning(f"⚠️ {symbol}: 计算结果为空")
                        
                except Exception as e:
                    self._update_stock_status(symbol, False, 0, date_range)
                    failed_count += 1
                    logger.error(f"❌ {symbol}: 计算失败 - {e}")
            
            # 合并批次结果
            if batch_results:
                batch_data = pd.concat(batch_results, ignore_index=True, sort=False)
                all_new_data.append(batch_data)
                logger.info(f"批次 {batch_num} 完成: {len(batch_data)} 行")
            
            # 保存状态
            self._save_stock_status()
            self._save_data_hashes()
        
        # 合并所有新数据
        if all_new_data:
            new_data = pd.concat(all_new_data, ignore_index=True, sort=False)
            logger.info(f"新数据总计: {len(new_data)} 行")
            
            # 与现有输出文件合并
            final_data = self._merge_with_existing_output(new_data, output_file)
            
            # 检查合并结果
            if final_data is not None and not final_data.empty:
                # 保存最终结果
                self.save_results(final_data, output_file)
                logger.info(f"保存最终结果: {len(final_data)} 行 -> {output_file}")
            else:
                logger.error("❌ 合并结果为空，无法保存")
                logger.info("🔄 保留现有文件")
                # 不进行任何操作，保留现有文件
        else:
            # 没有新数据时，检查是否需要保留现有文件
            if os.path.exists(output_file):
                logger.info("📋 没有新数据，保留现有输出文件")
                # 读取现有文件的行数作为最终结果
                try:
                    existing_data = pd.read_csv(output_file, skiprows=2, low_memory=False)
                    logger.info(f"📋 现有文件保留: {len(existing_data)} 行")
                except Exception as e:
                    logger.warning(f"📋 读取现有文件失败: {e}")
            else:
                logger.warning("📋 没有新数据且输出文件不存在")
        
        # 更新元数据
        self.metadata.update({
            "last_update": datetime.now().isoformat(),
            "total_stocks": len(stocks),
            "processed_stocks": success_count,
            "failed_stocks": failed_count,
            "skipped_stocks": skip_count,
            "output_file": output_file,
            "last_output_backup": backup_path
        })
        self._save_metadata()
        
        logger.info("=" * 80)
        logger.info("✅ 增强版增量计算完成！")
        logger.info(f"📊 成功: {success_count}, 失败: {failed_count}, 跳过: {skip_count}")
        logger.info(f"📈 总行数: {len(all_new_data) > 0 and sum(len(df) for df in all_new_data) or 0}")
        logger.info(f"💾 结果保存至: {output_file}")
        logger.info("=" * 80)
        
        return True
    
    def get_update_summary(self) -> Dict:
        """获取更新摘要"""
        if not self.enable_incremental:
            return {"error": "增量计算模式未启用"}
        
        total_stocks = len(self.get_available_stocks())
        processed_stocks = len([s for s in self.stock_status.values() if s.get('success', False)])
        failed_stocks = len([s for s in self.stock_status.values() if not s.get('success', False)])
        
        return {
            "total_stocks": total_stocks,
            "processed_stocks": processed_stocks,
            "failed_stocks": failed_stocks,
            "last_update": self.metadata.get('last_update'),
            "output_file": self.metadata.get('output_file')
        }
    
    def analyze_data_coverage(self) -> Dict:
        """
        分析数据覆盖率
        
        Returns:
        --------
        Dict: 覆盖率分析结果
        """
        if not self.enable_incremental:
            return {"error": "增量计算模式未启用"}
        
        stocks = self.get_available_stocks()
        if not stocks:
            return {"error": "没有可用的股票数据"}
        
        # 分析数据时间范围
        data_ranges = {}
        total_days = 0
        
        for symbol in stocks[:10]:  # 采样前10只股票
            start_date, end_date = self._get_stock_date_range(symbol)
            if start_date and end_date:
                start_dt = pd.to_datetime(start_date)
                end_dt = pd.to_datetime(end_date)
                days = (end_dt - start_dt).days + 1
                data_ranges[symbol] = {
                    'start_date': start_date,
                    'end_date': end_date,
                    'days': days
                }
                total_days += days
        
        # 分析已处理的数据
        processed_info = {}
        for symbol, status in self.stock_status.items():
            if status.get('success', False):
                date_range = self.date_ranges.get(symbol, {})
                if date_range.get('start_date') and date_range.get('end_date'):
                    start_dt = pd.to_datetime(date_range['start_date'])
                    end_dt = pd.to_datetime(date_range['end_date'])
                    days = (end_dt - start_dt).days + 1
                    processed_info[symbol] = {
                        'start_date': date_range['start_date'],
                        'end_date': date_range['end_date'],
                        'days': days,
                        'rows': status.get('rows', 0)
                    }
        
        # 计算覆盖率
        total_processed_stocks = len(processed_info)
        total_processed_days = sum(info['days'] for info in processed_info.values())
        
        coverage = {
            'total_stocks': len(stocks),
            'processed_stocks': total_processed_stocks,
            'coverage_percentage': (total_processed_stocks / len(stocks)) * 100 if stocks else 0,
            'sample_data_ranges': data_ranges,
            'processed_data_ranges': processed_info,
            'total_processed_days': total_processed_days,
            'average_days_per_stock': total_processed_days / total_processed_stocks if total_processed_stocks > 0 else 0
        }
        
        return coverage
    
    def clean_cache(self, keep_backups: bool = False):
        """清理缓存"""
        if not self.enable_incremental:
            logger.error("增量计算模式未启用")
            return
        
        try:
            # 删除缓存文件
            for file_path in [self.metadata_file, self.stock_status_file, 
                            self.data_hashes_file, self.date_ranges_file]:
                if file_path.exists():
                    file_path.unlink()
                    logger.info(f"删除缓存文件: {file_path}")
            
            # 清理备份文件
            if not keep_backups and self.output_backup_dir.exists():
                shutil.rmtree(self.output_backup_dir)
                logger.info(f"删除备份目录: {self.output_backup_dir}")
            
            # 重新初始化
            self.metadata = self._load_metadata()
            self.stock_status = self._load_stock_status()
            self.data_hashes = self._load_data_hashes()
            self.date_ranges = self._load_date_ranges()
            
            logger.info("✅ 缓存清理完成")
            
        except Exception as e:
            logger.error(f"❌ 清理缓存失败: {e}")
    
    def list_backups(self) -> List[str]:
        """列出备份文件"""
        if not self.enable_incremental:
            return []
        
        backups = []
        if self.output_backup_dir.exists():
            for backup_file in self.output_backup_dir.glob("backup_*.csv"):
                backups.append(str(backup_file))
        
        return sorted(backups, reverse=True)
    
    def restore_backup(self, backup_file: str, output_file: str) -> bool:
        """恢复备份文件"""
        if not self.enable_incremental:
            logger.error("增量计算模式未启用")
            return False
        
        try:
            if not os.path.exists(backup_file):
                logger.error(f"备份文件不存在: {backup_file}")
                return False
            
            shutil.copy2(backup_file, output_file)
            logger.info(f"✅ 备份恢复完成: {backup_file} -> {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 恢复备份失败: {e}")
            return False

    def _get_standard_column_order(self):
        """
        获取标准字段顺序，确保不同市场生成的CSV文件字段顺序一致
        """
        # 按照用户要求的特定顺序定义字段
        custom_columns = [
            'Date',
            'Symbol',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'RealizedVolatility_20',
            'NegativeSemiDeviation_20',
            'ContinuousVolatility_20',
            'PositiveSemiDeviation_20',
            'Volatility_10',
            'Volatility_30',
            'Volatility_60',
            'CDL2CROWS',
            'CDL3BLACKCROWS',
            'CDL3INSIDE',
            'CDL3LINESTRIKE',
            'CDL3OUTSIDE',
            'CDL3STARSINSOUTH',
            'CDL3WHITESOLDIERS',
            'CDLABANDONEDBABY',
            'CDLADVANCEBLOCK',
            'CDLBELTHOLD',
            'CDLBREAKAWAY',
            'CDLCLOSINGMARUBOZU',
            'CDLCONCEALBABYSWALL',
            'CDLCOUNTERATTACK',
            'CDLDARKCLOUDCOVER',
            'CDLDOJI',
            'CDLDOJISTAR',
            'CDLDRAGONFLYDOJI',
            'CDLENGULFING',
            'CDLEVENINGDOJISTAR',
            'CDLEVENINGSTAR',
            'CDLGAPSIDESIDEWHITE',
            'CDLGRAVESTONEDOJI',
            'CDLHAMMER',
            'CDLHANGINGMAN',
            'CDLHARAMI',
            'CDLHARAMICROSS',
            'CDLHIGHWAVE',
            'CDLHIKKAKE',
            'CDLHIKKAKEMOD',
            'CDLHOMINGPIGEON',
            'CDLIDENTICAL3CROWS',
            'CDLINNECK',
            'CDLINVERTEDHAMMER',
            'CDLKICKING',
            'CDLKICKINGBYLENGTH',
            'CDLLADDERBOTTOM',
            'CDLLONGLEGGEDDOJI',
            'CDLLONGLINE',
            'CDLMARUBOZU',
            'CDLMATCHINGLOW',
            'CDLMATHOLD',
            'CDLMORNINGDOJISTAR',
            'CDLMORNINGSTAR',
            'CDLONNECK',
            'CDLPIERCING',
            'CDLRICKSHAWMAN',
            'CDLRISEFALL3METHODS',
            'CDLSEPARATINGLINES',
            'CDLSHOOTINGSTAR',
            'CDLSHORTLINE',
            'CDLSPINNINGTOP',
            'CDLSTALLEDPATTERN',
            'CDLSTICKSANDWICH',
            'CDLTAKURI',
            'CDLTASUKIGAP',
            'CDLTHRUSTING',
            'CDLTRISTAR',
            'CDLUNIQUE3RIVER',
            'CDLUPSIDEGAP2CROWS',
            'CDLXSIDEGAP3METHODS',
            'PriceToBookRatio',
            'MarketCap',
            'PERatio',
            'PriceToSalesRatio',
            'ROE',
            'ROA',
            'ProfitMargins',
            'QuickRatio',
            'DebtToEquity',
            'TobinsQ',
            'DailyTurnover',
            'turnover_c1d',
            'turnover_c5d',
            'turnover_m5d',
            'turnover_c10d',
            'turnover_m10d',
            'turnover_c20d',
            'turnover_m20d',
            'turnover_c30d',
            'turnover_m30d',
            'CurrentRatio',
            'SMA_5',
            'SMA_10',
            'SMA_20',
            'SMA_50',
            'EMA_5',
            'EMA_10',
            'EMA_20',
            'EMA_50',
            'DEMA_20',
            'TEMA_20',
            'KAMA_30',
            'WMA_20',
            'MACD',
            'MACD_Signal',
            'MACD_Histogram',
            'MACDEXT',
            'MACDFIX',
            'RSI_14',
            'CCI_14',
            'CMO_14',
            'MFI_14',
            'WILLR_14',
            'ULTOSC',
            'ADX_14',
            'ADXR_14',
            'APO',
            'AROON_DOWN',
            'AROON_UP',
            'AROONOSC_14',
            'BOP',
            'DX_14',
            'MINUS_DI_14',
            'MINUS_DM_14',
            'PLUS_DI_14',
            'PLUS_DM_14',
            'PPO',
            'TRIX_30',
            'MOM_10',
            'ROC_10',
            'ROCP_10',
            'ROCR_10',
            'ROCR100_10',
            'BB_Upper',
            'BB_Middle',
            'BB_Lower',
            'STOCH_K',
            'STOCH_D',
            'STOCHF_K',
            'STOCHF_D',
            'STOCHRSI_K',
            'STOCHRSI_D',
            'ATR_14',
            'NATR_14',
            'TRANGE',
            'OBV',
            'AD',
            'ADOSC',
            'HT_DCPERIOD',
            'HT_DCPHASE',
            'HT_INPHASE',
            'HT_QUADRATURE',
            'HT_SINE',
            'HT_LEADSINE',
            'HT_TRENDMODE',
            'HT_TRENDLINE',
            'AVGPRICE',
            'MEDPRICE',
            'TYPPRICE',
            'WCLPRICE',
            'MIDPOINT',
            'MIDPRICE',
            'MAMA',
            'FAMA',
            'LINEARREG',
            'LINEARREG_ANGLE',
            'LINEARREG_INTERCEPT',
            'LINEARREG_SLOPE',
            'STDDEV',
            'TSF',
            'VAR',
            'MAXINDEX',
            'MININDEX'
        ]
        
        # Alpha360指标字段（按用户要求的顺序：CLOSE从59到0，然后OPEN从59到0，然后HIGH从59到0，然后LOW从59到0，然后VWAP从59到0，最后VOLUME从59到0）
        alpha360_columns = []
        
        # CLOSE从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_CLOSE{i}')
        
        # OPEN从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_OPEN{i}')
        
        # HIGH从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_HIGH{i}')
        
        # LOW从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_LOW{i}')
        
        # VWAP从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_VWAP{i}')
        
        # VOLUME从59到0
        for i in range(59, -1, -1):
            alpha360_columns.append(f'ALPHA360_VOLUME{i}')
        
        # Alpha158指标字段（按用户要求的顺序）
        alpha158_columns = [
            'ALPHA158_KMID',
            'ALPHA158_KLEN',
            'ALPHA158_KMID2',
            'ALPHA158_KUP',
            'ALPHA158_KUP2',
            'ALPHA158_KLOW',
            'ALPHA158_KLOW2',
            'ALPHA158_KSFT',
            'ALPHA158_KSFT2',
            'ALPHA158_OPEN0',
            'ALPHA158_HIGH0',
            'ALPHA158_LOW0',
            'ALPHA158_VWAP0',
            'ALPHA158_VOLUME0',
            'ALPHA158_ROC5',
            'ALPHA158_ROC10',
            'ALPHA158_ROC20',
            'ALPHA158_ROC30',
            'ALPHA158_ROC60',
            'ALPHA158_MA5',
            'ALPHA158_MA10',
            'ALPHA158_MA20',
            'ALPHA158_MA30',
            'ALPHA158_MA60',
            'ALPHA158_STD5',
            'ALPHA158_STD10',
            'ALPHA158_STD20',
            'ALPHA158_STD30',
            'ALPHA158_STD60',
            'ALPHA158_BETA5',
            'ALPHA158_BETA10',
            'ALPHA158_BETA20',
            'ALPHA158_BETA30',
            'ALPHA158_BETA60',
            'ALPHA158_RSQR5',
            'ALPHA158_RSQR10',
            'ALPHA158_RSQR20',
            'ALPHA158_RSQR30',
            'ALPHA158_RSQR60',
            'ALPHA158_MAX5',
            'ALPHA158_MIN5',
            'ALPHA158_MAX10',
            'ALPHA158_MIN10',
            'ALPHA158_MAX20',
            'ALPHA158_MIN20',
            'ALPHA158_MAX30',
            'ALPHA158_MIN30',
            'ALPHA158_MAX60',
            'ALPHA158_MIN60',
            'ALPHA158_QTLU5',
            'ALPHA158_QTLD5',
            'ALPHA158_QTLU10',
            'ALPHA158_QTLD10',
            'ALPHA158_QTLU20',
            'ALPHA158_QTLD20',
            'ALPHA158_QTLU30',
            'ALPHA158_QTLD30',
            'ALPHA158_QTLU60',
            'ALPHA158_QTLD60',
            'ALPHA158_RANK5',
            'ALPHA158_RANK10',
            'ALPHA158_RANK20',
            'ALPHA158_RANK30',
            'ALPHA158_RANK60',
            'ALPHA158_RSV5',
            'ALPHA158_RSV10',
            'ALPHA158_RSV20',
            'ALPHA158_RSV30',
            'ALPHA158_RSV60',
            'ALPHA158_RESI5',
            'ALPHA158_RESI10',
            'ALPHA158_RESI20',
            'ALPHA158_RESI30',
            'ALPHA158_RESI60',
            'ALPHA158_IMAX5',
            'ALPHA158_IMAX10',
            'ALPHA158_IMAX20',
            'ALPHA158_IMAX30',
            'ALPHA158_IMAX60',
            'ALPHA158_IMIN5',
            'ALPHA158_IMIN10',
            'ALPHA158_IMIN20',
            'ALPHA158_IMIN30',
            'ALPHA158_IMIN60',
            'ALPHA158_IMXD5',
            'ALPHA158_IMXD10',
            'ALPHA158_IMXD20',
            'ALPHA158_IMXD30',
            'ALPHA158_IMXD60',
            'ALPHA158_CORR5',
            'ALPHA158_CORR10',
            'ALPHA158_CORR20',
            'ALPHA158_CORR30',
            'ALPHA158_CORR60',
            'ALPHA158_CORD5',
            'ALPHA158_CORD10',
            'ALPHA158_CORD20',
            'ALPHA158_CORD30',
            'ALPHA158_CORD60',
            'ALPHA158_CNTP5',
            'ALPHA158_CNTP10',
            'ALPHA158_CNTP20',
            'ALPHA158_CNTP30',
            'ALPHA158_CNTP60',
            'ALPHA158_CNTN5',
            'ALPHA158_CNTN10',
            'ALPHA158_CNTN20',
            'ALPHA158_CNTN30',
            'ALPHA158_CNTN60',
            'ALPHA158_CNTD5',
            'ALPHA158_CNTD10',
            'ALPHA158_CNTD20',
            'ALPHA158_CNTD30',
            'ALPHA158_CNTD60',
            'ALPHA158_SUMP5',
            'ALPHA158_SUMP10',
            'ALPHA158_SUMP20',
            'ALPHA158_SUMP30',
            'ALPHA158_SUMP60',
            'ALPHA158_SUMN5',
            'ALPHA158_SUMN10',
            'ALPHA158_SUMN20',
            'ALPHA158_SUMN30',
            'ALPHA158_SUMN60',
            'ALPHA158_SUMD5',
            'ALPHA158_SUMD10',
            'ALPHA158_SUMD20',
            'ALPHA158_SUMD30',
            'ALPHA158_SUMD60',
            'ALPHA158_VMA5',
            'ALPHA158_VMA10',
            'ALPHA158_VMA20',
            'ALPHA158_VMA30',
            'ALPHA158_VMA60',
            'ALPHA158_VSTD5',
            'ALPHA158_VSTD10',
            'ALPHA158_VSTD20',
            'ALPHA158_VSTD30',
            'ALPHA158_VSTD60',
            'ALPHA158_WVMA5',
            'ALPHA158_WVMA10',
            'ALPHA158_WVMA20',
            'ALPHA158_WVMA30',
            'ALPHA158_WVMA60',
            'ALPHA158_VSUMP5',
            'ALPHA158_VSUMP10',
            'ALPHA158_VSUMP20',
            'ALPHA158_VSUMP30',
            'ALPHA158_VSUMP60',
            'ALPHA158_VSUMN5',
            'ALPHA158_VSUMN10',
            'ALPHA158_VSUMN20',
            'ALPHA158_VSUMN30',
            'ALPHA158_VSUMN60',
            'ALPHA158_VSUMD5',
            'ALPHA158_VSUMD10',
            'ALPHA158_VSUMD20',
            'ALPHA158_VSUMD30',
            'ALPHA158_VSUMD60'
        ]
        
        # 合并所有字段，按照用户要求的顺序
        all_columns = custom_columns + alpha360_columns + alpha158_columns
        
        return all_columns


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='增强版Qlib指标计算器 - 集成Alpha158、Alpha360指标体系',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
使用示例:
  # 全量计算模式 (默认)
  python qlib_indicators.py
  
  # 增强版增量计算模式
  python qlib_indicators.py --incremental --output indicators.csv
  
  # 强制更新所有股票
  python qlib_indicators.py --incremental --force-update
  
  # 只计算前10只股票
  python qlib_indicators.py --max-stocks 10
  
  # 指定数据目录和财务数据目录
  python qlib_indicators.py --data-dir ./data --financial-dir ./financial_data
  
  # 自定义输出文件名
  python qlib_indicators.py --output indicators_2025.csv
  
  # 禁用多线程并行计算
  python qlib_indicators.py --disable-parallel
  
  # 自定义线程数量
  python qlib_indicators.py --max-workers 16
  
  # 流式写入模式 (节省内存)
  python qlib_indicators.py --streaming --batch-size 50
  
  # 调试模式
  python qlib_indicators.py --log-level DEBUG --max-stocks 5

时间窗口设置:
  # 计算指定日期范围的数据
  python qlib_indicators.py --start-date 2023-01-01 --end-date 2023-12-31

  # 只计算2024年的数据
  python qlib_indicators.py --start-date 2024-01-01 --end-date 2024-12-31

  # 计算最近30天的数据
  python qlib_indicators.py --recent-days 30

  # 计算最近一年的数据
  python qlib_indicators.py --recent-days 365

  # 结合增量计算和时间窗口
  python qlib_indicators.py --incremental --recent-days 90

增强版增量计算管理:
  # 查看增量计算摘要
  python qlib_indicators.py --incremental --summary
  
  # 分析数据覆盖率
  python qlib_indicators.py --incremental --analyze-coverage
  
  # 清理缓存
  python qlib_indicators.py --incremental --clean-cache
  
  # 列出备份文件
  python qlib_indicators.py --incremental --list-backups
  
  # 恢复备份
  python qlib_indicators.py --incremental --restore-backup backup_file.csv

多线程性能优化:
  - 🚀 多只股票并行计算: 显著提升处理速度
  - ⚡ 单只股票指标类型并行: Alpha158、Alpha360、技术指标等同时计算
  - 🧵 智能线程管理: 自动优化线程数量，避免资源竞争
  - 📊 实时进度显示: 详细的计算进度和性能统计
  - 🔒 线程安全: 确保指标去重和数据一致性

增强版增量计算特性:
  - 🔄 智能增量更新: 基于"Stock X Date X Indicator"维度判断
  - 📊 数据哈希检测: 基于MD5哈希值检测数据变化
  - 📅 日期范围分析: 智能检测数据时间范围变化
  - 💾 自动备份: 计算前自动备份现有结果
  - 🔍 状态跟踪: 详细记录每只股票的处理状态
  - 🛡️ 容错恢复: 支持断点续传和错误恢复
  - 📈 覆盖率分析: 实时显示数据覆盖率统计

支持的指标类型:
  - Alpha158指标体系: ~158个 (KBAR、价格、成交量、滚动技术指标)
  - Alpha360指标体系: ~360个 (过去60天标准化价格和成交量数据)
  - 技术指标: ~60个 (移动平均、MACD、RSI、布林带等)
  - 蜡烛图形态: 61个 (锤子线、十字星、吞没形态等)
  - 财务指标: ~15个 (市净率、换手率、托宾Q值等)
  - 波动率指标: ~8个 (已实现波动率、半变差等)
  
总计约695个指标，具备去重功能和多线程加速
        '''
    )
    
    parser.add_argument(
        '--data-dir',
        default=r"D:\stk_data\trd\us_data",
        help='Qlib数据目录路径'
    )
    
    parser.add_argument(
        '--financial-dir',
        help='财务数据目录路径 (默认: ~/.qlib/financial_data)'
    )
    
    parser.add_argument(
        '--max-stocks',
        type=int,
        help='最大股票数量限制'
    )
    
    parser.add_argument(
        '--output',
        default='enhanced_quantitative_indicators.csv',
        help='输出文件名'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='日志级别'
    )
    
    parser.add_argument(
        '--disable-parallel',
        action='store_true',
        help='禁用多线程并行计算'
    )
    
    parser.add_argument(
        '--max-workers',
        type=int,
        help='最大线程数 (默认: CPU核心数+4，最大32)'
    )
    
    parser.add_argument('--streaming', action='store_true', help='是否启用流式写入模式')
    parser.add_argument('--batch-size', type=int, default=20, help='流式写入批次大小')
    
    # 时间窗口参数
    parser.add_argument('--start-date', type=str, help='计算开始日期 (格式: YYYY-MM-DD，如: 2023-01-01)')
    parser.add_argument('--end-date', type=str, help='计算结束日期 (格式: YYYY-MM-DD，如: 2023-12-31)')
    parser.add_argument('--recent-days', type=int, help='计算最近N天的数据 (与start-date/end-date互斥，如: --recent-days 30)')
    
    # 增量计算相关参数
    parser.add_argument('--incremental', action='store_true', help='启用增强版增量计算模式')
    parser.add_argument('--cache-dir', default='indicator_cache', help='增量计算缓存目录')
    parser.add_argument('--force-update', action='store_true', help='强制更新所有股票')
    parser.add_argument('--backup-output', action='store_true', default=True, help='是否备份输出文件')
    parser.add_argument('--enable-parallel', action='store_true', default=True, help='启用多线程并行计算')
    
    # 增量计算管理命令
    parser.add_argument('--summary', action='store_true', help='显示增量计算摘要')
    parser.add_argument('--clean-cache', action='store_true', help='清理增量计算缓存')
    parser.add_argument('--list-backups', action='store_true', help='列出备份文件')
    parser.add_argument('--restore-backup', help='恢复指定的备份文件')
    parser.add_argument('--analyze-coverage', action='store_true', help='分析数据覆盖率')
    
    args = parser.parse_args()
    
    # 设置日志级别
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level=args.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
    )
    
    try:
        # 验证时间窗口参数
        if args.recent_days and (args.start_date or args.end_date):
            logger.warning("同时指定了--recent-days和--start-date/--end-date参数，将优先使用--recent-days")
            
        # 创建计算器
        calculator = QlibIndicatorsEnhancedCalculator(
            data_dir=args.data_dir,
            financial_data_dir=args.financial_dir,
            enable_parallel=not args.disable_parallel,
            start_date=args.start_date,
            end_date=args.end_date,
            recent_days=args.recent_days,
            max_workers=args.max_workers,
            cache_dir=args.cache_dir,
            enable_incremental=args.incremental
        )
        
        # 处理增量计算管理命令
        if args.summary:
            summary = calculator.get_update_summary()
            logger.info("📊 增强版增量计算摘要:")
            for key, value in summary.items():
                logger.info(f"  {key}: {value}")
            return
        
        if args.analyze_coverage:
            coverage = calculator.analyze_data_coverage()
            if "error" in coverage:
                logger.error(f"❌ 覆盖率分析失败: {coverage['error']}")
                return
            
            logger.info("📊 数据覆盖率分析:")
            logger.info(f"  总股票数: {coverage['total_stocks']}")
            logger.info(f"  已处理股票数: {coverage['processed_stocks']}")
            logger.info(f"  覆盖率: {coverage['coverage_percentage']:.1f}%")
            logger.info(f"  总处理天数: {coverage['total_processed_days']}")
            logger.info(f"  平均每只股票天数: {coverage['average_days_per_stock']:.1f}")
            
            if coverage['sample_data_ranges']:
                logger.info("📅 样本数据时间范围:")
                for symbol, info in list(coverage['sample_data_ranges'].items())[:5]:
                    logger.info(f"  {symbol}: {info['start_date']} 至 {info['end_date']} ({info['days']} 天)")
            return
        
        if args.clean_cache:
            calculator.clean_cache(keep_backups=True)
            return
        
        if args.list_backups:
            backups = calculator.list_backups()
            if backups:
                logger.info("📁 备份文件列表:")
                for backup in backups:
                    logger.info(f"  {backup}")
            else:
                logger.info("📁 没有找到备份文件")
            return
        
        if args.restore_backup:
            if calculator.restore_backup(args.restore_backup, args.output):
                logger.info(f"✅ 备份恢复完成: {args.output}")
            return
        
        # 运行计算
        if args.incremental:
            # 增强版增量计算模式
            logger.info("🔄 启动增强版增量计算模式...")
            logger.info("📊 基于'Stock X Date X Indicator'维度进行增量判断")
            success = calculator.calculate_indicators_incremental(
                output_file=args.output,
                max_stocks=args.max_stocks,
                force_update=args.force_update,
                batch_size=args.batch_size,
                backup_output=args.backup_output
            )
            if not success:
                logger.error("❌ 增强版增量计算失败")
        elif args.streaming:
            # 流式模式
            calculator.calculate_all_indicators_streaming(
                output_file=args.output,
                max_stocks=args.max_stocks,
                batch_size=args.batch_size
            )
        else:
            # 标准全量计算模式
            calculator.run(
                max_stocks=args.max_stocks,
                output_filename=args.output
            )
        
    except KeyboardInterrupt:
        logger.info("用户中断计算")
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        raise


if __name__ == "__main__":
    main()
