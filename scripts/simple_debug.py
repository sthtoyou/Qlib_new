#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化调试脚本
只检查缓存状态，不加载财务数据
"""

import os
import sys
import json
from pathlib import Path

# 添加qlib路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator


class SimpleDebugCalculator(QlibIndicatorsEnhancedCalculator):
    """简化的调试计算器，不加载财务数据"""
    
    def __init__(self, data_dir: str, cache_dir: str = "advanced_indicator_cache"):
        # 不调用父类的__init__，避免加载财务数据
        self.data_dir = data_dir
        self.cache_dir = Path(cache_dir)
        
        # 缓存文件路径
        self.metadata_file = self.cache_dir / "metadata.json"
        self.stock_status_file = self.cache_dir / "stock_status.json"
        self.data_hashes_file = self.cache_dir / "data_hashes.json"
        
        # 加载缓存
        self.metadata = self._load_metadata()
        self.stock_status = self._load_stock_status()
        self.data_hashes = self._load_data_hashes()
    
    def _load_metadata(self):
        """加载元数据"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载元数据失败: {e}")
        return {}
    
    def _load_stock_status(self):
        """加载股票状态"""
        if self.stock_status_file.exists():
            try:
                with open(self.stock_status_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载股票状态失败: {e}")
        return {}
    
    def _load_data_hashes(self):
        """加载数据哈希"""
        if self.data_hashes_file.exists():
            try:
                with open(self.data_hashes_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"加载数据哈希失败: {e}")
        return {}


def debug_cache_issue(data_dir: str, cache_dir: str = "advanced_indicator_cache"):
    """调试缓存问题"""
    
    print("🔍 调试缓存问题")
    print("=" * 60)
    
    # 创建简化的计算器
    calculator = SimpleDebugCalculator(data_dir, cache_dir)
    
    # 获取股票列表
    stocks = calculator.get_available_stocks()
    print(f"📈 总股票数: {len(stocks)} 只")
    
    # 分析前几只股票
    sample_stocks = stocks[:3]
    print(f"\n🔍 分析前 {len(sample_stocks)} 只股票:")
    
    for symbol in sample_stocks:
        print(f"\n📊 {symbol}:")
        
        # 检查股票状态
        stock_status = calculator.stock_status.get(symbol, {})
        if stock_status:
            print(f"  最后更新: {stock_status.get('last_update', 'None')}")
            print(f"  成功状态: {stock_status.get('success', 'None')}")
            print(f"  行数: {stock_status.get('rows', 'None')}")
            
            last_range = stock_status.get("last_date_range")
            if last_range:
                print(f"  缓存日期范围: {last_range}")
                
                # 检查对应的哈希键
                if isinstance(last_range, list) and len(last_range) == 2:
                    start_date, end_date = last_range
                    hash_key = f"{symbol}_{start_date}_{end_date}"
                    hash_exists = hash_key in calculator.data_hashes
                    print(f"  哈希键: {hash_key}")
                    print(f"  哈希存在: {hash_exists}")
                    
                    if hash_exists:
                        hash_value = calculator.data_hashes[hash_key]
                        print(f"  哈希值: {hash_value[:16]}...")
        else:
            print(f"  股票状态: 无记录")
    
    # 统计需要更新的原因
    print(f"\n📊 统计需要更新的原因:")
    reasons = {}
    needs_update_count = 0
    
    for symbol in stocks:
        stock_status = calculator.stock_status.get(symbol, {})
        
        if not stock_status:
            needs_update_count += 1
            reasons["首次计算"] = reasons.get("首次计算", 0) + 1
            continue
        
        # 检查日期范围
        last_range = stock_status.get("last_date_range")
        if not last_range:
            needs_update_count += 1
            reasons["状态信息缺失"] = reasons.get("状态信息缺失", 0) + 1
            continue
        
        # 检查哈希键是否存在
        if isinstance(last_range, list) and len(last_range) == 2:
            start_date, end_date = last_range
            hash_key = f"{symbol}_{start_date}_{end_date}"
            
            if hash_key not in calculator.data_hashes:
                needs_update_count += 1
                reasons["哈希键不匹配"] = reasons.get("哈希键不匹配", 0) + 1
                continue
    
    print(f"需要更新的股票: {needs_update_count}/{len(stocks)}")
    for reason, count in reasons.items():
        print(f"  {reason}: {count} 只")
    
    # 检查缓存状态
    print(f"\n📋 缓存状态:")
    print(f"股票状态记录: {len(calculator.stock_status)} 只")
    print(f"数据哈希记录: {len(calculator.data_hashes)} 个")
    
    # 检查元数据
    print(f"\n📄 元数据:")
    for key, value in calculator.metadata.items():
        print(f"  {key}: {value}")


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="简化调试脚本")
    parser.add_argument("--data-dir", required=True, help="数据目录路径")
    parser.add_argument("--cache-dir", default="advanced_indicator_cache", help="缓存目录")
    
    args = parser.parse_args()
    
    # 调试缓存问题
    debug_cache_issue(
        data_dir=args.data_dir,
        cache_dir=args.cache_dir
    )


if __name__ == "__main__":
    main() 