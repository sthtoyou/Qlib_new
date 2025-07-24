#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
增强版Qlib指标计算器使用示例
演示全量计算、增量计算、流式模式等功能
"""

import os
import sys
from pathlib import Path
from loguru import logger

# 添加项目路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator


def example_full_calculation():
    """示例：全量计算模式"""
    logger.info("📊 示例：全量计算模式")
    
    # 初始化计算器
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir=r"D:\stk_data\trd\us_data",  # 请根据实际路径修改
        enable_parallel=True,
        max_workers=8
    )
    
    # 运行全量计算
    calculator.run(
        max_stocks=5,  # 只计算前5只股票作为示例
        output_filename="example_full_calculation.csv"
    )


def example_incremental_calculation():
    """示例：增量计算模式"""
    logger.info("📊 示例：增量计算模式")
    
    # 初始化计算器（启用增量模式）
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir=r"D:\stk_data\trd\us_data",  # 请根据实际路径修改
        enable_parallel=True,
        max_workers=8,
        enable_incremental=True,
        cache_dir="example_incremental_cache"
    )
    
    # 运行增量计算
    success = calculator.calculate_indicators_incremental(
        output_file="example_incremental.csv",
        max_stocks=5,  # 只计算前5只股票作为示例
        force_update=False,  # 不强制更新
        batch_size=2,
        backup_output=True
    )
    
    if success:
        logger.info("✅ 增量计算完成")
        
        # 显示摘要
        summary = calculator.get_update_summary()
        logger.info("📊 计算摘要:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")
        
        # 分析覆盖率
        coverage = calculator.analyze_data_coverage()
        logger.info("📈 覆盖率分析:")
        logger.info(f"  覆盖率: {coverage.get('coverage_percentage', 0):.1f}%")
    else:
        logger.error("❌ 增量计算失败")


def example_streaming_mode():
    """示例：流式模式"""
    logger.info("📊 示例：流式模式")
    
    # 初始化计算器
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir=r"D:\stk_data\trd\us_data",  # 请根据实际路径修改
        enable_parallel=True,
        max_workers=8
    )
    
    # 运行流式计算
    calculator.calculate_all_indicators_streaming(
        output_file="example_streaming.csv",
        max_stocks=5,  # 只计算前5只股票作为示例
        batch_size=2
    )


def example_custom_indicators():
    """示例：自定义指标计算"""
    logger.info("📊 示例：自定义指标计算")
    
    # 初始化计算器
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir=r"D:\stk_data\trd\us_data",  # 请根据实际路径修改
        enable_parallel=False  # 单线程模式便于调试
    )
    
    # 获取股票列表
    stocks = calculator.get_available_stocks()
    if not stocks:
        logger.warning("⚠️ 没有找到股票数据")
        return
    
    # 计算单只股票的指标
    symbol = stocks[0]
    logger.info(f"🔄 计算股票 {symbol} 的指标...")
    
    result = calculator.calculate_all_indicators_for_stock(symbol)
    if result is not None and not result.empty:
        logger.info(f"✅ 成功计算 {symbol} 的指标: {len(result)} 行")
        logger.info(f"📊 指标数量: {len(result.columns)} 个")
        
        # 显示部分指标
        sample_indicators = result.columns[:10].tolist()
        logger.info(f"📋 示例指标: {sample_indicators}")
    else:
        logger.warning(f"⚠️ 无法计算 {symbol} 的指标")


def example_cache_management():
    """示例：缓存管理"""
    logger.info("📊 示例：缓存管理")
    
    # 初始化计算器（启用增量模式）
    calculator = QlibIndicatorsEnhancedCalculator(
        data_dir=r"D:\stk_data\trd\us_data",  # 请根据实际路径修改
        enable_incremental=True,
        cache_dir="example_cache_management"
    )
    
    # 显示摘要
    summary = calculator.get_update_summary()
    logger.info("📊 当前状态摘要:")
    for key, value in summary.items():
        logger.info(f"  {key}: {value}")
    
    # 列出备份文件
    backups = calculator.list_backups()
    if backups:
        logger.info("📁 备份文件列表:")
        for backup in backups:
            logger.info(f"  {backup}")
    else:
        logger.info("📁 没有备份文件")
    
    # 分析覆盖率
    coverage = calculator.analyze_data_coverage()
    logger.info("📈 数据覆盖率分析:")
    logger.info(f"  总股票数: {coverage.get('total_stocks', 0)}")
    logger.info(f"  已处理股票数: {coverage.get('processed_stocks', 0)}")
    logger.info(f"  覆盖率: {coverage.get('coverage_percentage', 0):.1f}%")


def main():
    """主函数"""
    logger.info("🚀 增强版Qlib指标计算器使用示例")
    logger.info("=" * 60)
    
    # 检查数据目录是否存在
    data_dir = r"D:\stk_data\trd\us_data"
    if not os.path.exists(data_dir):
        logger.warning(f"⚠️ 数据目录不存在: {data_dir}")
        logger.info("请修改脚本中的数据目录路径，或创建相应的数据目录")
        logger.info("示例将展示功能，但不会执行实际计算")
        return
    
    try:
        # 运行各种示例
        example_full_calculation()
        logger.info("-" * 40)
        
        example_incremental_calculation()
        logger.info("-" * 40)
        
        example_streaming_mode()
        logger.info("-" * 40)
        
        example_custom_indicators()
        logger.info("-" * 40)
        
        example_cache_management()
        
        logger.info("=" * 60)
        logger.info("🎉 所有示例运行完成！")
        
    except Exception as e:
        logger.error(f"❌ 示例运行失败: {e}")
        raise


if __name__ == "__main__":
    main() 