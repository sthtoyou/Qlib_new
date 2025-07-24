# 增强版Qlib指标计算器

## 概述

增强版Qlib指标计算器是一个功能强大的金融指标计算工具，集成了Alpha158、Alpha360指标体系和多种技术分析指标。该工具支持全量计算、增量计算、流式计算等多种模式，具备多线程并行处理能力，能够高效处理大规模股票数据。

## 主要特性

### 🚀 性能优化
- **多线程并行计算**: 支持多只股票并行处理和单只股票指标类型并行计算
- **智能线程管理**: 自动优化线程数量，避免资源竞争
- **内存优化**: 支持流式写入模式，极大节省内存使用

### 🔄 增量计算
- **智能增量更新**: 基于"Stock X Date X Indicator"维度进行增量判断
- **数据哈希检测**: 基于MD5哈希值检测数据变化
- **日期范围分析**: 智能检测数据时间范围变化
- **自动备份**: 计算前自动备份现有结果
- **状态跟踪**: 详细记录每只股票的处理状态

### 📊 指标体系
- **Alpha158指标体系**: ~158个指标 (KBAR、价格、成交量、滚动技术指标)
- **Alpha360指标体系**: ~360个指标 (过去60天标准化价格和成交量数据)
- **技术指标**: ~60个指标 (移动平均、MACD、RSI、布林带等)
- **蜡烛图形态**: 61个形态 (锤子线、十字星、吞没形态等)
- **财务指标**: ~15个指标 (市净率、换手率、托宾Q值等)
- **波动率指标**: ~8个指标 (已实现波动率、半变差等)

**总计约695个指标**，具备去重功能和多线程加速。

## 安装要求

### 系统要求
- Python 3.7+
- Windows/Linux/macOS

### 依赖包
```bash
pip install pandas numpy talib-binary loguru
```

### 数据要求
- Qlib格式的股票数据
- 数据目录结构：
```
data/
├── features/
│   ├── stock1/
│   │   ├── open.day.bin
│   │   ├── high.day.bin
│   │   ├── low.day.bin
│   │   ├── close.day.bin
│   │   └── volume.day.bin
│   └── stock2/
│       └── ...
└── calendars/
    └── day.txt
```

## 使用方法

### 基本用法

#### 1. 全量计算模式
```bash
# 计算所有股票的指标
python scripts/qlib_indicators.py

# 只计算前10只股票
python scripts/qlib_indicators.py --max-stocks 10

# 指定输出文件名
python scripts/qlib_indicators.py --output my_indicators.csv
```

#### 2. 增强版增量计算模式
```bash
# 启用增量计算
python scripts/qlib_indicators.py --incremental --output indicators.csv

# 强制更新所有股票
python scripts/qlib_indicators.py --incremental --force-update

# 指定缓存目录
python scripts/qlib_indicators.py --incremental --cache-dir my_cache
```

#### 3. 流式模式（节省内存）
```bash
# 启用流式写入
python scripts/qlib_indicators.py --streaming --batch-size 50
```

### 高级用法

#### 1. 增量计算管理
```bash
# 查看增量计算摘要
python scripts/qlib_indicators.py --incremental --summary

# 分析数据覆盖率
python scripts/qlib_indicators.py --incremental --analyze-coverage

# 清理缓存
python scripts/qlib_indicators.py --incremental --clean-cache

# 列出备份文件
python scripts/qlib_indicators.py --incremental --list-backups

# 恢复备份
python scripts/qlib_indicators.py --incremental --restore-backup backup_file.csv
```

#### 2. 性能调优
```bash
# 禁用多线程并行计算
python scripts/qlib_indicators.py --disable-parallel

# 自定义线程数量
python scripts/qlib_indicators.py --max-workers 16

# 指定数据目录和财务数据目录
python scripts/qlib_indicators.py --data-dir ./data --financial-dir ./financial_data
```

#### 3. 调试模式
```bash
# 调试模式（只计算少量股票）
python scripts/qlib_indicators.py --log-level DEBUG --max-stocks 5
```

### 编程接口

#### 基本使用
```python
from scripts.qlib_indicators import QlibIndicatorsEnhancedCalculator

# 初始化计算器
calculator = QlibIndicatorsEnhancedCalculator(
    data_dir="path/to/qlib/data",
    enable_parallel=True,
    max_workers=8
)

# 全量计算
calculator.run(max_stocks=100, output_filename="indicators.csv")
```

#### 增量计算
```python
# 启用增量模式
calculator = QlibIndicatorsEnhancedCalculator(
    data_dir="path/to/qlib/data",
    enable_incremental=True,
    cache_dir="my_cache"
)

# 增量计算
success = calculator.calculate_indicators_incremental(
    output_file="indicators.csv",
    max_stocks=100,
    force_update=False,
    batch_size=20
)

# 查看摘要
summary = calculator.get_update_summary()
coverage = calculator.analyze_data_coverage()
```

#### 流式计算
```python
# 流式计算（节省内存）
calculator.calculate_all_indicators_streaming(
    output_file="indicators.csv",
    max_stocks=100,
    batch_size=20
)
```

## 输出格式

### CSV文件结构
输出CSV文件采用多行头部格式：

1. **第一行**: 字段名（英文列名）
2. **第二行**: 中文标签
3. **第三行开始**: 具体数据

### 标准字段顺序
输出文件严格按照以下顺序排列字段：

1. **基础字段**: Date, Symbol
2. **OHLCV数据**: Open, High, Low, Close, Volume
3. **波动率指标**: RealizedVolatility_20, NegativeSemiDeviation_20, ...
4. **蜡烛图形态**: CDL2CROWS, CDL3BLACKCROWS, ...
5. **移动平均线**: SMA_5, SMA_10, SMA_20, ...
6. **MACD指标**: MACD, MACD_Signal, MACD_Histogram, ...
7. **动量指标**: RSI_14, CCI_14, CMO_14, ...
8. **趋势指标**: ADX_14, AROON_UP, AROON_DOWN, ...
9. **价格动量**: MOM_10, ROC_10, ROCP_10, ...
10. **布林带**: BB_Upper, BB_Middle, BB_Lower
11. **随机指标**: STOCH_K, STOCH_D, STOCHF_K, ...
12. **波动率指标**: ATR_14, NATR_14, TRANGE
13. **成交量指标**: OBV, AD, ADOSC
14. **希尔伯特变换指标**: HT_DCPERIOD, HT_DCPHASE, ...
15. **价格指标**: AVGPRICE, MEDPRICE, TYPPRICE, ...
16. **统计指标**: LINEARREG, STDDEV, VAR, ...
17. **财务指标**: PriceToBookRatio, MarketCap, PERatio, ...
18. **Alpha158指标**: ALPHA158_KMID, ALPHA158_KLEN, ...
19. **Alpha360指标**: ALPHA360_CLOSE59, ALPHA360_CLOSE58, ...

### 空值处理
- 所有NaN值被替换为空字符串，以兼容SAS等统计软件
- 确保数据的一致性和可读性

## 增量计算详解

### 增量判断维度
基于"Stock X Date X Indicator"三个维度进行增量判断：

1. **Stock（股票）**: 按股票代码分别跟踪
2. **Date（日期）**: 检测数据时间范围变化
3. **Indicator（指标）**: 检测指标算法或数量变化

### 增量更新策略
1. **数据哈希检测**: 计算股票数据的MD5哈希值，检测数据变化
2. **日期范围分析**: 检测数据时间范围的扩展或收缩
3. **状态跟踪**: 记录每只股票的处理状态和指标数量
4. **智能合并**: 基于复合键（股票+日期）进行数据合并

### 缓存管理
- **元数据缓存**: 记录计算状态和配置信息
- **股票状态缓存**: 记录每只股票的处理状态
- **数据哈希缓存**: 记录数据哈希值用于变化检测
- **日期范围缓存**: 记录每只股票的数据时间范围
- **备份管理**: 自动备份和恢复功能

## 性能优化建议

### 1. 硬件配置
- **CPU**: 多核处理器，建议8核以上
- **内存**: 建议16GB以上，大数据集需要32GB+
- **存储**: SSD硬盘，提高I/O性能

### 2. 参数调优
- **线程数**: 根据CPU核心数调整，建议CPU核心数+4
- **批次大小**: 根据内存大小调整，建议20-50
- **流式模式**: 大数据集建议使用流式模式

### 3. 数据优化
- **数据预处理**: 确保数据质量和完整性
- **数据压缩**: 使用压缩格式减少存储空间
- **数据分区**: 按时间或股票分区存储

## 故障排除

### 常见问题

#### 1. 内存不足
```
解决方案:
- 使用流式模式: --streaming
- 减少批次大小: --batch-size 10
- 减少线程数: --max-workers 4
- 分批处理: --max-stocks 100
```

#### 2. 数据读取失败
```
解决方案:
- 检查数据目录路径
- 验证数据文件完整性
- 检查文件权限
- 确认数据格式正确
```

#### 3. 增量计算异常
```
解决方案:
- 清理缓存: --clean-cache
- 强制更新: --force-update
- 检查磁盘空间
- 验证备份文件完整性
```

#### 4. 性能问题
```
解决方案:
- 调整线程数
- 使用SSD硬盘
- 增加内存
- 优化数据格式
```

### 日志分析
- **INFO级别**: 正常操作信息
- **WARNING级别**: 警告信息，不影响运行
- **ERROR级别**: 错误信息，需要处理
- **DEBUG级别**: 调试信息，详细执行过程

## 示例脚本

### 基本示例
```python
# 运行示例脚本
python scripts/example_usage.py
```

### 测试脚本
```python
# 运行测试脚本
python scripts/test_qlib_indicators.py
```

## 更新日志

### v2.0 (当前版本)
- ✅ 集成增强版增量计算功能
- ✅ 支持"Stock X Date X Indicator"维度判断
- ✅ 添加数据覆盖率分析
- ✅ 优化输出格式和列顺序
- ✅ 增强缓存管理和备份功能
- ✅ 改进错误处理和日志记录

### v1.0
- ✅ 基础指标计算功能
- ✅ 多线程并行处理
- ✅ 基本增量计算
- ✅ 流式写入模式

## 贡献指南

欢迎提交Issue和Pull Request来改进这个项目。

### 开发环境设置
1. Fork项目
2. 创建功能分支
3. 提交更改
4. 创建Pull Request

### 代码规范
- 遵循PEP 8代码风格
- 添加适当的注释和文档
- 编写测试用例
- 确保向后兼容性

## 许可证

本项目采用MIT许可证，详见LICENSE文件。

## 联系方式

如有问题或建议，请通过以下方式联系：
- 提交GitHub Issue
- 发送邮件至项目维护者

---

**注意**: 使用本工具前请确保您有合法的数据访问权限，并遵守相关的数据使用协议。 