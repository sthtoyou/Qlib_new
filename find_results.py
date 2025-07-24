#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查找workflow_by_code_local_data_final.py的实验结果保存位置
"""

import os
import sys
from pathlib import Path
import qlib
from qlib.workflow import R

def find_experiment_results():
    """查找实验结果保存位置"""
    
    print("=== 查找workflow_by_code_local_data_final.py的实验结果 ===\n")
    
    # 1. 初始化qlib
    try:
        qlib.init()
        print("1. Qlib初始化成功")
        print(f"   数据路径: {qlib.config.get('provider_uri')}")
    except Exception as e:
        print(f"   Qlib初始化失败: {e}")
        return
    
    # 2. 查看实验记录
    print("\n2. 实验记录:")
    try:
        experiments = R.list_experiments()
        if experiments:
            print("   找到的实验:")
            for exp in experiments:
                print(f"   - {exp}")
        else:
            print("   未找到实验记录")
    except Exception as e:
        print(f"   获取实验记录失败: {e}")
    
    # 3. 查找特定实验名称
    experiment_name = "workflow_local_data_multi_models"
    print(f"\n3. 查找特定实验 '{experiment_name}':")
    
    try:
        experiments = R.list_experiments()
        found = False
        for exp in experiments:
            if experiment_name in exp:
                print(f"   找到匹配的实验: {exp}")
                found = True
                # 尝试加载该实验
                try:
                    with R.start(experiment_name=exp):
                        objects = R.list_objects()
                        if objects:
                            print("   该实验包含的对象:")
                            for obj in objects:
                                print(f"     - {obj}")
                        else:
                            print("   该实验未包含任何对象")
                except Exception as e:
                    print(f"   加载实验对象失败: {e}")
        
        if not found:
            print(f"   未找到包含 '{experiment_name}' 的实验")
            
    except Exception as e:
        print(f"   查找特定实验失败: {e}")
    
    # 4. 查找可能的保存位置
    print("\n4. 可能的保存位置:")
    
    # 用户目录下的.qlib文件夹
    user_qlib_path = Path.home() / ".qlib"
    if user_qlib_path.exists():
        print(f"   用户目录: {user_qlib_path}")
        
        # 查找实验相关文件夹
        experiment_found = False
        for item in user_qlib_path.rglob("*"):
            if "experiment" in str(item).lower() or "workflow" in str(item).lower():
                print(f"     - {item}")
                experiment_found = True
        
        if not experiment_found:
            print("     未找到实验相关文件夹")
    
    # 5. 查找MLflow实验记录
    print("\n5. MLflow实验记录:")
    mlruns_path = Path.cwd() / "mlruns"
    if mlruns_path.exists():
        print(f"   找到MLflow目录: {mlruns_path}")
        for item in mlruns_path.rglob("*"):
            if item.is_file() and item.suffix in ['.pkl', '.json', '.csv']:
                print(f"     - {item}")
    else:
        print("   未找到MLflow目录")
    
    # 6. 查找当前目录下的实验文件
    print("\n6. 当前目录实验文件:")
    current_dir = Path.cwd()
    experiment_files = []
    for item in current_dir.rglob("*"):
        if any(keyword in str(item).lower() for keyword in ["experiment", "workflow", "model", "prediction"]):
            if item.is_file() and item.suffix in ['.pkl', '.json', '.csv']:
                experiment_files.append(item)
    
    if experiment_files:
        print("   找到的实验文件:")
        for file in experiment_files:
            print(f"     - {file}")
    else:
        print("   未找到实验文件")

if __name__ == "__main__":
    find_experiment_results() 