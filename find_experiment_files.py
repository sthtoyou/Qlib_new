#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
详细查找workflow_by_code_local_data_final.py的实验结果文件
"""

import os
import sys
from pathlib import Path
import glob

def find_experiment_files():
    """查找所有可能的实验文件"""
    
    print("=== 详细查找workflow_by_code_local_data_final.py的实验结果 ===\n")
    
    # 1. 检查当前目录
    print("1. 当前目录检查:")
    current_dir = Path.cwd()
    print(f"   当前目录: {current_dir}")
    
    # 查找.pkl文件
    pkl_files = list(current_dir.rglob("*.pkl"))
    if pkl_files:
        print("   找到的.pkl文件:")
        for file in pkl_files:
            print(f"     - {file}")
    else:
        print("   未找到.pkl文件")
    
    # 查找.json文件
    json_files = list(current_dir.rglob("*.json"))
    if json_files:
        print("   找到的.json文件:")
        for file in json_files:
            print(f"     - {file}")
    else:
        print("   未找到.json文件")
    
    # 2. 检查用户目录
    print("\n2. 用户目录检查:")
    user_home = Path.home()
    print(f"   用户目录: {user_home}")
    
    # 检查.qlib目录
    qlib_dir = user_home / ".qlib"
    if qlib_dir.exists():
        print(f"   找到.qlib目录: {qlib_dir}")
        
        # 查找实验相关文件
        experiment_files = []
        for pattern in ["**/*experiment*", "**/*workflow*", "**/*model*", "**/*prediction*"]:
            experiment_files.extend(qlib_dir.glob(pattern))
        
        if experiment_files:
            print("   找到的实验相关文件:")
            for file in experiment_files:
                if file.is_file():
                    print(f"     - {file}")
        else:
            print("   未找到实验相关文件")
    else:
        print("   未找到.qlib目录")
    
    # 3. 检查MLflow目录
    print("\n3. MLflow目录检查:")
    
    # 检查当前目录下的mlruns
    mlruns_current = current_dir / "mlruns"
    if mlruns_current.exists():
        print(f"   找到mlruns目录: {mlruns_current}")
        
        # 查找实验文件
        mlflow_files = []
        for pattern in ["**/*.pkl", "**/*.json", "**/*.csv", "**/*.yaml", "**/*.yml"]:
            mlflow_files.extend(mlruns_current.glob(pattern))
        
        if mlflow_files:
            print("   找到的MLflow文件:")
            for file in mlflow_files:
                print(f"     - {file}")
        else:
            print("   未找到MLflow文件")
    else:
        print("   未找到mlruns目录")
    
    # 检查用户目录下的mlruns
    mlruns_user = user_home / "mlruns"
    if mlruns_user.exists():
        print(f"   找到用户mlruns目录: {mlruns_user}")
        
        # 查找实验文件
        mlflow_files = []
        for pattern in ["**/*.pkl", "**/*.json", "**/*.csv", "**/*.yaml", "**/*.yml"]:
            mlflow_files.extend(mlruns_user.glob(pattern))
        
        if mlflow_files:
            print("   找到的MLflow文件:")
            for file in mlflow_files:
                print(f"     - {file}")
        else:
            print("   未找到MLflow文件")
    else:
        print("   未找到用户mlruns目录")
    
    # 4. 检查临时目录
    print("\n4. 临时目录检查:")
    temp_dirs = [
        Path(os.environ.get('TEMP', '')),
        Path(os.environ.get('TMP', '')),
        Path('/tmp') if os.name != 'nt' else None
    ]
    
    for temp_dir in temp_dirs:
        if temp_dir and temp_dir.exists():
            print(f"   检查临时目录: {temp_dir}")
            
            # 查找实验相关文件
            temp_files = []
            for pattern in ["*experiment*", "*workflow*", "*model*", "*prediction*"]:
                temp_files.extend(temp_dir.glob(pattern))
            
            if temp_files:
                print("   找到的临时文件:")
                for file in temp_files:
                    if file.is_file():
                        print(f"     - {file}")
            else:
                print("   未找到相关临时文件")
    
    # 5. 检查examples目录
    print("\n5. Examples目录检查:")
    examples_dir = current_dir / "examples"
    if examples_dir.exists():
        print(f"   找到examples目录: {examples_dir}")
        
        # 查找实验文件
        example_files = []
        for pattern in ["**/*.pkl", "**/*.json", "**/*.csv", "**/*experiment*", "**/*workflow*", "**/*model*"]:
            example_files.extend(examples_dir.glob(pattern))
        
        if example_files:
            print("   找到的examples文件:")
            for file in example_files:
                if file.is_file():
                    print(f"     - {file}")
        else:
            print("   未找到相关examples文件")
    else:
        print("   未找到examples目录")
    
    # 6. 检查是否有隐藏的实验记录
    print("\n6. 隐藏文件检查:")
    
    # 检查当前目录的隐藏文件
    hidden_files = []
    for item in current_dir.iterdir():
        if item.name.startswith('.') and item.is_file():
            hidden_files.append(item)
    
    if hidden_files:
        print("   找到的隐藏文件:")
        for file in hidden_files:
            print(f"     - {file}")
    else:
        print("   未找到隐藏文件")

if __name__ == "__main__":
    find_experiment_files() 