#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主控制脚本：统一调用PLAYLIST/EPG生成功能
运行方式：
  python main.py playlist    # 仅运行PLAYLIST生成
  python main.py epg     # 仅运行EPG生成
  python main.py all     # 顺序运行PLAYLIST+EPG
"""
import sys
import importlib

def main():
    # 动态导入子模块（也可直接 import playlist_generator, epg_generator）
    try:
        playlist_mod = importlib.import_module("playlist_generator")
        epg_mod = importlib.import_module("epg_generator")
    except ImportError as e:
        print(f"❌ 导入子脚本失败：{e}")
        print("请确保 playlist_generator.py 和 epg_generator.py 与 main.py 在同一目录！")
        sys.exit(1)

    # 参数校验
    if len(sys.argv) < 2:
        print("="*60)
        print("主控制脚本运行说明：")
        print("  1. 仅运行playlist频道生成：python main.py playlist")
        print("  2. 仅运行EPG生成：python main.py epg")
        print("  3. 顺序执行playlist+EPG：python main.py all")
        print("="*60)
        sys.exit(0)

    script_type = sys.argv[1].lower()
    # 执行对应功能
    if script_type == "playlist":
        print("🔹 开始执行playlist频道生成...")
        playlist_mod.playlist_main()
    elif script_type == "epg":
        print("🔹 开始执行EPG生成...")
        epg_mod.epg_main()
    elif script_type == "all":
        print("🔹 开始顺序执行：playlist生成 → EPG生成")
        print("="*60)
        # 第一步：执行playlist
        playlist_mod.playlist_main()
        print("\n✅ playlist生成完成，准备执行EPG...")
        print("="*60)
        # 第二步：执行EPG
        epg_mod.epg_main()
        print("\n🎉 playlist+EPG 全部执行完成！")
    else:
        print(f"❌ 不支持的参数：{script_type}")
        print("支持的参数：playlist / epg / all")
        sys.exit(1)

if __name__ == "__main__":
    main()
