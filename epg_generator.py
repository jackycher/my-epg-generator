#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立EPG生成脚本
可单独运行：python epg_generator.py
"""
import os
import sys
import json
import datetime
import xml.etree.ElementTree as ET
import gzip
import re
import urllib.request
import traceback
import hashlib
import shutil
import urllib.parse

# ===================== EPG配置区 =====================
EPG_CONFIG = {
    'ENABLE_OFFICIAL_EPG': False,
    # 外部EPG总开关
    'ENABLE_EXTERNAL_EPG': True,
    # 新增：是否保留外部EPG源中的其他频道（非txt文件里的频道）
    'ENABLE_KEEP_OTHER_CHANNELS': True,
    'EPG_SERVER_URL': "http://210.13.21.3",
    'BJcul_PATH': "./bjcul.txt",
    'EPG_SAVE_PATH': "./epg.xml",          # 精简版XML
    'EPG_GZ_PATH': "./epg.xml.gz",        # 精简版GZ
    'EPG_FULL_SAVE_PATH': "./epg_full.xml",  # 完整版XML
    'EPG_FULL_GZ_PATH': "./epg_full.xml.gz",# 完整版GZ
    'LOG_PATH': "./epg_run.log",
    'EPG_OFFSET_START': -1,
    'EPG_OFFSET_END': 3,
    'CACHE_DIR': "./epg_cache",
    'CACHE_TIMEOUT': 30,
    'CACHE_RETRY_TIMES': 2,
    'KEEP_4K_NAMES': ["CCTV4K", "CCTV4k", "爱上4K", "4K超清"],
    'CLEAN_SUFFIX': ["4k", "4K", "SDR", "HDR", "超高清", "英语", "英文"],
    'TIMEOUT': 30,
    'RETRY_TIMES': 2,
    'EXTERNAL_EPG_SOURCES': [                              
        {
            "url": "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/epg.xml.gz",
            "name": "主EPG源-zzzz0317",
            "is_official": False,
            "clean_name": True,
            "enabled": True
        },
        {
            "url": "https://epg.zsdc.eu.org/t.xml.gz",
            "name": "备用EPG源1-zsdc",
            "is_official": False,
            "clean_name": True,
            "enabled": False
        },
        {
            "url": "https://raw.githubusercontent.com/kuke31/xmlgz/main/all.xml.gz",
            "name": "备用EPG源2-e.erw.cc",
            "is_official": False,
            "clean_name": True,
            "enabled": False
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/112114.xml.gz",
            "name": "备用EPG源3-112114",
            "is_official": False,
            "clean_name": True,
            "enabled": False
        },
        {
            "url": "https://raw.githubusercontent.com/taksssss/tv/main/epg/erw.xml.gz",
            "name": "备用EPG源4-erw",
            "is_official": False,
            "clean_name": True,
            "enabled": True
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/51zmt.xml.gz",
            "name": "备用EPG源5-51zmt",
            "is_official": False,
            "clean_name": True,
            "enabled": False
        },
        {
            "url": "https://gitee.com/taksssss/tv/raw/main/epg/epgpw_cn.xml.gz",
            "name": "备用EPG源6-epgpw_cn",
            "is_official": False,
            "clean_name": True,
            "enabled": False
        }
    ],
    'PLAYLIST_FILE_PATH': "https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/playlist-zz.json",
    'PLAYLIST_FORMAT': "zz",
    'FORMAT_MAPPING': {
        "zz": {
            "channel_id_field": "id_sys",          
            "user_channel_id_field": "tvg_id",     
            "channel_url_path": ["live", "bjunicom-multicast", "addr"],
            "is_dict_format": True,                
            "url_replace_rule": None               
        },
        "raw": {
            "channel_id_field": "channelID",
            "user_channel_id_field": "userChannelID",
            "channel_url_path": ["channelURL"],
            "is_dict_format": False,               
            "url_replace_rule": ("igmp://", "rtp://")
        }
    }
}

# ===================== 工具函数 =====================
def write_log(content, section="INFO"):
    """EPG专属日志函数"""
    log_path = EPG_CONFIG['LOG_PATH']
    try:
        log_dir = os.path.dirname(log_path)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] [{section}] {content}\n")
        print(f"[{timestamp}] [{section}] {content}")
    except Exception as e:
        print(f"日志写入失败：{str(e)}")

def get_nested_value(data, path_list):
    """获取嵌套字典值"""
    if not isinstance(data, dict) or not path_list:
        return None
    current = data
    for key in path_list:
        if key not in current:
            return None
        current = current[key]
    return current

def compress_xml_to_gz(xml_path, gz_path):
    """压缩XML为GZ"""
    try:
        write_log(f"开始压缩：{xml_path} → {gz_path}", "GZ_COMPRESS")
        with open(xml_path, 'rb') as f_in:
            with gzip.open(gz_path, 'wb', compresslevel=6) as f_out:
                shutil.copyfileobj(f_in, f_out)
        if os.path.exists(gz_path):
            gz_size = os.path.getsize(gz_path)
            xml_size = os.path.getsize(xml_path)
            ratio = round((1 - gz_size/xml_size) * 100, 2)
            write_log(f"压缩成功！原{xml_size}字节 → 压缩后{gz_size}字节（{ratio}%）", "GZ_SUCCESS")
            print(f"  → 压缩完成：{gz_path}（{ratio}%）")
            return True
        else:
            write_log("压缩文件生成失败", "GZ_FAIL")
            return False
    except Exception as e:
        write_log(f"压缩失败：{str(e)}", "GZ_ERROR")
        print(f"  ❌ 压缩失败：{str(e)}")
        return False

def get_url_md5(url):
    """生成URL的MD5"""
    encoded_url = urllib.parse.quote_plus(url).encode('utf-8')
    return hashlib.md5(encoded_url).hexdigest()

def download_with_cache(url, cache_dir, timeout=30, retry=2):
    """带缓存的下载"""
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
    
    url_md5 = get_url_md5(url)
    cache_file = os.path.join(cache_dir, f"{url_md5}.txt")
    old_cache_file = os.path.join(cache_dir, f"{url_md5}_old.txt")
    
    # 备份旧缓存
    if os.path.exists(cache_file):
        try:
            if os.path.exists(old_cache_file):
                os.remove(old_cache_file)
            os.rename(cache_file, old_cache_file)
            write_log(f"备份旧缓存：{old_cache_file}", "CACHE")
        except Exception as e:
            write_log(f"备份缓存失败：{e}", "CACHE_ERROR")
    
    # 下载新文件
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    download_success = False
    for i in range(retry + 1):
        try:
            write_log(f"下载（重试{i}/{retry}）：{url}", "DOWNLOAD")
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as res:
                if res.status == 200:
                    with open(cache_file, 'wb') as f:
                        f.write(res.read())
                    download_success = True
                    write_log(f"下载成功，缓存到：{cache_file}", "DOWNLOAD_SUCCESS")
                    break
                else:
                    write_log(f"下载失败，状态码：{res.status}", "DOWNLOAD_ERROR")
        except Exception as e:
            write_log(f"下载重试{i}失败：{e}", "DOWNLOAD_ERROR")
            continue
    
    # 处理结果
    if download_success:
        if os.path.exists(old_cache_file):
            os.remove(old_cache_file)
        return cache_file
    else:
        if os.path.exists(old_cache_file):
            os.rename(old_cache_file, cache_file)
            write_log(f"下载失败，使用旧缓存：{cache_file}", "CACHE_FALLBACK")
            return cache_file
        else:
            write_log(f"下载失败且无缓存：{url}", "CACHE_FATAL")
            return None

def get_local_path(path):
    """处理本地/远程路径"""
    if path.startswith(('http://', 'https://')):
        local_file = download_with_cache(
            path, 
            EPG_CONFIG['CACHE_DIR'],
            EPG_CONFIG['CACHE_TIMEOUT'],
            EPG_CONFIG['CACHE_RETRY_TIMES']
        )
        if not local_file or not os.path.exists(local_file):
            raise Exception(f"远程文件处理失败：{path}")
        return local_file
    else:
        if not os.path.exists(path):
            raise Exception(f"本地文件不存在：{path}")
        return path

def clean_channel_name(raw_name):
    """清理频道名"""
    if not raw_name:
        return ""
    raw_name = str(raw_name)
    
    # 保留4K相关标识
    if "4K" in raw_name and any(key in raw_name for key in ["CCTV4K", "4K超高清", "爱上4K"]):
        return raw_name.replace("-", "").replace(" ", "")
    
    if raw_name in EPG_CONFIG['KEEP_4K_NAMES']:
        return raw_name
    
    raw_name = raw_name.replace("-", "").replace(" ", "")
    suffix_pattern = r"(\s*[-_()]?\s*(4K|SDR|HDR|超清))+$"
    clean_name = re.sub(suffix_pattern, "", raw_name, flags=re.IGNORECASE).strip()
    return re.sub(r"\s+", "", clean_name)

def fuzzy_match(local_clean_name, ext_names, clean_ext_name=True):
    """模糊匹配频道名"""
    if not local_clean_name:
        return None
    
    # CGTN纪录优先匹配
    if "CGTN" in local_clean_name and "纪录" in local_clean_name:
        for ext_name in ext_names:
            ext_clean = clean_channel_name(ext_name) if clean_ext_name else ext_name.strip().replace(" ", "")
            if "CGTN" in ext_clean and "纪录" in ext_clean and "英文" in ext_clean:
                return ext_name
            elif "CGTN" in ext_clean and "纪录" in ext_clean:
                return ext_name
    
    # 提取核心标识
    is_cctv4_europe = "CCTV4" in local_clean_name and "欧洲" in local_clean_name
    is_cctv4_america = "CCTV4" in local_clean_name and "美洲" in local_clean_name
    is_cctv4k = "CCTV4K" in local_clean_name
    local_is_4k = is_cctv4k or local_clean_name in EPG_CONFIG['KEEP_4K_NAMES']

    cctv_pattern = re.compile(r'CCTV(4K|\d+\+?)')
    local_cctv_tag = cctv_pattern.search(local_clean_name).group(1) if cctv_pattern.search(local_clean_name) else None
    
    # 预处理外部频道
    ext_candidate = []
    for ext_name in ext_names:
        ext_clean = clean_channel_name(ext_name) if clean_ext_name else ext_name.strip().replace(" ", "")
        
        if not local_is_4k and "4K" in ext_clean:
            continue
        
        ext_cctv_tag = cctv_pattern.search(ext_clean).group(1) if cctv_pattern.search(ext_clean) else None
        ext_candidate.append({
            "clean": ext_clean,
            "tag": ext_cctv_tag,
            "original": ext_name,
            "len": len(ext_clean)
        })
    
    # 精准匹配
    for ext in ext_candidate:
        if local_clean_name == ext["clean"]:
            return ext["original"]
    
    # CCTV4欧洲/美洲优先
    if is_cctv4_europe or is_cctv4_america:
        region_key = "欧洲" if is_cctv4_europe else "美洲"
        region_matched = [ext for ext in ext_candidate if ext["tag"] == "4" and region_key in ext["clean"]]
        if region_matched:
            region_matched.sort(key=lambda x: x["len"])
            return region_matched[0]["original"]
    
    # CCTV4K匹配
    if is_cctv4k:
        cctv4k_matched = [ext for ext in ext_candidate if "CCTV4K" in ext["clean"]]
        if cctv4k_matched:
            cctv4k_matched.sort(key=lambda x: x["len"])
            return cctv4k_matched[0]["original"]
    
    # CCTV标识精准匹配
    if local_cctv_tag:
        tag_matched = [ext for ext in ext_candidate if ext["tag"] == local_cctv_tag]
        if tag_matched:
            tag_matched.sort(key=lambda x: x["len"])
            return tag_matched[0]["original"]
    
    # 正向包含匹配
    include_matched = [
        ext for ext in ext_candidate
        if local_clean_name in ext["clean"] and ext["len"] <= len(local_clean_name) + 10
    ]
    if include_matched:
        include_matched.sort(key=lambda x: x["len"])
        return include_matched[0]["original"]
    
    # 兜底匹配（去掉+号）
    local_no_plus = local_clean_name.replace("+", "")
    for ext in ext_candidate:
        ext_no_plus = ext["clean"].replace("+", "")
        if local_no_plus == ext_no_plus:
            return ext["original"]
    
    return None

def extract_program_title(prog_elem):
    """提取节目标题"""
    title_zh = prog_elem.find(".//title[@lang='zh']")
    if title_zh is not None and title_zh.text is not None:
        title = title_zh.text.strip()
        if title:
            return title
    
    title_any = prog_elem.find(".//title")
    if title_any is not None and title_any.text is not None:
        title = title_any.text.strip()
        if title:
            return title
    
    return "未知节目"

def download_url(url):
    """轻量化下载"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    for i in range(EPG_CONFIG['RETRY_TIMES']):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=EPG_CONFIG['TIMEOUT']) as res:
                if res.status == 200:
                    return res.read()
            write_log(f"下载失败：{url} 状态码：{res.status}", "ERROR")
        except Exception as e:
            write_log(f"下载重试{i+1}失败：{url} {str(e)}", "ERROR")
    return None

def parse_external_epg(epg_data, is_official=False):
    """解析外部EPG（增强版：返回完整的频道信息和节目）"""
    external_epg_map = {}  # 用于匹配的映射（key: 频道标识，value: 节目列表）
    ext_channel_identifiers = []  # 频道标识列表
    id_to_name_map = {}  # id到频道名的映射
    # 新增：完整的频道信息（保留所有频道）
    full_channel_info = {}  # key: 频道id，value: {id, main_name, aliases}
    # 新增：完整的节目信息（保留所有节目）
    full_program_info = []  # 所有节目列表 [{"channel_id": "", "start": "", "stop": "", "title": ""}]
    
    try:
        # 解压
        try:
            epg_data = gzip.decompress(epg_data)
        except Exception as e:
            write_log(f"解压失败（非GZ）：{str(e)}", "EPG_PARSE_WARN")
        
        # 解析XML
        ext_xml = ET.fromstring(epg_data.decode("utf-8", errors="ignore"))
        
        # 提取所有频道（完整保留）
        for channel in ext_xml.findall(".//channel"):
            cid = channel.get("id", "")
            if not cid:
                continue
            aliases = [elem.text.strip() for elem in channel.findall("display-name") if elem.text and elem.text.strip()]
            main_name = aliases[0] if aliases else cid
            full_channel_info[cid] = {
                "id": cid,
                "main_name": main_name,
                "aliases": aliases
            }
            id_to_name_map[cid] = main_name
            ext_channel_identifiers.append(main_name if not is_official else cid)
        
        # 提取所有节目（完整保留）
        for prog in ext_xml.findall(".//programme"):
            cid = prog.get("channel", "")
            start = prog.get("start")
            stop = prog.get("stop")
            if not cid or not start or not stop:
                continue
            if cid not in full_channel_info:
                continue
            
            title = extract_program_title(prog)
            full_program_info.append({
                "channel_id": cid,
                "start": start,
                "stop": stop,
                "title": title
            })
            
            # 维护原有映射（兼容旧逻辑）
            key = cid if is_official else full_channel_info[cid]["main_name"]
            if key not in external_epg_map:
                external_epg_map[key] = []
            external_epg_map[key].append({
                "start": start,
                "stop": stop,
                "title": title
            })
        
        ext_channel_identifiers = list(external_epg_map.keys())
        write_log(f"EPG解析完成 - 频道{len(full_channel_info)}个（总），匹配用{len(ext_channel_identifiers)}个，节目{len(full_program_info)}条（总）", "EPG_PARSE_DETAIL")
    
    except Exception as e:
        error_info = f"解析失败：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "EPG_PARSE_ERROR")
    
    # 返回增强后的结果：原有结果 + 完整频道/节目信息
    return external_epg_map, ext_channel_identifiers, id_to_name_map, full_channel_info, full_program_info

def generate_unique_ext_channel_id(existing_ids, prefix="ext_"):
    """生成唯一的外部频道ID，避免和已有ID冲突"""
    counter = 1
    while True:
        new_id = f"{prefix}{counter}"
        if new_id not in existing_ids:
            return new_id
        counter += 1

# ===================== 主函数 =====================
def epg_main():
    """EPG生成主逻辑（可被主文件导入调用）"""
    config = EPG_CONFIG
    # 初始化日志
    if os.path.exists(config['LOG_PATH']):
        os.remove(config['LOG_PATH'])
    write_log("="*60 + " EPG生成脚本开始运行 " + "="*60, "START")
    start_time = datetime.datetime.now()
    
    # 验证格式配置
    if config['PLAYLIST_FORMAT'] not in config['FORMAT_MAPPING']:
        error_msg = f"不支持的格式：{config['PLAYLIST_FORMAT']}，支持：{list(config['FORMAT_MAPPING'].keys())}"
        write_log(error_msg, "FATAL")
        print(f"❌ {error_msg}")
        return
    format_config = config['FORMAT_MAPPING'][config['PLAYLIST_FORMAT']]
    write_log(f"使用格式配置：{config['PLAYLIST_FORMAT']}", "CONFIG")
    write_log(f"官方EPG：{'开启' if config['ENABLE_OFFICIAL_EPG'] else '关闭'}", "CONFIG")
    write_log(f"外部EPG：{'开启' if config['ENABLE_EXTERNAL_EPG'] else '关闭'}", "CONFIG")
    write_log(f"保留其他频道：{'开启' if config['ENABLE_KEEP_OTHER_CHANNELS'] else '关闭'}", "CONFIG")
    
    # 新增：存储所有外部源的完整频道和节目
    all_external_channels = {}  # key: 频道id，value: 频道信息（去重）
    all_external_programs = []   # 所有外部节目（去重前）
    # 新增：外部频道原ID到新ID的映射（解决ID冲突）
    ext_id_mapping = {}  # key: 外部原ID，value: 新的唯一ID
    
    try:
        # 步骤1：读取bjcul.txt
        write_log("开始读取bjcul.txt", "STEP1")
        bjcul_channel_map = {}
        all_bjcul_rtp_urls = []
        
        bjcul_local_path = get_local_path(config['BJcul_PATH'])
        valid_line_count = 0
        filtered_line_count = 0
        with open(bjcul_local_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or "#genre#" in line or ',' not in line:
                    filtered_line_count += 1
                    continue
                
                raw_name, rtp_url = line.split(',', 1)
                raw_name = raw_name.strip()
                rtp_url = rtp_url.strip()
                clean_name = clean_channel_name(raw_name)
                bjcul_channel_map[rtp_url] = {"raw_name": raw_name, "clean_name": clean_name}
                all_bjcul_rtp_urls.append(rtp_url)
                valid_line_count += 1
        
        all_bjcul_rtp_urls = list(set(all_bjcul_rtp_urls))
        total_valid_channels = len(all_bjcul_rtp_urls)
        print(f"[1/7] 读取bjcul.txt：{total_valid_channels} 个有效频道")
        write_log(f"读取完成 - 过滤{filtered_line_count}行，有效{total_valid_channels}个", "STEP1")

        # 步骤2：匹配频道ID
        write_log(f"开始匹配频道ID：{config['PLAYLIST_FILE_PATH']}", "STEP2")
        matched_channels = {}
        unmatched_bjcul_channels = []
        playlist_local_path = get_local_path(config['PLAYLIST_FILE_PATH'])
        
        with open(playlist_local_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)
        
        channel_items = [(name, info) for name, info in raw_data.items()] if format_config["is_dict_format"] else [(f"channel_{idx}", item) for idx, item in enumerate(raw_data)]
        match_success_count = 0
        
        for channel_name, channel_info in channel_items:
            channel_url = get_nested_value(channel_info, format_config["channel_url_path"])
            if not channel_url:
                write_log(f"频道{channel_name}无URL", "STEP2_WARN")
                continue
            
            rtp_url = channel_url
            if format_config["url_replace_rule"]:
                old_str, new_str = format_config["url_replace_rule"]
                if channel_url.startswith(old_str):
                    rtp_url = channel_url.replace(old_str, new_str, 1)
            
            if rtp_url in bjcul_channel_map:
                channel_id = channel_info.get(format_config["channel_id_field"])
                user_channel_id = channel_info.get(format_config["user_channel_id_field"])
                
                if not channel_id:
                    write_log(f"频道{channel_name}无{format_config['channel_id_field']}", "STEP2_WARN")
                    continue
                if not user_channel_id:
                    user_channel_id = f"UN_{str(channel_id)[:8]}"
                
                bjcul_info = bjcul_channel_map[rtp_url]
                matched_channels[channel_id] = {
                    "raw_name": bjcul_info["raw_name"],
                    "clean_name": bjcul_info["clean_name"],
                    "local_num": str(user_channel_id),
                    "rtp_url": rtp_url,
                    "channel_name": channel_name
                }
                match_success_count += 1
        
        # 收集未匹配频道
        matched_rtp_urls = [v['rtp_url'] for v in matched_channels.values()]
        for rtp_url in all_bjcul_rtp_urls:
            if rtp_url not in matched_rtp_urls and rtp_url in bjcul_channel_map:
                unmatched_bjcul_channels.append({
                    "type": "unmatched_id",
                    "raw_name": bjcul_channel_map[rtp_url]["raw_name"],
                    "clean_name": bjcul_channel_map[rtp_url]["clean_name"],
                    "rtp_url": rtp_url,
                    "local_num": None
                })
        
        unmatched_count = len(unmatched_bjcul_channels)
        print(f"[2/7] 匹配频道ID：{match_success_count} 成功，{unmatched_count} 未匹配")
        write_log(f"匹配完成 - 成功{match_success_count}个，未匹配{unmatched_count}个", "STEP2")

        # 步骤3：处理官方EPG
        write_log("开始处理官方EPG", "STEP3")
        programme_list = []
        official_fail_count = 0
        
        if config['ENABLE_OFFICIAL_EPG']:
            datetime_now = datetime.datetime.now()
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                raw_name = channel_info["raw_name"]
                local_num = channel_info["local_num"]
                download_fail = True
                channel_prog_count = 0
                
                for day_offset in range(config['EPG_OFFSET_START'], config['EPG_OFFSET_END']):
                    datestr = (datetime_now + datetime.timedelta(days=day_offset)).strftime("%Y%m%d")
                    url = f"{config['EPG_SERVER_URL']}/schedules/{channel_code}_{datestr}.json"
                    data = download_url(url)
                    if not data:
                        continue
                    
                    try:
                        epg_data = json.loads(data.decode("utf-8"))
                        for schedule in epg_data.get("schedules", []):
                            start_str = schedule.get("starttime", schedule.get("showStarttime", ""))
                            end_str = schedule.get("endtime", start_str)
                            if not start_str or not end_str:
                                continue
                            try:
                                start_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S")
                                end_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")
                            except:
                                continue
                            title = schedule.get("title", "").strip() or "未知节目"
                            time_format = "%Y%m%d%H%M%S +0800"
                            programme_list.append({
                                "channel": local_num,
                                "start": start_time.strftime(time_format),
                                "stop": end_time.strftime(time_format),
                                "title": title
                            })
                            channel_prog_count += 1
                        download_fail = False
                    except Exception as e:
                        write_log(f"解析{raw_name}({channel_code})失败：{str(e)}", "STEP3_ERROR")
                        continue
                
                if download_fail:
                    unmatched_bjcul_channels.append({
                        "type": "official_fail",
                        "raw_name": raw_name,
                        "clean_name": channel_info["clean_name"],
                        "rtp_url": channel_info["rtp_url"],
                        "local_num": local_num
                    })
                    official_fail_count += 1
                else:
                    write_log(f"{raw_name}({channel_code})下载{channel_prog_count}条节目", "STEP3_DETAIL")
        else:
            write_log("官方EPG关闭，所有匹配ID的频道使用外部源", "STEP3_SKIP")
            for channel_code in matched_channels.keys():
                channel_info = matched_channels[channel_code]
                unmatched_bjcul_channels.append({
                    "type": "official_skip",
                    "raw_name": channel_info["raw_name"],
                    "clean_name": channel_info["clean_name"],
                    "rtp_url": channel_info["rtp_url"],
                    "local_num": channel_info["local_num"]
                })
            official_fail_count = len(matched_channels)
        
        total_pending_channels = len(unmatched_bjcul_channels)
        print(f"[3/7] 官方EPG处理：{len(programme_list)} 条节目，{official_fail_count} 个需匹配外部源")
        write_log(f"官方EPG完成 - 节目{len(programme_list)}条，需外部匹配{official_fail_count}个", "STEP3")

        # 步骤4：多EPG源匹配
        write_log("开始多EPG源匹配", "STEP4")
        temp_local_num_prefix = "unm_"
        temp_num_counter = 1
        total_matched_by_external = 0
        pending_channels = unmatched_bjcul_channels
        
        if not config['ENABLE_EXTERNAL_EPG']:
            write_log("外部EPG总开关关闭，跳过所有外部源匹配", "STEP4_SKIP_ALL")
            print(f"[4/7] 外部EPG总开关关闭，跳过所有外部源匹配")
        else:
            # 遍历外部源（过滤掉disabled的源）
            enabled_sources = [s for s in config['EXTERNAL_EPG_SOURCES'] if s.get("enabled", True)]
            write_log(f"外部EPG总开关开启，有效源数量：{len(enabled_sources)}（总源数：{len(config['EXTERNAL_EPG_SOURCES'])}）", "STEP4_SOURCE_COUNT")
            
            for source_idx, epg_source in enumerate(enabled_sources):
                if len(pending_channels) == 0:
                    write_log("无待匹配频道，终止匹配", "STEP4_TERMINATE")
                    break
                
                source_name = epg_source["name"]
                source_url = epg_source["url"]
                is_official = epg_source.get("is_official", False)
                clean_name = epg_source.get("clean_name", True)
                
                write_log(f"处理第{source_idx+1}个源：{source_name} ({source_url})", "STEP4_SOURCE")
                print(f"[4/7] 匹配外部源{source_idx+1}：{source_name}（待匹配{len(pending_channels)}个）")
                
                # 下载EPG源
                epg_data = download_url(source_url)
                if not epg_data:
                    write_log(f"源{source_name}下载失败", "STEP4_SOURCE_FAIL")
                    continue
                
                # 解析EPG源（增强版）
                epg_map, epg_identifiers, id_to_name_map, full_channel_info, full_program_info = parse_external_epg(epg_data, is_official)
                if not epg_map or len(epg_identifiers) == 0:
                    write_log(f"源{source_name}解析失败", "STEP4_SOURCE_PARSE_FAIL")
                    continue
                
                # 新增：收集外部源的所有频道和节目（去重+ID重映射）
                if config['ENABLE_KEEP_OTHER_CHANNELS']:
                    # 先收集已存在的频道ID（避免重复）
                    # 修复：只提取matched_channels中的local_num，而非整个字典
                    matched_local_nums = [v['local_num'] for v in matched_channels.values()]
                    existing_ids = set(matched_local_nums) | set([c['local_num'] for c in unmatched_bjcul_channels if c['local_num']])
                    existing_ids.update(all_external_channels.keys())
    
                    # 合并频道（按id去重+ID重映射）
                    for cid, channel_info in full_channel_info.items():
                        # 如果原ID已存在，生成新ID
                        if cid in existing_ids or cid in ext_id_mapping:
                            if cid not in ext_id_mapping:
                                new_id = generate_unique_ext_channel_id(existing_ids | set(ext_id_mapping.values()))
                                ext_id_mapping[cid] = new_id
                            use_id = ext_id_mapping[cid]
                        else:
                            use_id = cid
        
                        if use_id not in all_external_channels:
                            all_external_channels[use_id] = {
                                "original_id": cid,
                                "main_name": channel_info["main_name"],
                                "aliases": channel_info["aliases"]
                            }
                        existing_ids.add(use_id)

                    
                    # 合并节目（使用新ID，并且统一键名为channel）
                    for prog in full_program_info:
                        original_cid = prog["channel_id"]
                        use_cid = ext_id_mapping.get(original_cid, original_cid)
                        # 关键修复：将channel_id改为channel，统一键名
                        all_external_programs.append({
                            "channel": use_cid,  # 替换channel_id为channel
                            "start": prog["start"],
                            "stop": prog["stop"],
                            "title": prog["title"]
                        })
                
                # 原有匹配逻辑（修改：保留频道供后续源补充节目）
                prog_duplicate_key = set()
                # 先初始化已有的节目去重键（频道+开始时间）
                for prog in programme_list:
                    if prog.get("channel") and prog.get("start"):
                        prog_duplicate_key.add((prog["channel"], prog["start"]))

                matched_in_this_source = 0
                new_pending_channels = []  # 保留所有频道，让后续源继续补充
                for channel in pending_channels:
                    clean_name_local = channel["clean_name"]
                    raw_name = channel["raw_name"]
                    local_num = channel["local_num"]
                    channel_matched = False  # 标记当前频道是否在本源性匹配到节目
                    
                    # 官方源：ID匹配
                    if is_official and local_num and not local_num.startswith(temp_local_num_prefix):
                        if local_num in epg_identifiers and epg_map.get(local_num):
                            ext_channel_name = id_to_name_map.get(local_num, f"ID_{local_num}")
                            ext_progs = epg_map[local_num]
                            new_prog_count = 0  # 新增节目数（去重后）
                            for prog in ext_progs:
                                # 生成去重键：频道+开始时间（核心去重逻辑）
                                key = (local_num, prog["start"])
                                if key not in prog_duplicate_key:
                                    programme_list.append({
                                        "channel": local_num,
                                        "start": prog["start"],
                                        "stop": prog["stop"],
                                        "title": prog["title"]
                                    })
                                    prog_duplicate_key.add(key)
                                    new_prog_count += 1
                            if new_prog_count > 0:
                                matched_in_this_source += 1
                                total_matched_by_external += 1
                                write_log(f"{raw_name}({local_num})从{source_name}补充{new_prog_count}条节目（总计{len(ext_progs)}条）", "STEP4_MATCH_SUCCESS")
                            channel_matched = True
                    else:
                        # 非官方源：名称匹配
                        match_ext_name = fuzzy_match(clean_name_local, epg_identifiers, clean_name)
                        if match_ext_name and match_ext_name in epg_map:
                            ext_progs = epg_map[match_ext_name]
                            if ext_progs:
                                if not local_num:
                                    local_num = f"{temp_local_num_prefix}{temp_num_counter}"
                                    temp_num_counter += 1
                                    channel["local_num"] = local_num
                                
                                new_prog_count = 0  # 新增节目数（去重后）
                                for prog in ext_progs:
                                    # 生成去重键：频道+开始时间（核心去重逻辑）
                                    key = (local_num, prog["start"])
                                    if key not in prog_duplicate_key:
                                        programme_list.append({
                                            "channel": local_num,
                                            "start": prog["start"],
                                            "stop": prog["stop"],
                                            "title": prog["title"]
                                        })
                                        prog_duplicate_key.add(key)
                                        new_prog_count += 1
                                if new_prog_count > 0:
                                    matched_in_this_source += 1
                                    total_matched_by_external += 1
                                    write_log(f"{raw_name}({local_num})从{source_name}补充{new_prog_count}条节目（总计{len(ext_progs)}条）", "STEP4_MATCH_SUCCESS")
                                channel_matched = True
                    
                    # 关键修改：无论是否匹配成功，都保留频道到待匹配列表
                    new_pending_channels.append(channel)

                pending_channels = new_pending_channels  # 所有频道都保留，供后续源补充
                write_log(f"{source_name}匹配完成 - 补充{matched_in_this_source}个频道的节目，剩余{len(pending_channels)}个待补充", "STEP4_SOURCE_SUMMARY")
                print(f"  → 源{source_idx+1}补充{matched_in_this_source}个频道的节目，剩余{len(pending_channels)}个待补充")


        total_unmatched_final = len(pending_channels)
        print(f"[5/7] 多源匹配完成：总计{total_matched_by_external}个，剩余{total_unmatched_final}个未匹配")
        write_log(f"多源匹配汇总 - 成功{total_matched_by_external}个，未匹配{total_unmatched_final}个", "STEP4_FINAL_SUMMARY")

        # 步骤5：生成XML（先精简版，再完整版）
        write_log("开始生成精简版EPG XML", "STEP5_LITE")
        # ===== 生成精简版XML（仅包含txt中的频道）=====
        root_lite = ET.Element("tv", {
            "generator-info-name": "MY EPG Generator v4.1 (Lite)",
            "generator-info-url": "https://github.com/jackycher/my-epg-generator",
            "generated-time": "UTC" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # 第一步：添加txt匹配的频道
        channel_add_count = 0
        for channel_code in matched_channels.keys():
            channel_info = matched_channels[channel_code]
            local_num = channel_info["local_num"]
            raw_name = channel_info["raw_name"]
            channel_elem = ET.SubElement(root_lite, "channel", {"id": local_num})
            ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = raw_name
            channel_add_count += 1
        
        # 第二步：添加临时频道
        temp_channel_add_count = 0
        for channel in unmatched_bjcul_channels:
            if channel["local_num"] and channel["local_num"].startswith(temp_local_num_prefix):
                local_num = channel["local_num"]
                raw_name = channel["raw_name"]
                channel_elem = ET.SubElement(root_lite, "channel", {"id": local_num})
                ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = raw_name
                temp_channel_add_count += 1
        
        # 第三步：添加节目（仅txt相关）
        seen_progs_lite = set()
        sorted_progs_lite = sorted(programme_list, key=lambda x: (x["channel"], x["start"]))
        prog_add_count_lite = 0
        non_unknown_count_lite = 0
        
        for prog in sorted_progs_lite:
            if not prog.get("channel") or not prog.get("start") or not prog.get("title"):
                continue
            # 去重键：频道+开始时间+标题
            key = (prog["channel"], prog["start"], prog["title"])
            if key in seen_progs_lite:
                continue
            seen_progs_lite.add(key)
            
            prog_elem = ET.SubElement(root_lite, "programme", {
                "start": prog["start"],
                "stop": prog["stop"],
                "channel": prog["channel"]
            })
            ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
            prog_add_count_lite += 1
            if prog["title"] != "未知节目":
                non_unknown_count_lite += 1
        
        # 保存精简版XML
        ET.ElementTree(root_lite).write(
            config['EPG_SAVE_PATH'],
            encoding="UTF-8",
            xml_declaration=True,
            short_empty_elements=False
        )
        os.chmod(config['EPG_SAVE_PATH'], 0o644)
        print(f"[6/7] 生成精简版EPG：{config['EPG_SAVE_PATH']}（{prog_add_count_lite}条节目）")
        write_log(f"精简版XML生成成功：{config['EPG_SAVE_PATH']}，总频道{channel_add_count + temp_channel_add_count}个（txt{channel_add_count} + 临时{temp_channel_add_count}）", "STEP5_LITE")
        
        # 压缩精简版为GZ
        print("[6/7] 压缩精简版为epg.xml.gz...")
        compress_xml_to_gz(config['EPG_SAVE_PATH'], config['EPG_GZ_PATH'])

        # ===== 生成完整版XML（包含外部所有频道）=====
        other_channel_add_count = 0  # 初始化变量，避免未定义
        prog_add_count_full = 0
        non_unknown_count_full = 0
        if config['ENABLE_KEEP_OTHER_CHANNELS'] and all_external_channels:
            write_log("开始生成完整版EPG XML", "STEP5_FULL")
            # 复制精简版的根节点
            root_full = ET.Element("tv", {
                "generator-info-name": "MY EPG Generator v4.1 (Full)",
                "generator-info-url": "https://github.com/jackycher/my-epg-generator",
                "generated-time": "UTC" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
            # 第一步：复制精简版的所有频道
            for channel_elem in root_lite.findall(".//channel"):
                new_channel = ET.SubElement(root_full, "channel", {"id": channel_elem.get("id")})
                for dn_elem in channel_elem.findall(".//display-name"):
                    ET.SubElement(new_channel, "display-name", {"lang": "zh"}).text = dn_elem.text
            
            # 第二步：添加外部源的其他频道
            other_channel_add_count = 0
            existing_channel_ids = set([c.get("id") for c in root_full.findall(".//channel")])
            for cid, channel_info in all_external_channels.items():
                if cid in existing_channel_ids:
                    continue
                channel_elem = ET.SubElement(root_full, "channel", {"id": cid})
                # 添加主名称
                ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = channel_info["main_name"]
                # 添加别名（可选）
                for alias in channel_info["aliases"][1:]:  # 跳过第一个（主名称）
                    ET.SubElement(channel_elem, "display-name", {"lang": "zh"}).text = alias
                other_channel_add_count += 1
            write_log(f"添加外部源其他频道：{other_channel_add_count}个", "STEP5_FULL_CHANNELS")
            
            # 第三步：准备所有节目（精简版 + 外部源节目）
            all_programs_full = []
            all_programs_full.extend(programme_list)
            all_programs_full.extend(all_external_programs)
            
            # 第四步：添加节目（去重，增加数据校验）
            seen_progs_full = set()
            # 修复：排序前过滤无效数据，避免KeyError
            valid_progs_full = []
            for prog in all_programs_full:
                # 校验必要字段是否存在
                if isinstance(prog, dict) and "channel" in prog and "start" in prog and "title" in prog:
                    valid_progs_full.append(prog)
                else:
                    write_log(f"过滤无效节目数据：{prog}", "STEP5_FULL_WARN")
            
            # 对有效数据排序
            sorted_progs_full = sorted(valid_progs_full, key=lambda x: (x["channel"], x["start"]))
            prog_add_count_full = 0
            non_unknown_count_full = 0
            
            for prog in sorted_progs_full:
                if not prog.get("channel") or not prog.get("start") or not prog.get("title"):
                    continue
                # 去重键：频道+开始时间+标题
                key = (prog["channel"], prog["start"], prog["title"])
                if key in seen_progs_full:
                    continue
                seen_progs_full.add(key)
                
                prog_elem = ET.SubElement(root_full, "programme", {
                    "start": prog["start"],
                    "stop": prog["stop"],
                    "channel": prog["channel"]
                })
                ET.SubElement(prog_elem, "title", {"lang": "zh"}).text = prog["title"]
                prog_add_count_full += 1
                if prog["title"] != "未知节目":
                    non_unknown_count_full += 1
            
            # 保存完整版XML
            ET.ElementTree(root_full).write(
                config['EPG_FULL_SAVE_PATH'],
                encoding="UTF-8",
                xml_declaration=True,
                short_empty_elements=False
            )
            os.chmod(config['EPG_FULL_SAVE_PATH'], 0o644)
            print(f"[6/7] 生成完整版EPG：{config['EPG_FULL_SAVE_PATH']}（去重后{prog_add_count_full}条，新增外部频道{other_channel_add_count}个）")
            write_log(f"完整版XML生成成功：{config['EPG_FULL_SAVE_PATH']}，总频道{channel_add_count + temp_channel_add_count + other_channel_add_count}个", "STEP5_FULL")
            
            # 压缩完整版为GZ
            print("[6/7] 压缩完整版为epg_full.xml.gz...")
            compress_xml_to_gz(config['EPG_FULL_SAVE_PATH'], config['EPG_FULL_GZ_PATH'])
        else:
            write_log("未开启保留其他频道，跳过完整版生成", "STEP5_FULL_SKIP")

        # 步骤6：统计结果
        write_log("统计运行结果", "STEP6")
        end_time = datetime.datetime.now()
        run_duration = (end_time - start_time).total_seconds()
        
        summary = {
            "总耗时(秒)": round(run_duration, 2),
            "bjcul有效频道": total_valid_channels,
            "匹配ID频道": match_success_count,
            "外部匹配成功": total_matched_by_external,
            "最终未匹配": total_unmatched_final,
            "精简版EPG频道数": channel_add_count + temp_channel_add_count,
            "精简版EPG节目数(去重)": prog_add_count_lite,
            "精简版非未知节目": non_unknown_count_lite
        }
        if config['ENABLE_KEEP_OTHER_CHANNELS'] and all_external_channels:
            summary["完整版EPG频道数"] = channel_add_count + temp_channel_add_count + other_channel_add_count
            summary["完整版EPG节目数(去重)"] = prog_add_count_full
            summary["完整版非未知节目"] = non_unknown_count_full
        
        print(f"[7/7] 运行完成：耗时{summary['总耗时(秒)']}秒，精简版非未知节目{summary['精简版非未知节目']}条")
        write_log("="*30 + " 运行结果 " + "="*30, "STEP6_SUMMARY")
        for key, value in summary.items():
            write_log(f"{key}：{value}", "STEP6_SUMMARY")
        write_log("="*60 + " EPG生成完成 " + "="*60 + "\n\n", "END")
        
    except Exception as e:
        error_info = f"运行异常：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "FATAL")
        print(f"❌ EPG运行异常：{str(e)}")
        print(f"详细日志：{config['LOG_PATH']}")
        sys.exit(1)

# ===================== 独立运行入口 =====================
if __name__ == "__main__":
    # 单独运行此脚本时，直接执行EPG生成
    print("="*60)
    print("独立运行EPG生成脚本")
    print("="*60)
    epg_main()
