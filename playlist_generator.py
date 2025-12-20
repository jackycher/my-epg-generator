#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立playlist频道生成脚本
可单独运行：python playlist_generator.py
"""
import os
import sys
import json
import datetime
import re
import requests
import traceback

# ===================== playlist配置区 =====================
PLAYLIST_CONFIG = {
    'txt_source': './bjcul.txt',
    'remote_m3u_url': 'https://raw.githubusercontent.com/qwerttvv/Beijing-IPTV/master/IPTV-Unicom-Multicast.m3u',
    'remote_json_url': 'https://raw.githubusercontent.com/zzzz0317/beijing-unicom-iptv-playlist/main/playlist-zz.json',
    'm3u_output': './playlist.m3u',
    'epg_url': 'https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml',
    'rtsp_enabled': True,
    'prelogo': 'https://gh-proxy.org/raw.githubusercontent.com/jackycher/my-epg-generator/main/logo/{name}.png',
    'log_path': "./playlist_run.log"  # playlist专属日志
}

# ===================== 工具函数 =====================
def write_log(content, section="INFO"):
    """playlist专属日志函数"""
    log_path = PLAYLIST_CONFIG['log_path']
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

def read_txt_channels(txt_path):
    """读取TXT频道文件"""
    write_log(f"开始读取TXT频道文件：{txt_path}", "STEP1")
    channels = {}
    current_group = "默认分组"
    
    if not os.path.exists(txt_path):
        raise FileNotFoundError(f"TXT文件不存在：{txt_path}")
    
    valid_line_count = 0
    filtered_line_count = 0
    with open(txt_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line or line.startswith('//'):
                filtered_line_count += 1
                continue
            
            if line.endswith(',#genre#'):
                current_group = line.replace(',#genre#', '').strip() or current_group
                filtered_line_count += 1
                continue
            
            parts = line.split(',', 1)
            if len(parts) != 2:
                raise ValueError(f"TXT第{line_num}行格式错误：{line}")
            
            name = parts[0].strip()
            url = parts[1].strip()
            channels[url] = {
                'name': name,
                'url': url,
                'group': current_group,
                'tvg_name': name
            }
            valid_line_count += 1
    
    txt_channel_names = [ch['name'] for ch in channels.values()]
    write_log(f"TXT读取完成 - 过滤{filtered_line_count}行，有效频道{valid_line_count}个", "STEP1_DETAIL")
    write_log(f"TXT频道列表：{', '.join(txt_channel_names)}", "STEP1_CHANNEL_LIST")
    return channels

def fetch_remote_m3u(remote_m3u_url):
    """下载并解析远程M3U"""
    write_log(f"开始读取远程M3U：{remote_m3u_url}", "STEP2")
    try:
        response = requests.get(remote_m3u_url, timeout=10)
        response.raise_for_status()
        lines = response.text.splitlines()
        write_log(f"远程M3U下载成功，共{len(lines)}行", "STEP2_DETAIL")
    except Exception as e:
        raise ConnectionError(f"读取远程M3U失败：{str(e)}")
    
    remote_channels = {}
    parsed_channel_count = 0
    remote_channel_names = []
    for i, line in enumerate(lines):
        line = line.strip()
        if line.startswith('#EXTINF:'):
            tvg_name_match = re.search(r'tvg-name="([^"]+)"', line)
            name_match = re.search(r',([^,]+)$', line)

            # 分离：逗号后的显示名 和 tvg-name属性值
            display_name = name_match.group(1).strip() if name_match else ""  # 逗号后显示名（最终M3U里的名称）
            tvg_name_attr = tvg_name_match.group(1).strip() if tvg_name_match else display_name  # tvg-name属性值

            if i + 1 < len(lines):
                url = lines[i+1].strip()
                if url and not url.startswith('#'):
                    remote_channels[url] = {
                        'name': display_name,  # 关键：逗号后显示名用这个
                        'url': url,
                        'group': '新增频道',
                        'tvg_name': tvg_name_attr  # tvg-name属性用这个（不影响显示名）
                    }
                    remote_channel_names.append(display_name)
                    parsed_channel_count += 1
    
    write_log(f"远程M3U解析完成 - 提取{parsed_channel_count}个频道", "STEP2_DETAIL")
    write_log(f"远程M3U频道：{', '.join(remote_channel_names)}", "STEP2_CHANNEL_LIST")
    return remote_channels

def supplement_channels_from_remote(txt_channels, remote_channels):
    """补充远程频道"""
    write_log("开始对比补充频道", "STEP3")
    all_channels = txt_channels.copy()
    added_channels = []
    added_count = 0
    for url, ch in remote_channels.items():
        if url not in all_channels:
            all_channels[url] = ch
            added_channels.append(ch['name'])
            added_count += 1
    
    write_log(f"频道补充完成 - 新增{added_count}个，总频道{len(all_channels)}", "STEP3_DETAIL")
    write_log(f"新增频道：{', '.join(added_channels)}", "STEP3_ADDED_CHANNELS")
    return list(all_channels.values())

def fetch_remote_json(remote_json_url):
    """下载远程JSON元数据"""
    write_log(f"开始读取远程JSON：{remote_json_url}", "STEP4")
    try:
        response = requests.get(remote_json_url, timeout=10)
        response.raise_for_status()
        json_data = response.json()
        write_log(f"JSON下载成功，包含{len(json_data)}个频道元数据", "STEP4_DETAIL")
        return json_data
    except Exception as e:
        raise ConnectionError(f"读取远程JSON失败：{str(e)}")

def match_json_metadata(channels, remote_json_url):
    """匹配JSON元数据"""
    json_data = fetch_remote_json(remote_json_url)
    addr_metadata_map = {}
    addr_json_name_map = {}
    
    parsed_meta_count = 0
    for channel_name, channel_info in json_data.items():
        base_meta = {
            'name': channel_info.get('name', channel_name),
            'chno': channel_info.get('chno', ''),
            'tvg_id': channel_info.get('tvg_id', ''),
            'tvg_name': channel_info.get('tvg_name', channel_name),
            'logo': channel_info.get('logo', ''),
            'group_title': channel_info.get('group_title', ''),
            'timeshift_addr': ''
        }
        
        timeshift = channel_info.get('timeshift', {})
        if 'bjunicom-rtsp' in timeshift:
            base_meta['timeshift_addr'] = timeshift['bjunicom-rtsp'].get('addr', '')
        
        live = channel_info.get('live', {})
        for live_type, live_info in live.items():
            addr = live_info.get('addr', '')
            if addr:
                addr_metadata_map[addr] = base_meta
                addr_json_name_map[addr] = channel_name
                parsed_meta_count += 1
    
    write_log(f"JSON元数据解析完成 - 建立{parsed_meta_count}个地址映射", "STEP4_DETAIL")
    
    matched_channels = []
    matched_details = []
    unmatched_details = []
    
    for ch in channels:
        ch_url = ch['url']
        ch_name = ch['name']
        metadata = addr_metadata_map.get(ch_url, {})
        json_channel_name = addr_json_name_map.get(ch_url, "")
        
        matched_channel = {
            'name': ch['name'],
            'group': ch['group'],
            'url': ch_url,
            'chno': metadata.get('chno', ''),
            'tvg_id': metadata.get('tvg_id', ''),
            'tvg_name': metadata.get('tvg_name', ch.get('tvg_name', ch['name'])),
            'logo': metadata.get('logo', ''),
            'timeshift_addr': metadata.get('timeshift_addr', ''),
            'is_remote': ch['group'] == '新增频道'
        }
        matched_channels.append(matched_channel)
        
        if metadata:
            matched_details.append({'channel_name': ch_name, 'json_channel_name': json_channel_name, 'url': ch_url})
        else:
            unmatched_details.append({'channel_name': ch_name, 'url': ch_url})
    
    matched_count = len(matched_details)
    unmatched_count = len(unmatched_details)
    write_log(f"元数据匹配完成 - 成功{matched_count}个，未匹配{unmatched_count}个", "STEP4_DETAIL")
    
    if matched_details:
        matched_names = [f"{d['channel_name']}(匹配{d['json_channel_name']})" for d in matched_details[:20]]
        write_log(f"匹配成功：{', '.join(matched_names)}{'...' if len(matched_details)>20 else ''}", "STEP4_MATCHED_LIST")
    
    if unmatched_details:
        unmatched_names = [f"{d['channel_name']}(URL:{d['url'][:50]}...)" for d in unmatched_details[:20]]
        write_log(f"未匹配：{', '.join(unmatched_names)}{'...' if len(unmatched_details)>20 else ''}", "STEP4_UNMATCHED_LIST")
    
    return matched_channels

def parse_prelogo_placeholder(prelogo, channel_data):
    """
    解析logo占位符，新增逻辑：
    - 若prelogo不含{}占位符，将prelogo与channel_data的logo拼接
    - 若prelogo含{}占位符，执行原有占位符替换逻辑
    """
    if not prelogo:
        return channel_data.get('logo', '')  # prelogo为空时直接返回json中的logo
    
    # 检测是否包含占位符（{xxx}格式）
    placeholder_pattern = r'\{(\w+)\}'
    has_placeholder = re.search(placeholder_pattern, prelogo) is not None
    
    # 无占位符时，拼接prelogo和json中的logo
    if not has_placeholder:
        json_logo = channel_data.get('logo', '')
        # 处理拼接时的路径分隔符（避免重复/）
        if json_logo:
            # 确保prelogo末尾有/，且json_logo开头无/
            prelogo_end = prelogo.rstrip('/') + '/'
            json_logo_start = json_logo.lstrip('/')
            final_logo = prelogo_end + json_logo_start
        else:
            final_logo = prelogo  # json中无logo时直接返回prelogo
        return final_logo
    
    # 有占位符时，执行原有替换逻辑
    placeholder_mapping = {
        '{tvgname}': channel_data.get('tvg_name', ''),
        '{name}': channel_data.get('name', ''),
        '{chno}': channel_data.get('chno', ''),
        '{tvg_id}': channel_data.get('tvg_id', ''),
        '{logo}': channel_data.get('logo', '')
    }
    
    result = prelogo
    for placeholder, value in placeholder_mapping.items():
        result = result.replace(placeholder, str(value))
    
    # 处理未匹配的占位符
    matches = re.findall(placeholder_pattern, result)
    for match in matches:
        result = result.replace(f'{{{match}}}', str(channel_data.get(match, '')))
    
    return result

def htmlspecialchars(s):
    """模拟PHP的htmlspecialchars"""
    if not isinstance(s, str):
        s = str(s)
    return s.replace('"', '&quot;').replace("'", '&#039;').replace('&', '&amp;')

def generate_m3u_content(channels):
    """生成M3U内容"""
    # 第一步：提前生成时间字符串（只调用一次，避免多次now()产生时差）
    generated_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    write_log(f"开始生成M3U，共处理{len(channels)}个频道", "STEP5")
    config = PLAYLIST_CONFIG
    # 提前生成时间字符串（只调用一次，避免多次now()产生时差）
    generated_time = "UTC" + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output = [f'#EXTM3U name="MY_Playlist_generator V4.1" x-tvg-url="{config["epg_url"]}" generated-time="{generated_time}"']
    
    processed_count = 0
    has_timeshift_count = 0
    remote_channel_count = 0
    remote_channel_names = []
    
    for item in channels:
        name = item['name']
        group = item['group']
        ch_url = item['url']
        timeshift_addr = item['timeshift_addr']
        is_remote = item['is_remote']
        tvg_name = item['tvg_name']
        
        if is_remote:
            remote_channel_names.append(name)
            remote_channel_count += 1
        
        # 解析logo（使用修改后的逻辑）
        channel_data = {
            'tvg_name': tvg_name,
            'name': name,
            'chno': item['chno'],
            'tvg_id': item['tvg_id'],
            'logo': item['logo']
        }
        tvg_logo = parse_prelogo_placeholder(config['prelogo'], channel_data)
        
        # 新增频道logo兜底（保持原有逻辑）
        if is_remote and not tvg_logo:
            tvg_logo = f"{config['prelogo']}{name}.png"
        
        # 构建EXTINF行
        extinf_parts = ['#EXTINF:-1']
        if item['chno']:
            extinf_parts.append(f'channel-number="{htmlspecialchars(item["chno"])}"')
        if item['tvg_id']:
            extinf_parts.append(f'tvg-id="{htmlspecialchars(item["tvg_id"])}"')
        extinf_parts.append(f'tvg-name="{htmlspecialchars(tvg_name)}"')
        if tvg_logo:
            extinf_parts.append(f'tvg-logo="{htmlspecialchars(tvg_logo)}"')
        extinf_parts.append(f'group-title="{htmlspecialchars(group)}"')
        
        # 时移功能
        if timeshift_addr:
            catchup_source = timeshift_addr + '?playseek=${(b)yyyyMMddHHmmss}-${(e)yyyyMMddHHmmss}'
            extinf_parts.append('catchup="default"')
            extinf_parts.append(f'catchup-source="{htmlspecialchars(catchup_source)}"')
            has_timeshift_count += 1
        
        extinf_line = ' '.join(extinf_parts) + f',{name}'
        output.append(extinf_line)
        output.append(ch_url)
        
        # 时移警告
        if config['rtsp_enabled'] and not timeshift_addr:
            output.append(f'#【警告】未找到{ch_url}的时移地址')
        
        processed_count += 1
    
    write_log(f"M3U生成完成 - 处理{processed_count}个频道，时移{has_timeshift_count}个，新增远程{remote_channel_count}个", "STEP5_DETAIL")
    write_log(f"新增远程频道：{', '.join(remote_channel_names)}", "STEP5_REMOTE_CHANNEL_LIST")
    return '\n'.join(output)

# ===================== 主函数 =====================
def playlist_main():
    """PLAYLIST生成主逻辑（可被主文件导入调用）"""
    config = PLAYLIST_CONFIG
    # 初始化日志
    if os.path.exists(config['log_path']):
        os.remove(config['log_path'])
    write_log("="*60 + " PLAYLIST频道生成脚本开始运行 " + "="*60, "START")
    start_time = datetime.datetime.now()
    
    try:
        # 步骤1：读取TXT
        txt_channels = read_txt_channels(config['txt_source'])
        # 步骤2：下载远程M3U并补充
        remote_channels = fetch_remote_m3u(config['remote_m3u_url'])
        supplemented_channels = supplement_channels_from_remote(txt_channels, remote_channels)
        # 步骤3：匹配JSON元数据
        matched_channels = match_json_metadata(supplemented_channels, config['remote_json_url'])
        # 步骤4：生成M3U
        m3u_content = generate_m3u_content(matched_channels)
        # 保存文件
        with open(config['m3u_output'], 'w', encoding='utf-8') as f:
            f.write(m3u_content)
        write_log(f"M3U保存成功：{config['m3u_output']}", "STEP6")
        
        # 统计结果
        added_count = len(supplemented_channels) - len(txt_channels)
        run_duration = (datetime.datetime.now() - start_time).total_seconds()
        
        write_log("\n" + "="*30 + " 运行结果 " + "="*30, "FINAL_SUMMARY")
        write_log(f"总耗时：{round(run_duration, 2)}秒", "FINAL_SUMMARY")
        write_log(f"新增频道：{added_count}个", "FINAL_SUMMARY")
        write_log(f"总频道：{len(supplemented_channels)}个", "FINAL_SUMMARY")
        write_log(f"输出文件：{config['m3u_output']}", "FINAL_SUMMARY")
        write_log("="*60 + " PLAYLIST生成完成 " + "="*60 + "\n\n", "END")
        
        # 控制台输出
        print(f"\n✅ PLAYLIST生成完成！")
        print(f"📄 输出文件：{config['m3u_output']}")
        print(f"📝 日志文件：{config['log_path']}")
        print(f"⏱️  耗时：{round(run_duration, 2)}秒")
        print(f"📊 新增频道：{added_count}个，总频道：{len(supplemented_channels)}个")
        
    except Exception as e:
        error_info = f"执行失败：{str(e)}\n{traceback.format_exc()}"
        write_log(error_info, "FATAL")
        print(f"\n❌ PLAYLIST运行异常：{str(e)}")
        print(f"详细日志：{config['log_path']}")
        sys.exit(1)

# ===================== 独立运行入口 =====================
if __name__ == "__main__":
    # 单独运行此脚本时，直接执行PLAYLIST生成
    print("="*60)
    print("独立运行PLAYLIST频道生成脚本")
    print("="*60)
    playlist_main()
