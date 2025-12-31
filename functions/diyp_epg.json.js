import { XMLParser } from 'fast-xml-parser';

// 替换为你的GitHub Raw地址（必须确认正确）
const EPG_XML_RAW_URL = 'https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml';

// 核心解析函数（适配你的规范XML格式）
function parseEpgToDiyp(xmlContent) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text',
    // 强制将单节点转为数组（避免单个节点时不是数组）
    isArray: (tagName) => tagName === 'channel' || tagName === 'programme' || tagName === 'display-name' || tagName === 'title'
  });

  const xmlData = parser.parse(xmlContent);
  const tvData = xmlData.tv || {};

  // 1. 提取频道（防护：确保channels是数组）
  const channels = Array.isArray(tvData.channel) ? tvData.channel : [];
  const channelMap = {};

  channels.forEach(channel => {
    const channelId = (channel['@_id'] || '') + ''; // 强制转字符串
    if (!channelId) return;

    // 提取频道名称（你的XML中display-name是数组，且带lang="zh"）
    let channelName = '未知频道';
    const displayNames = Array.isArray(channel['display-name']) ? channel['display-name'] : [];
    
    displayNames.forEach(dn => {
      const lang = (dn['@_lang'] || '') + '';
      // 强制转字符串 + trim，彻底避免trim报错
      const nameText = ((dn['#text'] || '') + '').trim();
      
      if (lang === 'zh' && nameText) {
        channelName = nameText;
      }
    });

    // 提取LOGO（你的XML中无icon，默认空）
    const logo = ((channel.icon?.['@_src'] || '') + '').trim();
    channelMap[channelId] = {
      name: channelName,
      tvgid: channelId,
      logo: logo,
      program: []
    };
  });

  // 2. 提取节目（防护：确保programmes是数组）
  const programmes = Array.isArray(tvData.programme) ? tvData.programme : [];
  
  programmes.forEach(program => {
    const channelId = (program['@_channel'] || '') + ''; // 强制转字符串（匹配channel的id）
    if (!channelMap[channelId]) return;

    // 处理时间（强制转字符串后分割）
    const start = ((program['@_start'] || '') + '').split(' ')[0] || '';
    const end = ((program['@_stop'] || '') + '').split(' ')[0] || '';
    if (!start || !end) return;

    // 提取节目名称（你的XML中title是数组，且带lang="zh"）
    let programTitle = '未知节目';
    const titles = Array.isArray(program.title) ? program.title : [];
    
    titles.forEach(t => {
      const lang = (t['@_lang'] || '') + '';
      // 核心修复：强制转字符串后trim
      const titleText = ((t['#text'] || '') + '').trim();
      
      if (lang === 'zh' && titleText) {
        programTitle = titleText;
      }
    });

    // 添加节目到对应频道
    channelMap[channelId].program.push({
      start: start,
      end: end,
      title: programTitle
    });
  });

  // 转为数组返回
  return Object.values(channelMap);
}

// Cloudflare Pages 入口函数
export async function onRequestGet(context) {
  try {
    // 1. 读取GitHub Raw的epg.xml
    const response = await fetch(EPG_XML_RAW_URL, {
      headers: { 
        'User-Agent': 'Cloudflare Pages Functions',
        'Accept': 'text/xml'
      },
      cache: 'no-cache' // 禁用缓存，获取最新数据
    });

    if (!response.ok) {
      return new Response(
        JSON.stringify({ error: `获取epg.xml失败：HTTP ${response.status}`, epg: [] }),
        { 
          status: 500,
          headers: { 'Content-Type': 'application/json; charset=utf-8' }
        }
      );
    }

    // 2. 读取XML内容
    const xmlContent = await response.text();
    if (!xmlContent) {
      return new Response(
        JSON.stringify({ error: 'epg.xml内容为空', epg: [] }),
        { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
      );
    }

    // 3. 解析为DIYP格式
    const diypEpg = parseEpgToDiyp(xmlContent);

    // 4. 处理ch参数（模糊筛选，忽略大小写）
    const chParam = ((context.request.url.searchParams.get('ch') || '') + '').trim().toLowerCase();
    let filteredEpg = diypEpg;
    
    if (chParam) {
      filteredEpg = diypEpg.filter(channel => {
        const channelName = (channel.name || '') + '';
        const tvgid = (channel.tvgid || '') + '';
        return channelName.toLowerCase().includes(chParam) || tvgid.toLowerCase().includes(chParam);
      });
    }

    // 5. 返回响应（解决跨域）
    return new Response(
      JSON.stringify({ epg: filteredEpg }, null, 2), // 格式化JSON，便于调试
      { 
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*', // 跨域允许
          'Cache-Control': 'max-age=300' // 5分钟缓存（平衡性能和实时性）
        }
      }
    );

  } catch (error) {
    // 详细错误日志，便于调试
    console.error('解析失败详情：', error);
    return new Response(
      JSON.stringify({ 
        error: `解析失败：${error.message}`, 
        epg: [],
        debug: '可检查GitHub Raw地址是否正确，或XML格式是否规范'
      }),
      { 
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      }
    );
  }
}
