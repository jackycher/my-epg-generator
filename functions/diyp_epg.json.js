import { XMLParser } from 'fast-xml-parser';

// 替换为你的GitHub Raw地址（必须确认可直接访问）
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

// Cloudflare Pages 入口函数（核心修复URL参数解析）
export async function onRequestGet(context) {
  try {
    // 防护：确保context.request存在
    if (!context || !context.request) {
      throw new Error('context.request 未定义');
    }

    // 1. 读取GitHub Raw的epg.xml
    const response = await fetch(EPG_XML_RAW_URL, {
      headers: { 
        'User-Agent': 'Cloudflare Pages Functions',
        'Accept': 'text/xml'
      },
      cache: 'no-cache' // 禁用缓存，获取最新数据
    });

    if (!response.ok) {
      throw new Error(`获取epg.xml失败：HTTP ${response.status}`);
    }

    // 2. 读取XML内容
    const xmlContent = await response.text();
    if (!xmlContent) {
      throw new Error('epg.xml内容为空');
    }

    // 3. 解析为DIYP格式
    const diypEpg = parseEpgToDiyp(xmlContent);

    // ========== 核心修复：正确解析URL参数 ==========
    let chParam = '';
    try {
      // 先把url转为URL对象，再获取searchParams
      const url = new URL(context.request.url);
      // 安全获取ch参数：强制转字符串+trim+小写
      chParam = ((url.searchParams.get('ch') || '') + '').trim().toLowerCase();
    } catch (urlError) {
      console.warn('URL参数解析失败：', urlError.message);
      chParam = ''; // 解析失败则默认空（返回全量）
    }

    // 4. 筛选频道（无参数返回全量，有参数模糊匹配）
    let filteredEpg = diypEpg;
    if (chParam) {
      filteredEpg = diypEpg.filter(channel => {
        const channelName = ((channel.name || '') + '').toLowerCase();
        const tvgid = ((channel.tvgid || '') + '').toLowerCase();
        return channelName.includes(chParam) || tvgid.includes(chParam);
      });
    }

    // 5. 返回响应（解决跨域+格式化JSON）
    return new Response(
      JSON.stringify({ epg: filteredEpg }, null, 2),
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
    console.error('整体解析失败详情：', error);
    return new Response(
      JSON.stringify({ 
        error: `解析失败：${error.message}`, 
        epg: [],
        debug: [
          '1. 检查GitHub Raw地址是否可直接访问',
          '2. 检查XML格式是否规范',
          '3. 错误类型：' + error.name
        ]
      }),
      { 
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      }
    );
  }
}
