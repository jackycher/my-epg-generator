// 引入依赖：XML解析+文件读取
import { XMLParser } from 'fast-xml-parser';
import fs from 'fs';

// 核心函数：解析XMLTV格式的epg.xml为DIYP JSON格式
function parseEpgToDiyp(xmlContent) {
  // 初始化XML解析器（适配XMLTV格式）
  const parser = new XMLParser({
    ignoreAttributes: false,    // 保留XML属性（如channel/@id、programme/@start）
    attributeNamePrefix: '@_',  // 属性前缀（如@_id、@_start）
    textNodeName: '#text'       // 文本节点名称
  });

  // 解析XML内容
  const xmlData = parser.parse(xmlContent);
  const tvData = xmlData.tv || {};

  // 第一步：提取所有频道信息
  const channelMap = {};
  // 兼容单频道/多频道（转为数组处理）
  const channelList = Array.isArray(tvData.channel) ? tvData.channel : [tvData.channel].filter(Boolean);
  
  channelList.forEach(channel => {
    const channelId = channel['@_id'];
    if (!channelId) return;

    // 提取频道名称（优先中文）
    let channelName = '未知频道';
    const nameList = Array.isArray(channel['display-name']) ? channel['display-name'] : [channel['display-name']].filter(Boolean);
    nameList.forEach(name => {
      const lang = name['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        channelName = name['#text']?.trim() || channelName;
      }
    });

    // 提取频道LOGO
    const logo = channel.icon?.['@_src']?.trim() || '';

    // 初始化频道节目列表
    channelMap[channelId] = {
      name: channelName,
      tvgid: channelId,
      logo: logo,
      program: []
    };
  });

  // 第二步：提取所有节目信息并关联到频道
  const programList = Array.isArray(tvData.programme) ? tvData.programme : [tvData.programme].filter(Boolean);
  programList.forEach(program => {
    const channelId = program['@_channel'];
    if (!channelMap[channelId]) return; // 跳过无对应频道的节目

    // 处理节目时间（XMLTV格式：20250101080000 +0800 → 保留纯时间）
    const start = program['@_start']?.split(' ')[0] || '';
    const end = program['@_stop']?.split(' ')[0] || '';
    if (!start || !end) return;

    // 提取节目名称（优先中文）
    let programTitle = '未知节目';
    const titleList = Array.isArray(program.title) ? program.title : [program.title].filter(Boolean);
    titleList.forEach(title => {
      const lang = title['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        programTitle = title['#text']?.trim() || programTitle;
      }
    });

    // 添加节目到对应频道
    channelMap[channelId].program.push({
      start: start,
      end: end,
      title: programTitle
    });
  });

  // 转换为DIYP最终格式（数组）
  return Object.values(channelMap);
}

// Cloudflare Pages Functions 入口（处理GET请求）
export async function onRequestGet(context) {
  try {
    // 1. 读取仓库中的epg.xml文件
    if (!fs.existsSync('./epg.xml')) {
      return new Response(
        JSON.stringify({ error: 'epg.xml文件不存在', epg: [] }),
        { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
      );
    }
    const xmlContent = fs.readFileSync('./epg.xml', 'utf8');

    // 2. 解析为DIYP格式
    const diypEpg = parseEpgToDiyp(xmlContent);

    // 3. 解析URL参数ch（支持模糊筛选，忽略大小写）
    const chParam = context.request.url.searchParams.get('ch')?.trim().toLowerCase() || '';
    
    // 4. 筛选频道（无参数返回全量，有参数模糊匹配）
    let filteredEpg = diypEpg;
    if (chParam) {
      filteredEpg = diypEpg.filter(channel => {
        return channel.name.toLowerCase().includes(chParam) || channel.tvgid.toLowerCase().includes(chParam);
      });
    }

    // 5. 返回JSON响应（解决中文乱码）
    return new Response(
      JSON.stringify({ epg: filteredEpg }),
      { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
    );

  } catch (error) {
    // 异常处理
    return new Response(
      JSON.stringify({ error: `解析失败：${error.message}`, epg: [] }),
      { 
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      }
    );
  }
}
