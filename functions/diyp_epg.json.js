// 移除fs导入，改用fetch读取GitHub Raw的epg.xml
import { XMLParser } from 'fast-xml-parser';

// 替换为你的GitHub仓库Raw地址（关键！格式：https://raw.githubusercontent.com/用户名/仓库名/分支名/epg.xml）
const EPG_XML_RAW_URL = 'https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml';

// 核心函数：解析XMLTV格式的epg.xml为DIYP JSON格式
function parseEpgToDiyp(xmlContent) {
  const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: '@_',
    textNodeName: '#text'
  });

  const xmlData = parser.parse(xmlContent);
  const tvData = xmlData.tv || {};

  // 提取频道
  const channelMap = {};
  const channelList = Array.isArray(tvData.channel) ? tvData.channel : [tvData.channel].filter(Boolean);
  
  channelList.forEach(channel => {
    const channelId = channel['@_id'];
    if (!channelId) return;

    let channelName = '未知频道';
    const nameList = Array.isArray(channel['display-name']) ? channel['display-name'] : [channel['display-name']].filter(Boolean);
    nameList.forEach(name => {
      const lang = name['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        channelName = name['#text']?.trim() || channelName;
      }
    });

    const logo = channel.icon?.['@_src']?.trim() || '';
    channelMap[channelId] = {
      name: channelName,
      tvgid: channelId,
      logo: logo,
      program: []
    };
  });

  // 提取节目
  const programList = Array.isArray(tvData.programme) ? tvData.programme : [tvData.programme].filter(Boolean);
  programList.forEach(program => {
    const channelId = program['@_channel'];
    if (!channelMap[channelId]) return;

    const start = program['@_start']?.split(' ')[0] || '';
    const end = program['@_stop']?.split(' ')[0] || '';
    if (!start || !end) return;

    let programTitle = '未知节目';
    const titleList = Array.isArray(program.title) ? program.title : [program.title].filter(Boolean);
    titleList.forEach(title => {
      const lang = title['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        programTitle = title['#text']?.trim() || programTitle;
      }
    });

    channelMap[channelId].program.push({
      start: start,
      end: end,
      title: programTitle
    });
  });

  return Object.values(channelMap);
}

// Cloudflare Pages Functions 入口（Edge Runtime兼容）
export async function onRequestGet(context) {
  try {
    // 1. 从GitHub Raw地址读取epg.xml
    const response = await fetch(EPG_XML_RAW_URL, {
      headers: { 'User-Agent': 'Cloudflare Pages Functions' },
      cache: 'no-cache' // 避免缓存，获取最新的epg.xml
    });

    if (!response.ok) {
      return new Response(
        JSON.stringify({ error: `获取epg.xml失败：${response.status}`, epg: [] }),
        { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
      );
    }

    const xmlContent = await response.text();

    // 2. 解析为DIYP格式
    const diypEpg = parseEpgToDiyp(xmlContent);

    // 3. 解析ch参数（模糊筛选）
    const chParam = context.request.url.searchParams.get('ch')?.trim().toLowerCase() || '';
    let filteredEpg = diypEpg;
    if (chParam) {
      filteredEpg = diypEpg.filter(channel => {
        return channel.name.toLowerCase().includes(chParam) || channel.tvgid.toLowerCase().includes(chParam);
      });
    }

    // 4. 返回响应
    return new Response(
      JSON.stringify({ epg: filteredEpg }),
      { 
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*' // 解决跨域（DIYP播放器访问需要）
        }
      }
    );

  } catch (error) {
    return new Response(
      JSON.stringify({ error: `解析失败：${error.message}`, epg: [] }),
      { 
        status: 500,
        headers: { 'Content-Type': 'application/json; charset=utf-8' }
      }
    );
  }
}
