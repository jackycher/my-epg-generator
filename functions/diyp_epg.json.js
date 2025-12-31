import { XMLParser } from 'fast-xml-parser';

// 替换为你的GitHub Raw地址
const EPG_XML_RAW_URL = 'https://raw.githubusercontent.com/jackycher/my-epg-generator/main/epg.xml';

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
      // 关键修正：兼容纯字符串/对象两种格式
      let nameText = '';
      if (typeof name === 'string') {
        nameText = name; // 纯字符串直接使用
      } else {
        nameText = name['#text'] || ''; // 对象取#text
      }
      // 过滤空值+去空格
      if (!nameText) return;
      const lang = name['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        channelName = nameText.trim() || channelName; // 现在trim()不会报错
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
      // 关键修正：兼容纯字符串/对象两种格式
      let titleText = '';
      if (typeof title === 'string') {
        titleText = title; // 纯字符串直接使用
      } else {
        titleText = title['#text'] || ''; // 对象取#text
      }
      // 过滤空值+去空格
      if (!titleText) return;
      const lang = title['@_lang'] || '';
      if (lang === 'zh' || lang === 'zh-CN' || lang === '') {
        programTitle = titleText.trim() || programTitle; // 现在trim()不会报错
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

export async function onRequestGet(context) {
  try {
    const response = await fetch(EPG_XML_RAW_URL, {
      headers: { 'User-Agent': 'Cloudflare Pages Functions' },
      cache: 'no-cache'
    });

    if (!response.ok) {
      return new Response(
        JSON.stringify({ error: `获取epg.xml失败：${response.status}`, epg: [] }),
        { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
      );
    }

    const xmlContent = await response.text();
    const diypEpg = parseEpgToDiyp(xmlContent);

    const chParam = context.request.url.searchParams.get('ch')?.trim().toLowerCase() || '';
    let filteredEpg = diypEpg;
    if (chParam) {
      filteredEpg = diypEpg.filter(channel => {
        return channel.name.toLowerCase().includes(chParam) || channel.tvgid.toLowerCase().includes(chParam);
      });
    }

    return new Response(
      JSON.stringify({ epg: filteredEpg }),
      { 
        headers: { 
          'Content-Type': 'application/json; charset=utf-8',
          'Access-Control-Allow-Origin': '*'
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
