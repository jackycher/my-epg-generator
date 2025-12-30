import { XMLParser } from 'fast-xml-parser';
import fs from 'fs';

// 解析epg.xml为DIYP格式
async function parseEpg() {
  const xml = fs.readFileSync('./epg.xml', 'utf8');
  const parser = new XMLParser({ ignoreAttributes: false });
  const data = parser.parse(xml);
  
  const channels = {};
  // 提取频道
  (data.tv.channel || []).forEach(channel => {
    const id = channel['@_id'];
    let name = '未知频道';
    (channel['display-name'] || []).forEach(n => {
      if (!n['@_lang'] || n['@_lang'] === 'zh') name = n['#text'] || name;
    });
    const logo = channel.icon?.['@_src'] || '';
    channels[id] = { name, tvgid: id, logo, program: [] };
  });
  
  // 提取节目
  (data.tv.programme || []).forEach(prog => {
    const channelId = prog['@_channel'];
    if (!channels[channelId]) return;
    const start = prog['@_start'].split(' ')[0];
    const end = prog['@_stop'].split(' ')[0];
    let title = '未知节目';
    (prog.title || []).forEach(t => {
      if (!t['@_lang'] || t['@_lang'] === 'zh') title = t['#text'] || title;
    });
    channels[channelId].program.push({ start, end, title });
  });
  
  return Object.values(channels);
}

// 处理请求（支持ch参数）
export async function onRequestGet(context) {
  const epg = await parseEpg();
  const ch = context.request.url.searchParams.get('ch')?.toLowerCase() || '';
  const filtered = ch ? epg.filter(c => c.name.toLowerCase().includes(ch) || c.tvgid.toLowerCase().includes(ch)) : epg;
  return new Response(JSON.stringify({ epg: filtered }), {
    headers: { 'Content-Type': 'application/json; charset=utf-8' }
  });
}
