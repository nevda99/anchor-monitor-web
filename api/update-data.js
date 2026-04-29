export const config = { maxDuration: 60 };

export default async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const SERVICE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const SUPABASE_URL = process.env.SUPABASE_URL || 'https://yoywhdfqhymduaxhalhq.supabase.co';

  if (!SERVICE_KEY) return res.status(500).json({ error: 'Server config error' });

  try {
    const { uadData, streamData, date, timeProgress } = req.body;
    if (!uadData || !streamData || !date) {
      return res.status(400).json({ error: '缺少必要参数' });
    }

    // 构建查找映射
    const uadMap = {};
    const stMap = {};
    for (const row of uadData) {
      if (row.anchor_id) uadMap[String(row.anchor_id)] = Number(row.value) || 0;
    }
    for (const row of streamData) {
      if (row.anchor_id) stMap[String(row.anchor_id)] = Number(row.value) || 0;
    }

    // 从 Supabase 获取两个系统的基期数据（所有主播）
    const results = { bet: 0, crc: 0, errors: [] };

    for (const system of ['bet', 'crc']) {
      try {
        // 分页读取该系统基期（用最早有记录的日期作为基期）
        let baseRecords = [];
        let from = 0;
        while (true) {
          const r = await fetch(
            `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&select=anchor_id,anchor_name,source,target&order=date.asc&limit=1000&offset=${from}`,
            { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
          );
          const rows = await r.json();
          if (!Array.isArray(rows) || rows.length === 0) break;
          baseRecords = baseRecords.concat(rows);
          if (rows.length < 1000) break;
          from += 1000;
        }

        // 去重（同一个anchor_id保留第一条，用于获取最新基期目标）
        // 改为：获取该系统基期数据（最新基期日期的数据）
        const baseR = await fetch(
          `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&select=date&order=date.asc&limit=1`,
          { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
        );
        const baseDate = await baseR.json();
        const bd = baseDate[0]?.date;
        if (!bd) { results.errors.push(`${system}: 无基期数据`); continue; }

        // 读该基期日期的所有主播
        let anchors = [];
        let frm = 0;
        while (true) {
          const r2 = await fetch(
            `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&date=eq.${bd}&select=anchor_id,anchor_name,source,target`,
            { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`,
              'Range': `${frm}-${frm+999}` } }
          );
          const rows2 = await r2.json();
          if (!Array.isArray(rows2) || rows2.length === 0) break;
          anchors = anchors.concat(rows2);
          if (rows2.length < 1000) break;
          frm += 1000;
        }

        // 构建新记录
        const records = anchors.map(a => {
          const aid = String(a.anchor_id);
          const actual = uadMap[aid] || 0;
          const streaming = stMap[aid] || 0;
          const cr = a.target > 0 ? Math.round(actual / a.target * 10000) / 100 : 0;
          return {
            date, anchor_id: aid, anchor_name: a.anchor_name,
            source: a.source, target: a.target, actual,
            streaming_minutes: streaming, completion_rate: cr,
            time_progress: timeProgress, system
          };
        });

        // 删除当天旧数据
        await fetch(`${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&date=eq.${date}`, {
          method: 'DELETE',
          headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`, Prefer: 'return=minimal' }
        });

        // 分批 INSERT
        const BATCH = 200;
        for (let i = 0; i < records.length; i += BATCH) {
          const batch = records.slice(i, i + BATCH);
          const ir = await fetch(`${SUPABASE_URL}/rest/v1/anchor_data`, {
            method: 'POST',
            headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`,
              'Content-Type': 'application/json', Prefer: 'return=minimal' },
            body: JSON.stringify(batch)
          });
          if (!ir.ok) results.errors.push(`${system} batch${i}: ${ir.status}`);
        }
        results[system] = records.length;
      } catch (e) {
        results.errors.push(`${system}: ${e.message}`);
      }
    }

    return res.status(200).json({ success: true, ...results });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
