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
    if (!uadData || !streamData || !date || uadData.length === 0 || streamData.length === 0) {
      return res.status(400).json({ error: '缺少必要参数或数据为空，拒绝执行' });
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

    // 计算本周一日期（作为基期来源）
    const dateObj = new Date(date + 'T00:00:00');
    const dow = dateObj.getDay(); // 0=周日
    const diffToMon = dow === 0 ? -6 : 1 - dow;
    const monObj = new Date(dateObj);
    monObj.setDate(dateObj.getDate() + diffToMon);
    const thisMonday = monObj.toISOString().slice(0, 10);

    const results = { bet: 0, crc: 0, errors: [] };

    for (const system of ['bet', 'crc']) {
      try {
        // 优先取本周一的基期数据，没有则取最近一条
        const baseCheckR = await fetch(
          `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&date=eq.${thisMonday}&select=date&limit=1`,
          { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
        );
        const baseCheck = await baseCheckR.json();
        let bd;
        if (Array.isArray(baseCheck) && baseCheck.length > 0) {
          bd = thisMonday;
        } else {
          // 本周一无数据，退回取最近有数据的日期
          const fallbackR = await fetch(
            `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&select=date&order=date.desc&limit=1`,
            { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
          );
          const fallback = await fallbackR.json();
          bd = fallback[0]?.date;
        }
        if (!bd) { results.errors.push(`${system}: 无基期数据`); continue; }

        // 读该基期日期的所有主播（含 base_target）
        let anchors = [];
        let frm = 0;
        while (true) {
          const r2 = await fetch(
            `${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&date=eq.${bd}&select=anchor_id,anchor_name,source,target,base_target`,
            { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`,
              'Range': `${frm}-${frm+999}` } }
          );
          const rows2 = await r2.json();
          if (!Array.isArray(rows2) || rows2.length === 0) break;
          anchors = anchors.concat(rows2);
          if (rows2.length < 1000) break;
          frm += 1000;
        }

        // DEBUG
        const sampleAnchors = anchors.slice(0,3).map(a=>({id:a.anchor_id,target:a.target,base_target:a.base_target}));
        console.log(`[DEBUG ${system}] bd=${bd} anchors=${anchors.length} sample=`, JSON.stringify(sampleAnchors));

        // 构建新记录（透传 target 和 base_target）
        const records = anchors.map(a => {
          const aid = String(a.anchor_id);
          const actual = uadMap[aid] || 0;
          const streaming = stMap[aid] || 0;
          const cr = a.target > 0 ? Math.round(actual / a.target * 10000) / 100 : 0;
          return {
            date, anchor_id: aid, anchor_name: a.anchor_name,
            source: a.source, target: a.target,
            base_target: a.base_target || 0,
            actual,
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
