export const config = { maxDuration: 60 };

/**
 * 涨幅公式：作用于 base_target（= Q1/90*7）
 */
function calcTarget(baseTarget) {
  if (!baseTarget || baseTarget === 0) return 500;
  if (baseTarget > 1000000) return Math.round(baseTarget * 1.1);
  if (baseTarget > 700000)  return Math.round(baseTarget * 1.12);
  if (baseTarget > 300000)  return Math.round(baseTarget * 1.15);
  if (baseTarget > 50000)   return Math.round(baseTarget * 1.17);
  if (baseTarget >= 15000)  return Math.round(baseTarget * 1.1);
  return 15000;
}

async function writeSystem(supabaseUrl, serviceKey, system, date, anchors) {
  if (!anchors || anchors.length === 0) return 0;

  // 删除该系统该日期的旧基期数据
  await fetch(`${supabaseUrl}/rest/v1/anchor_data?system=eq.${system}&date=eq.${date}`, {
    method: 'DELETE',
    headers: { apikey: serviceKey, Authorization: `Bearer ${serviceKey}`, Prefer: 'return=minimal' }
  });

  // 构建占位记录
  const records = anchors.map(a => {
    const q1 = Number(a.q1_total) || 0;
    const base_target = Math.round(q1 / 90 * 7);
    const target = calcTarget(base_target);
    return {
      date,
      anchor_id:         String(a.anchor_id),
      anchor_name:       String(a.anchor_name || ''),
      source:            String(a.source || ''),      // 二级来源 → source 字段
      target,
      base_target,
      actual:            0,
      streaming_minutes: 0,
      completion_rate:   0,
      time_progress:     '0%',
      system
    };
  });

  // 分批 INSERT（每批200条）
  const BATCH = 200;
  let inserted = 0;
  for (let i = 0; i < records.length; i += BATCH) {
    const r = await fetch(`${supabaseUrl}/rest/v1/anchor_data`, {
      method: 'POST',
      headers: {
        apikey: serviceKey, Authorization: `Bearer ${serviceKey}`,
        'Content-Type': 'application/json', Prefer: 'return=minimal'
      },
      body: JSON.stringify(records.slice(i, i + BATCH))
    });
    if (r.ok) inserted += Math.min(BATCH, records.length - i);
    else {
      const errText = await r.text();
      throw new Error(`[${system}] batch error: ${r.status} ${errText.slice(0, 120)}`);
    }
  }
  return inserted;
}

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
    const { date, anchors } = req.body;
    if (!date || !anchors || anchors.length === 0) {
      return res.status(400).json({ error: '缺少必要参数或数据为空' });
    }

    // 按系统列分拣（system 字段由前端从表格读取）
    const betAnchors = anchors.filter(a => (a.system || '').toLowerCase() === 'bet');
    const crcAnchors = anchors.filter(a => (a.system || '').toLowerCase() === 'crc');

    if (betAnchors.length === 0 && crcAnchors.length === 0) {
      return res.status(400).json({ error: '没有有效的系统标识，请检查表格中的「系统」列是否填写 bet 或 crc' });
    }

    const betCount = await writeSystem(SUPABASE_URL, SERVICE_KEY, 'bet', date, betAnchors);
    const crcCount = await writeSystem(SUPABASE_URL, SERVICE_KEY, 'crc', date, crcAnchors);

    return res.status(200).json({ success: true, bet: betCount, crc: crcCount });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
