export const config = { maxDuration: 60 };

/**
 * 涨幅公式：作用于 base_target（= Q1/90*7）
 * - base_target = 0 → 任务目标 = 500
 * - > 1,000,000 → × 1.1
 * - > 700,000   → × 1.12
 * - > 300,000   → × 1.15
 * - > 50,000    → × 1.17
 * - ≥ 15,000    → × 1.1
 * - < 15,000    → 保底 15,000
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
    const { system, date, anchors } = req.body;
    if (!system || !date || !anchors || anchors.length === 0) {
      return res.status(400).json({ error: '缺少必要参数或数据为空' });
    }
    if (!['bet', 'crc'].includes(system)) {
      return res.status(400).json({ error: '无效的系统标识' });
    }

    // 删除该系统该日期的旧基期数据
    await fetch(`${SUPABASE_URL}/rest/v1/anchor_data?system=eq.${system}&date=eq.${date}`, {
      method: 'DELETE',
      headers: {
        apikey: SERVICE_KEY,
        Authorization: `Bearer ${SERVICE_KEY}`,
        Prefer: 'return=minimal'
      }
    });

    // 构建占位记录：从 Q1总数 自动计算两个口径
    const records = anchors.map(a => {
      const q1 = Number(a.q1_total) || 0;
      // base_target = Q1/90*7（取整）
      const base_target = Math.round(q1 / 90 * 7);
      // target = 套涨幅公式
      const target = calcTarget(base_target);
      return {
        date,
        anchor_id:         String(a.anchor_id),
        anchor_name:       String(a.anchor_name || ''),
        source:            String(a.source || ''),
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
    const errors = [];
    for (let i = 0; i < records.length; i += BATCH) {
      const batch = records.slice(i, i + BATCH);
      const r = await fetch(`${SUPABASE_URL}/rest/v1/anchor_data`, {
        method: 'POST',
        headers: {
          apikey: SERVICE_KEY,
          Authorization: `Bearer ${SERVICE_KEY}`,
          'Content-Type': 'application/json',
          Prefer: 'return=minimal'
        },
        body: JSON.stringify(batch)
      });
      if (r.ok) {
        inserted += batch.length;
      } else {
        const errText = await r.text();
        errors.push(`batch${Math.floor(i / BATCH) + 1}: ${r.status} ${errText.slice(0, 100)}`);
      }
    }

    if (errors.length > 0) {
      return res.status(500).json({ error: errors.join('; '), count: inserted });
    }
    return res.status(200).json({ success: true, count: inserted });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
