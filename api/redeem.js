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
    const { source, system, reward_type, reward_name, quantity, points_cost } = req.body;
    if (!source || !system || !reward_type || !reward_name || !quantity || !points_cost) {
      return res.status(400).json({ error: '缺少必要参数' });
    }

    // 检查余额是否充足
    const balR = await fetch(
      `${SUPABASE_URL}/rest/v1/org_balance?source=eq.${encodeURIComponent(source)}&system=eq.${system}&select=balance`,
      { headers: { apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}` } }
    );
    const balRows = await balR.json();
    const balance = Number(balRows?.[0]?.balance || 0);
    if (balance < points_cost) {
      return res.status(400).json({ error: `余额不足，当前余额 ¥${balance.toFixed(2)}，需要 ¥${points_cost}` });
    }

    // 写入兑换申请
    const ir = await fetch(`${SUPABASE_URL}/rest/v1/redeem_requests`, {
      method: 'POST',
      headers: {
        apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`,
        'Content-Type': 'application/json', Prefer: 'return=representation'
      },
      body: JSON.stringify({ source, system, reward_type, reward_name, quantity, points_cost, status: 'pending' })
    });
    if (!ir.ok) {
      const err = await ir.text();
      return res.status(500).json({ error: `写入申请失败: ${err}` });
    }
    const inserted = await ir.json();

    // 扣除余额
    const newBalance = Math.round((balance - points_cost) * 10000) / 10000;
    await fetch(
      `${SUPABASE_URL}/rest/v1/org_balance?source=eq.${encodeURIComponent(source)}&system=eq.${system}`,
      {
        method: 'PATCH',
        headers: {
          apikey: SERVICE_KEY, Authorization: `Bearer ${SERVICE_KEY}`,
          'Content-Type': 'application/json', Prefer: 'return=minimal'
        },
        body: JSON.stringify({ balance: newBalance, last_updated: new Date().toISOString() })
      }
    );

    return res.status(200).json({ success: true, request_id: inserted[0]?.id, new_balance: newBalance });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
}
