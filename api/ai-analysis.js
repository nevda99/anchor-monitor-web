// api/ai-analysis.js — DeepSeek 代理（避免浏览器 CORS 限制）
export default async function handler(req, res) {
  // CORS 预检
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const DS_KEY = process.env.DEEPSEEK_API_KEY;
  if (!DS_KEY) return res.status(500).json({ error: 'Missing DEEPSEEK_API_KEY env var' });

  try {
    const { messages, temperature = 0.6, max_tokens = 1200 } = req.body;
    if (!messages) return res.status(400).json({ error: 'Missing messages' });

    const upstream = await fetch('https://api.deepseek.com/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${DS_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'deepseek-chat',
        messages,
        temperature,
        max_tokens,
        response_format: { type: 'json_object' }
      })
    });

    const data = await upstream.json();
    return res.status(upstream.status).json(data);
  } catch (e) {
    console.error('ai-analysis proxy error:', e);
    return res.status(500).json({ error: e.message });
  }
}
