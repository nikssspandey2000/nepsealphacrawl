import asyncio
import pandas as pd
import re
import json
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from tqdm import tqdm
import logging
import time
import os
import argparse
import numpy as np
from datetime import datetime

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crawler_debug.log"),
        logging.StreamHandler()
    ]
)

# ─────────────────────────────────────────────
#  HTML DASHBOARD GENERATOR
# ─────────────────────────────────────────────
def generate_dashboard(df_attrs: pd.DataFrame, df_add: pd.DataFrame, output_file="index.html"):
    def safe_float(val):
        try:
            if pd.isna(val):
                return None
            return float(str(val).replace(',', '').replace('%', '').strip())
        except:
            return None

    scored = []
    for _, row in df_attrs.iterrows():
        symbol = str(row.get('Symbol', '')).strip()
        if not symbol:
            continue

        def get(keyword):
            for col in df_attrs.columns:
                if keyword.lower() in col.lower():
                    return safe_float(row[col])
            return None

        eps         = get('EPS')
        pe          = get('PE') or get('P/E')
        pb          = get('PB') or get('P/B') or get('Book')
        roe         = get('ROE')
        roa         = get('ROA')
        dividend    = get('Dividend') or get('DPS')
        market_cap  = get('Market Cap') or get('MarketCap') or get('Mkt Cap')
        high_52     = get('52W High') or get('52 Week High') or get('52')
        low_52      = get('52W Low') or get('52 Week Low')
        ltp         = get('LTP') or get('Last') or get('Close') or get('Price')
        volume      = get('Volume') or get('Traded') or get('Qty')
        net_profit  = get('Net Profit') or get('Profit')
        debt_equity = get('Debt') or get('D/E')
        current_r   = get('Current Ratio') or get('CR')

        score = 0
        signals = []

        if eps and eps > 0:
            score += 15
            signals.append(f"✅ Positive EPS: {eps:.2f}")
        elif eps and eps < 0:
            score -= 10
            signals.append(f"❌ Negative EPS: {eps:.2f}")

        if pe:
            if 5 <= pe <= 20:
                score += 15
                signals.append(f"✅ Attractive PE: {pe:.1f}")
            elif 20 < pe <= 35:
                score += 5
                signals.append(f"⚠️ High PE: {pe:.1f}")
            else:
                score -= 5
                signals.append(f"❌ Very High/Negative PE: {pe:.1f}")

        if pb:
            if pb < 1.5:
                score += 10
                signals.append(f"✅ Low PB: {pb:.2f}")
            elif pb < 3:
                score += 5
                signals.append(f"⚠️ Moderate PB: {pb:.2f}")
            else:
                signals.append(f"❌ High PB: {pb:.2f}")

        if roe:
            if roe > 20:
                score += 15
                signals.append(f"✅ Strong ROE: {roe:.1f}%")
            elif roe > 10:
                score += 8
                signals.append(f"⚠️ Moderate ROE: {roe:.1f}%")
            else:
                signals.append(f"❌ Weak ROE: {roe:.1f}%")

        if roa and roa > 5:
            score += 8
            signals.append(f"✅ Good ROA: {roa:.1f}%")
        elif roa and roa > 0:
            score += 3

        if dividend and dividend > 0:
            score += 10
            signals.append(f"✅ Dividend: {dividend:.2f}")

        if ltp and high_52 and low_52 and high_52 > low_52:
            position = (ltp - low_52) / (high_52 - low_52) * 100
            if position < 30:
                score += 12
                signals.append(f"✅ Near 52W Low ({position:.0f}% of range) — Accumulation zone")
            elif position < 60:
                score += 6
                signals.append(f"⚠️ Mid range ({position:.0f}%)")
            else:
                signals.append(f"📈 Near 52W High ({position:.0f}%)")

        if debt_equity:
            if debt_equity < 1:
                score += 8
                signals.append(f"✅ Low D/E: {debt_equity:.2f}")
            elif debt_equity < 3:
                score += 3
                signals.append(f"⚠️ Moderate D/E: {debt_equity:.2f}")
            else:
                score -= 5
                signals.append(f"❌ High D/E: {debt_equity:.2f}")

        if current_r and current_r > 1.5:
            score += 5
            signals.append(f"✅ Good Liquidity CR: {current_r:.2f}")

        if market_cap and market_cap > 0:
            if market_cap > 10_000_000_000:
                score += 5
                signals.append(f"✅ Large Cap")
            elif market_cap > 1_000_000_000:
                score += 3
                signals.append(f"⚠️ Mid Cap")

        if score >= 70:
            rating, badge = "⭐⭐⭐⭐⭐ Strong Buy", "strong-buy"
        elif score >= 50:
            rating, badge = "⭐⭐⭐⭐ Buy", "buy"
        elif score >= 30:
            rating, badge = "⭐⭐⭐ Hold", "hold"
        elif score >= 10:
            rating, badge = "⭐⭐ Watch", "watch"
        else:
            rating, badge = "⭐ Avoid", "avoid"

        scored.append({
            'Symbol': symbol, 'Score': score, 'Rating': rating, 'Badge': badge,
            'Signals': signals, 'EPS': eps, 'PE': pe, 'PB': pb, 'ROE': roe,
            'ROA': roa, 'Dividend': dividend, 'LTP': ltp,
            '52W_High': high_52, '52W_Low': low_52,
            'MarketCap': market_cap, 'DebtEquity': debt_equity, 'Volume': volume,
        })

    scored.sort(key=lambda x: x['Score'], reverse=True)

    def fmt(val, suffix=''):
        if val is None:
            return '<span class="na">N/A</span>'
        return f"{val:,.2f}{suffix}" if isinstance(val, float) else str(val)

    badge_map = {
        'strong-buy': 'badge-sb', 'buy': 'badge-b',
        'hold': 'badge-h', 'watch': 'badge-w', 'avoid': 'badge-a'
    }

    rows_html = ""
    cards_html = ""
    for i, s in enumerate(scored[:50]):
        rank = i + 1
        sigs = "".join(f"<li>{sg}</li>" for sg in s['Signals'])
        rows_html += f"""
        <tr class="stock-row" data-badge="{s['Badge']}">
            <td class="rank">#{rank}</td>
            <td class="symbol-cell"><strong>{s['Symbol']}</strong></td>
            <td><span class="badge {badge_map.get(s['Badge'], 'badge-w')}">{s['Rating']}</span></td>
            <td class="score-cell"><div class="score-bar"><div class="score-fill" style="width:{min(s['Score'],100)}%"></div><span>{s['Score']}</span></div></td>
            <td>{fmt(s['LTP'])}</td>
            <td>{fmt(s['EPS'])}</td>
            <td>{fmt(s['PE'])}</td>
            <td>{fmt(s['PB'])}</td>
            <td>{fmt(s['ROE'], '%')}</td>
            <td>{fmt(s['Dividend'])}</td>
            <td>{fmt(s['52W_Low'])} – {fmt(s['52W_High'])}</td>
            <td class="signals-col"><ul class="signal-list">{sigs}</ul></td>
        </tr>"""
        if rank <= 10:
            cards_html += f"""
            <div class="card card-{s['Badge']}">
                <div class="card-rank">#{rank}</div>
                <div class="card-symbol">{s['Symbol']}</div>
                <div class="card-rating">{s['Rating']}</div>
                <div class="card-score">Score: {s['Score']}/100</div>
                <div class="card-metrics">
                    <span>LTP: {fmt(s['LTP'])}</span>
                    <span>EPS: {fmt(s['EPS'])}</span>
                    <span>PE: {fmt(s['PE'])}</span>
                    <span>ROE: {fmt(s['ROE'], '%')}</span>
                    <span>Div: {fmt(s['Dividend'])}</span>
                </div>
                <ul class="card-signals">{sigs}</ul>
            </div>"""

    total       = len(scored)
    strong_buys = sum(1 for s in scored if s['Badge'] == 'strong-buy')
    buys        = sum(1 for s in scored if s['Badge'] == 'buy')
    holds       = sum(1 for s in scored if s['Badge'] == 'hold')
    watches     = sum(1 for s in scored if s['Badge'] == 'watch')
    avoids      = sum(1 for s in scored if s['Badge'] == 'avoid')
    updated     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    top10_labels = json.dumps([s['Symbol'] for s in scored[:10]])
    top10_scores = json.dumps([s['Score']  for s in scored[:10]])
    top10_colors = json.dumps([
        '#00c896' if s['Badge'] == 'strong-buy' else
        '#4caf50' if s['Badge'] == 'buy' else
        '#ff9800' if s['Badge'] == 'hold' else
        '#f44336' for s in scored[:10]
    ])

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>NEPSE Stock Analyzer Dashboard</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{{--bg:#0d1117;--surface:#161b22;--surface2:#21262d;--border:#30363d;--text:#e6edf3;--muted:#8b949e;--green:#00c896;--blue:#58a6ff;--orange:#ff9800;--red:#f85149;--yellow:#d29922;--purple:#a371f7}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px}}
.header{{background:linear-gradient(135deg,#161b22,#0d1117);border-bottom:1px solid var(--border);padding:20px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}}
.header h1{{font-size:22px;font-weight:700;color:var(--green)}}
.header .subtitle{{color:var(--muted);font-size:12px;margin-top:4px}}
.updated{{font-size:11px;color:var(--muted);text-align:right}}
.container{{max-width:1600px;margin:0 auto;padding:24px 16px}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:12px;margin-bottom:24px}}
.stat{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px;text-align:center}}
.stat .val{{font-size:28px;font-weight:700;line-height:1}}
.stat .lbl{{font-size:11px;color:var(--muted);margin-top:6px;text-transform:uppercase;letter-spacing:.5px}}
.stat.green .val{{color:var(--green)}}.stat.blue .val{{color:var(--blue)}}.stat.orange .val{{color:var(--orange)}}.stat.red .val{{color:var(--red)}}.stat.purple .val{{color:var(--purple)}}
.section-title{{font-size:16px;font-weight:600;margin-bottom:14px;padding-bottom:8px;border-bottom:1px solid var(--border)}}
.cards{{display:flex;gap:14px;overflow-x:auto;padding-bottom:8px;margin-bottom:28px;scrollbar-width:thin}}
.card{{min-width:230px;background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px;flex-shrink:0;position:relative;overflow:hidden}}
.card::before{{content:'';position:absolute;top:0;left:0;right:0;height:3px}}
.card-strong-buy::before{{background:var(--green)}}.card-buy::before{{background:#4caf50}}.card-hold::before{{background:var(--orange)}}.card-avoid::before{{background:var(--red)}}.card-watch::before{{background:var(--blue)}}
.card-rank{{font-size:11px;color:var(--muted);font-weight:600}}
.card-symbol{{font-size:20px;font-weight:700;margin:4px 0;color:var(--blue)}}
.card-rating{{font-size:12px;margin-bottom:6px}}
.card-score{{font-size:13px;color:var(--green);font-weight:600;margin-bottom:10px}}
.card-metrics{{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}}
.card-metrics span{{background:var(--surface2);border-radius:4px;padding:2px 7px;font-size:11px;color:var(--muted)}}
.card-signals{{list-style:none;font-size:11px;color:var(--muted);line-height:1.8}}
.charts{{display:grid;grid-template-columns:2fr 1fr;gap:16px;margin-bottom:28px}}
.chart-box{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:16px}}
.filters{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px;align-items:center}}
.filters input{{background:var(--surface2);border:1px solid var(--border);color:var(--text);border-radius:6px;padding:7px 12px;font-size:13px;outline:none;width:200px}}
.filters input:focus{{border-color:var(--blue)}}
.filter-btn{{background:var(--surface2);border:1px solid var(--border);color:var(--muted);border-radius:6px;padding:6px 14px;font-size:12px;cursor:pointer;transition:.2s}}
.filter-btn:hover,.filter-btn.active{{background:var(--blue);color:#fff;border-color:var(--blue)}}
.table-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:10px;overflow:auto}}
table{{width:100%;border-collapse:collapse;min-width:900px}}
thead tr{{background:var(--surface2);position:sticky;top:0;z-index:1}}
th{{padding:10px 12px;text-align:left;font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;white-space:nowrap;cursor:pointer;user-select:none}}
th:hover{{color:var(--text)}}
td{{padding:10px 12px;border-top:1px solid var(--border);vertical-align:top}}
tr:hover td{{background:var(--surface2)}}
.rank{{color:var(--muted);font-weight:700}}.symbol-cell{{font-size:15px}}.na{{color:var(--muted)}}
.badge{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:600;white-space:nowrap}}
.badge-sb{{background:rgba(0,200,150,.15);color:var(--green);border:1px solid rgba(0,200,150,.3)}}
.badge-b{{background:rgba(76,175,80,.15);color:#4caf50;border:1px solid rgba(76,175,80,.3)}}
.badge-h{{background:rgba(255,152,0,.15);color:var(--orange);border:1px solid rgba(255,152,0,.3)}}
.badge-w{{background:rgba(88,166,255,.15);color:var(--blue);border:1px solid rgba(88,166,255,.3)}}
.badge-a{{background:rgba(248,81,73,.15);color:var(--red);border:1px solid rgba(248,81,73,.3)}}
.score-cell{{min-width:110px}}
.score-bar{{background:var(--surface2);border-radius:20px;height:20px;position:relative;overflow:hidden}}
.score-fill{{background:linear-gradient(90deg,var(--green),var(--blue));height:100%;border-radius:20px}}
.score-bar span{{position:absolute;right:8px;top:50%;transform:translateY(-50%);font-size:11px;font-weight:700}}
.signal-list{{list-style:none;font-size:11px;line-height:1.8;min-width:220px}}
.disclaimer{{margin-top:32px;padding:16px;background:var(--surface);border:1px solid var(--border);border-radius:8px;font-size:11px;color:var(--muted);line-height:1.7}}
@media(max-width:768px){{.charts{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<div class="header">
  <div>
    <h1>📊 NEPSE Stock Analyzer</h1>
    <div class="subtitle">Fundamental + Technical scoring across all scraped NEPSE stocks</div>
  </div>
  <div class="updated">🕐 Last updated<br/>{updated} NPT</div>
</div>
<div class="container">
  <div class="stats">
    <div class="stat green"><div class="val">{total}</div><div class="lbl">Stocks Analyzed</div></div>
    <div class="stat green"><div class="val">{strong_buys}</div><div class="lbl">Strong Buy</div></div>
    <div class="stat blue"><div class="val">{buys}</div><div class="lbl">Buy</div></div>
    <div class="stat orange"><div class="val">{holds}</div><div class="lbl">Hold</div></div>
    <div class="stat purple"><div class="val">{watches}</div><div class="lbl">Watch</div></div>
    <div class="stat red"><div class="val">{avoids}</div><div class="lbl">Avoid</div></div>
  </div>
  <div class="section-title">🏆 Top 10 Stock Picks</div>
  <div class="cards">{cards_html}</div>
  <div class="charts">
    <div class="chart-box">
      <div class="section-title">Top 10 Stocks by Score</div>
      <canvas id="barChart" height="200"></canvas>
    </div>
    <div class="chart-box">
      <div class="section-title">Rating Distribution</div>
      <canvas id="pieChart" height="200"></canvas>
    </div>
  </div>
  <div class="filters">
    <input id="search" type="text" placeholder="🔍 Search symbol..."/>
    <button class="filter-btn active" onclick="filterTable('all',this)">All ({total})</button>
    <button class="filter-btn" onclick="filterTable('strong-buy',this)">Strong Buy ({strong_buys})</button>
    <button class="filter-btn" onclick="filterTable('buy',this)">Buy ({buys})</button>
    <button class="filter-btn" onclick="filterTable('hold',this)">Hold ({holds})</button>
    <button class="filter-btn" onclick="filterTable('watch',this)">Watch ({watches})</button>
    <button class="filter-btn" onclick="filterTable('avoid',this)">Avoid ({avoids})</button>
  </div>
  <div class="table-wrap">
    <table id="stockTable">
      <thead><tr>
        <th onclick="sortTable(0)">#</th>
        <th onclick="sortTable(1)">Symbol ↕</th>
        <th onclick="sortTable(2)">Rating</th>
        <th onclick="sortTable(3)">Score ↕</th>
        <th onclick="sortTable(4)">LTP ↕</th>
        <th onclick="sortTable(5)">EPS ↕</th>
        <th onclick="sortTable(6)">PE ↕</th>
        <th onclick="sortTable(7)">PB ↕</th>
        <th onclick="sortTable(8)">ROE ↕</th>
        <th onclick="sortTable(9)">Dividend ↕</th>
        <th>52W Range</th>
        <th>Analysis Signals</th>
      </tr></thead>
      <tbody id="tableBody">{rows_html}</tbody>
    </table>
  </div>
  <div class="disclaimer">
    ⚠️ <strong>Disclaimer:</strong> This dashboard is for informational and educational purposes only.
    Stock scores are algorithmically generated based on publicly available data from NepseAlpha.
    This is <strong>NOT financial advice</strong>. Always do your own research (DYOR) before investing.
    Past performance does not guarantee future results. Consult a SEBON-registered investment advisor.
  </div>
</div>
<script>
const barCtx = document.getElementById('barChart').getContext('2d');
new Chart(barCtx,{{type:'bar',data:{{labels:{top10_labels},datasets:[{{label:'Score',data:{top10_scores},backgroundColor:{top10_colors},borderRadius:6,borderSkipped:false}}]}},options:{{responsive:true,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:{{color:'#8b949e'}},grid:{{color:'#21262d'}}}},y:{{ticks:{{color:'#8b949e'}},grid:{{color:'#21262d'}},min:0,max:100}}}}}}}});
const pieCtx = document.getElementById('pieChart').getContext('2d');
new Chart(pieCtx,{{type:'doughnut',data:{{labels:['Strong Buy','Buy','Hold','Watch','Avoid'],datasets:[{{data:[{strong_buys},{buys},{holds},{watches},{avoids}],backgroundColor:['#00c896','#4caf50','#ff9800','#58a6ff','#f85149'],borderWidth:0,hoverOffset:8}}]}},options:{{responsive:true,plugins:{{legend:{{position:'bottom',labels:{{color:'#8b949e',font:{{size:11}}}}}}}}}}}});
function filterTable(badge,btn){{document.querySelectorAll('.filter-btn').forEach(b=>b.classList.remove('active'));btn.classList.add('active');document.querySelectorAll('.stock-row').forEach(row=>{{row.style.display=(badge==='all'||row.dataset.badge===badge)?'':'none'}})}}
document.getElementById('search').addEventListener('input',function(){{const q=this.value.toLowerCase();document.querySelectorAll('.stock-row').forEach(row=>{{row.style.display=row.querySelector('.symbol-cell').textContent.toLowerCase().includes(q)?'':'none'}})}});
let sortDir={{}};
function sortTable(col){{const tbody=document.getElementById('tableBody');const rows=Array.from(tbody.querySelectorAll('tr'));sortDir[col]=!sortDir[col];rows.sort((a,b)=>{{const av=a.cells[col].textContent.replace(/[^0-9.\-]/g,'');const bv=b.cells[col].textContent.replace(/[^0-9.\-]/g,'');const an=parseFloat(av),bn=parseFloat(bv);if(!isNaN(an)&&!isNaN(bn))return sortDir[col]?bn-an:an-bn;return sortDir[col]?b.cells[col].textContent.localeCompare(a.cells[col].textContent):a.cells[col].textContent.localeCompare(b.cells[col].textContent)}});rows.forEach(r=>tbody.appendChild(r))}}
</script>
</body>
</html>"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    logging.info(f"Dashboard saved: {output_file}")
    print(f"✅ Dashboard saved: {output_file}")


# ─────────────────────────────────────────────
#  FETCH HTML USING PLAYWRIGHT (replaces crawl4ai)
# ─────────────────────────────────────────────
async def fetch_html_with_playwright(url: str, browser) -> str:
    """Fetch a page using Playwright so JS-rendered content is captured."""
    page = await browser.new_page()
    try:
        await page.set_extra_http_headers({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://nepsealpha.com/",
        })
        await page.goto(url, wait_until="networkidle", timeout=60000)
        # Wait for tables to appear
        try:
            await page.wait_for_selector("table", timeout=15000)
        except Exception:
            pass
        html = await page.content()
        return html
    except Exception as e:
        logging.error(f"Playwright fetch error for {url}: {e}")
        return ""
    finally:
        await page.close()


# ─────────────────────────────────────────────
#  CRAWLER
# ─────────────────────────────────────────────
class DataCrawler:
    def __init__(self, sheet_name):
        self.sheet_name = sheet_name

    @staticmethod
    def extract_symbol(url):
        return url.split('=')[-1].upper()

    @staticmethod
    def extract_tables(html, url):
        symbol = DataCrawler.extract_symbol(url)
        soup   = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table')
        logging.info(f"Found {len(tables)} tables for {symbol}")
        table_data = []
        for table_idx, table in enumerate(tables, 1):
            headers    = []
            header_row = (table.find('thead') and table.find('thead').find('tr')) or table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            rows = []
            body = table.find('tbody') or table
            for tr in body.find_all('tr'):
                if tr.find('th'):
                    continue
                row = [cell.get_text(strip=True) for cell in tr.find_all('td')]
                if row:
                    rows.append(row)
            if not rows:
                continue
            if headers and len(headers) != len(rows[0]):
                headers = [f"Column_{j}" for j in range(1, len(rows[0]) + 1)]
            df = pd.DataFrame(rows, columns=headers)
            df['Table_Index'] = table_idx
            df['Symbol']      = symbol
            table_data.append(df)
            logging.info(f"Table {table_idx} for {symbol}: {df.shape[0]}r x {df.shape[1]}c")
        return table_data

    @staticmethod
    def save_csv(df, filename):
        try:
            if os.path.exists(filename):
                existing = pd.read_csv(filename)
                if 'Symbol' in existing.columns and 'Symbol' in df.columns:
                    existing = existing[~existing['Symbol'].isin(df['Symbol'])]
                df = pd.concat([existing, df], ignore_index=True)
            df.to_csv(filename, index=False)
            logging.info(f"CSV saved: {filename}")
        except Exception as e:
            logging.error(f"CSV save failed {filename}: {e}")

    async def crawl_all_urls(self):
        start_time = time.time()
        df_links   = pd.read_excel('NepseAlphaLink.xlsx', sheet_name=self.sheet_name)
        urls       = df_links['Link'].tolist()
        logging.info(f"Found {len(urls)} URLs")

        excel_file = "nepsealpha.xlsx"
        sanitized  = re.sub(r'[^a-zA-Z0-9]', '_', self.sheet_name)
        csv_attrs  = f"{sanitized}_Attributes.csv"
        csv_add    = f"{sanitized}_Additional.csv"

        all_pivoted, all_third = [], []
        progress = tqdm(total=len(urls), desc="Crawling")

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                ]
            )

            for url in urls:
                symbol = self.extract_symbol(url)
                try:
                    html = await fetch_html_with_playwright(url, browser)
                    if html:
                        tables = self.extract_tables(html, url)
                        if len(tables) >= 2:
                            combined = pd.concat([tables[0], tables[1]], ignore_index=True)
                            combined.columns = [str(c).strip() for c in combined.columns]
                            combined = combined.drop(
                                columns=[c for c in combined.columns if c.startswith('Compare')],
                                errors='ignore'
                            )
                            data_cols = [c for c in combined.columns if c not in ('Table_Index', 'Symbol')]
                            if len(data_cols) >= 2:
                                pivot_df = combined.pivot_table(
                                    index='Symbol', columns=data_cols[0],
                                    values=data_cols[1], aggfunc='first'
                                ).reset_index()
                                all_pivoted.append(pivot_df)
                        if len(tables) >= 3:
                            t3 = tables[2].copy()
                            t3.columns = [str(c).strip() for c in t3.columns]
                            t3 = t3.drop(
                                columns=[c for c in t3.columns if c.startswith('Compare')],
                                errors='ignore'
                            )
                            all_third.append(t3)
                    progress.update(1)
                    progress.set_postfix({"OK": symbol})
                    await asyncio.sleep(2)
                except Exception as e:
                    logging.error(f"Error {symbol}: {e}")
                    progress.update(1)
                    progress.set_postfix({"ERR": symbol})
                    await asyncio.sleep(2)

            await browser.close()

        progress.close()

        book = {}
        if os.path.exists(excel_file):
            try:
                book = pd.read_excel(excel_file, sheet_name=None, engine='openpyxl')
            except Exception as e:
                logging.warning(f"Could not read existing Excel: {e}")

        df_attrs_out, df_add_out = None, None

        if all_pivoted:
            df_attrs_out = pd.concat(all_pivoted, ignore_index=True)
            df_attrs_out.columns = [str(c).strip() for c in df_attrs_out.columns]
            book[f"{sanitized}_Attributes"] = df_attrs_out
            self.save_csv(df_attrs_out, csv_attrs)
            print(f"✅ Attributes: {df_attrs_out.shape[0]} stocks")
        else:
            logging.warning("No attributes data!")
            if os.path.exists(csv_attrs):
                df_attrs_out = pd.read_csv(csv_attrs)
                print(f"ℹ️  Loaded existing attributes CSV: {len(df_attrs_out)} stocks")

        if all_third:
            df_add_out = pd.concat(all_third, ignore_index=True)
            df_add_out.columns = [str(c).strip() for c in df_add_out.columns]
            if 'Table_Index' in df_add_out.columns:
                df_add_out = df_add_out.drop(columns=['Table_Index'])
            if 'Symbol' in df_add_out.columns:
                df_add_out = df_add_out[['Symbol'] + [c for c in df_add_out.columns if c != 'Symbol']]
            book[f"{sanitized}_Additional"] = df_add_out
            self.save_csv(df_add_out, csv_add)
            print(f"✅ Additional: {df_add_out.shape[0]} rows")
        else:
            if os.path.exists(csv_add):
                df_add_out = pd.read_csv(csv_add)

        # Save Excel
        try:
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                for sname, df in book.items():
                    df.to_excel(writer, sheet_name=sname[:31], index=False)
            print(f"✅ Excel saved: {excel_file}")
        except Exception as e:
            logging.error(f"Excel save failed: {e}")
            print(f"❌ Excel save failed — check log")

        # Generate dashboard
        if df_attrs_out is not None and not df_attrs_out.empty:
            generate_dashboard(df_attrs_out, df_add_out, output_file="index.html")
        else:
            print("⚠️  No data available to generate dashboard")

        mins, secs = divmod(time.time() - start_time, 60)
        print(f"\nTotal time: {int(mins)}m {secs:.0f}s")
        print("\nOutput files:")
        for f in [excel_file, csv_attrs, csv_add, "index.html"]:
            if os.path.exists(f):
                print(f"  ✅ {f}  ({os.path.getsize(f) / 1024:.1f} KB)")
            else:
                print(f"  ❌ {f}  (not created)")
        return True


def run_crawler(sheet_name):
    os.makedirs("html", exist_ok=True)
    print(f"Starting crawl — sheet: {sheet_name}")
    try:
        crawler = DataCrawler(sheet_name)
        asyncio.run(crawler.crawl_all_urls())
        print("Done!")
    except Exception as e:
        print(f"Failed: {e}")
        logging.error(f"Failed: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('sheet_name', nargs='?', default='Link',
                        help='Sheet name in NepseAlphaLink.xlsx (default: Link)')
    args = parser.parse_args()
    run_crawler(args.sheet_name)
