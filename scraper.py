import asyncio
import json
import re
import os
from datetime import datetime, date
from typing import Optional
from playwright.async_api import async_playwright
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

GENRE_KEYWORDS = {
    'rock': ['rock', 'metal', 'punk', 'duman', 'pinhani', 'pentagram', 'yüksek sadakat', 'mor ve ötesi', 'kargo', 'athena'],
    'elec': ['elektronik', 'electronic', 'dj', 'techno', 'house', 'edm', 'cortini', 'synthesizer', 'rave', 'avangart tabldot'],
    'alt': ['alternatif', 'indie', 'folk', 'caz', 'jazz', 'blues', 'emir can', 'can bonomo', 'cem adrian', 'paribu', 'nardis'],
    'pop': ['pop', 'gökhan', 'yalın', 'sıla', 'tarkan', 'irem derici', 'melike şahin', 'hadise', 'murat boz'],
    'rap': ['rap', 'hip hop', 'hip-hop', 'şanışer', 'sokrat', 'bege', 'lvbel', 'sagopa', 'ceza', 'ezhel'],
}

DISTRICT_MAP = {
    'Beşiktaş': ['beşiktaş', 'zorlu', 'if performance', 'akaretler', 'vodafone'],
    'Beyoğlu': ['beyoğlu', 'babylon', 'klein', 'blind', 'jj arena', 'taksim', 'harbiye'],
    'Kadıköy': ['kadıköy', 'paribu', 'terminal', 'dorock', 'bant mag', 'moda'],
    'Şişli': ['şişli', 'volkswagen arena', 'levent', 'maslak'],
    'Kartal': ['kartal', 'istmarina'],
    'Küçükçekmece': ['küçükçekmece', 'atakent'],
    'Ataşehir': ['ataşehir'],
    'Bostancı': ['bostancı'],
    'Sarıyer': ['sarıyer', 'çırağan'],
}

COORDS_MAP = {
    'zorlu': (41.0602, 29.0103),
    'if performance': (41.0422, 28.9927),
    'atakent': (41.0082, 28.7786),
    'vadistanbul': (41.0784, 28.9879),
    'volkswagen': (41.0784, 28.9879),
    'paribu': (40.9906, 29.0229),
    'babylon': (41.0335, 28.9795),
    'klein': (41.0335, 28.9795),
    'blind': (41.0335, 28.9795),
    'nardis': (41.0335, 28.9795),
    'dorock': (40.9906, 29.0229),
    'bostancı': (40.9637, 29.0843),
    'kartal': (40.9081, 29.1878),
    'harbiye': (41.0483, 28.9905),
}

def detect_genre(text):
    t = text.lower()
    for genre, kws in GENRE_KEYWORDS.items():
        if any(k in t for k in kws):
            return genre
    return 'alt'

def detect_district(venue):
    v = venue.lower()
    for district, kws in DISTRICT_MAP.items():
        if any(k in v for k in kws):
            return district
    return 'İstanbul'

def get_coords(venue):
    v = venue.lower()
    for key, coords in COORDS_MAP.items():
        if key in v:
            return coords
    return (41.0082, 28.9784)

def clean_price(s):
    if not s:
        return 'Fiyat TBA'
    nums = re.findall(r'\d+', s.replace('.', '').replace(',', ''))
    if nums:
        return f"{nums[0]}₺"
    return 'Fiyat TBA'

def parse_date(text):
    if not text:
        return None
    text = text.strip().lower()
    MONTHS = {
        'ocak':1,'şubat':2,'mart':3,'nisan':4,'mayıs':5,'haziran':6,
        'temmuz':7,'ağustos':8,'eylül':9,'ekim':10,'kasım':11,'aralık':12,
        'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,
        'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12,
    }
    # YYYY-MM-DD
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    # DD.MM.YYYY
    m = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', text)
    if m:
        try: return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except: pass
    # DD Month YYYY
    m = re.search(r'(\d{1,2})\s+([a-zşçığüö]+)\s*(\d{4})?', text)
    if m:
        day, month_str = int(m.group(1)), m.group(2)
        year = int(m.group(3)) if m.group(3) else datetime.now().year
        for mn, mv in MONTHS.items():
            if mn in month_str:
                try: return date(year, mv, day)
                except: pass
    return None

async def scrape_biletix(page):
    print("🎫 Biletix scraping...")
    concerts = []
    try:
        # Use direct Istanbul music URL
        await page.goto('https://www.biletix.com/category/MUSIC/ISTANBUL/tr', timeout=45000)
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(5)
        
        # Get all links that look like event pages
        links = await page.eval_on_selector_all('a[href*="/etkinlik/"]', 
            'els => els.map(e => ({href: e.href, text: e.innerText.trim()}))')
        
        print(f"  Found {len(links)} event links")
        
        for link in links[:50]:
            try:
                href = link.get('href', '')
                text = link.get('text', '')
                if not text or len(text) < 3:
                    continue
                
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                title = lines[0] if lines else text[:100]
                
                # Try to find date in text
                date_str = ''
                for line in lines:
                    if re.search(r'\d{1,2}[./]\d{1,2}|\d{1,2}\s+[a-zşçığüö]+', line.lower()):
                        date_str = line
                        break
                
                venue = next((l for l in lines[1:] if any(v in l.lower() for v in 
                    ['hall', 'arena', 'psm', 'joker', 'sahne', 'merkezi', 'park', 'babylon', 'blind', 'nardis'])), '')
                
                price_str = next((l for l in lines if '₺' in l or ('tl' in l.lower() and any(c.isdigit() for c in l))), '')
                
                concert_date = parse_date(date_str)
                if not concert_date or concert_date < date.today():
                    continue
                
                concerts.append({
                    'title': title[:200],
                    'artist': title.split(' - ')[0][:200],
                    'venue': venue[:200] or 'İstanbul',
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': clean_price(price_str),
                    'genre': detect_genre(title + ' ' + venue),
                    'ticket_url': href[:500],
                    'district': detect_district(venue),
                    'going_count': 0,
                    'lat': get_coords(venue)[0],
                    'lng': get_coords(venue)[1],
                    'source': 'biletix',
                })
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"  Biletix error: {e}")
    
    # Deduplicate
    seen = set()
    unique = []
    for c in concerts:
        key = (c['title'], c['date'])
        if key not in seen:
            seen.add(key)
            unique.append(c)
    
    print(f"  ✅ Biletix: {len(unique)} concerts")
    return unique

async def scrape_bubilet(page):
    print("🎫 Bubilet scraping...")
    concerts = []
    try:
        await page.goto('https://www.bubilet.com.tr/istanbul/kategori/konser', timeout=45000)
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(4)
        
        # Get event links
        links = await page.eval_on_selector_all('a[href*="/etkinlik/"], a[href*="/konser/"]',
            'els => els.map(e => ({href: e.href, text: e.innerText.trim()}))')
        
        print(f"  Found {len(links)} event links on Bubilet")
        
        for link in links[:50]:
            try:
                href = link.get('href', '')
                text = link.get('text', '')
                if not text or len(text) < 3:
                    continue
                
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                title = lines[0] if lines else ''
                if not title:
                    continue
                    
                date_str = next((l for l in lines if re.search(r'\d{1,2}[./]\d{1,2}|\d{1,2}\s+[a-zşçığüö]+', l.lower())), '')
                venue = next((l for l in lines[1:] if any(v in l.lower() for v in 
                    ['hall', 'arena', 'psm', 'joker', 'sahne', 'merkezi', 'istanbul'])), '')
                price_str = next((l for l in lines if '₺' in l), '')
                
                concert_date = parse_date(date_str)
                if not concert_date or concert_date < date.today():
                    continue
                
                concerts.append({
                    'title': title[:200],
                    'artist': title[:200],
                    'venue': venue[:200] or 'İstanbul',
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': clean_price(price_str),
                    'genre': detect_genre(title + ' ' + venue),
                    'ticket_url': href[:500] or 'https://www.bubilet.com.tr',
                    'district': detect_district(venue),
                    'going_count': 0,
                    'lat': get_coords(venue)[0],
                    'lng': get_coords(venue)[1],
                    'source': 'bubilet',
                })
            except:
                continue
    except Exception as e:
        print(f"  Bubilet error: {e}")
    
    seen = set()
    unique = []
    for c in concerts:
        key = (c['title'], c['date'])
        if key not in seen:
            seen.add(key)
            unique.append(c)
    
    print(f"  ✅ Bubilet: {len(unique)} concerts")
    return unique

async def scrape_paribu(page):
    """Paribu Art directly — reliable source"""
    print("🎫 Paribu Art scraping...")
    concerts = []
    try:
        await page.goto('https://art.paribu.com/calendar', timeout=45000)
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(4)
        
        # Get all text content
        content = await page.inner_text('body')
        lines = [l.strip() for l in content.split('\n') if l.strip()]
        
        # Find concert entries — look for KONSER pattern
        i = 0
        while i < len(lines):
            line = lines[i]
            if 'KONSER' in line.upper() or 'KONSER' in lines[i-1].upper() if i > 0 else False:
                # Try to build a concert entry
                date_str = ''
                title = ''
                
                # Look nearby lines for date and title
                for j in range(max(0, i-3), min(len(lines), i+5)):
                    if re.search(r'\d{1,2}\s+[A-Za-zşçığüöŞÇİĞÜÖ]+\s+202\d', lines[j]):
                        date_str = lines[j]
                    elif len(lines[j]) > 5 and 'KONSER' not in lines[j].upper() and not re.search(r'^\d', lines[j]):
                        if not title:
                            title = lines[j]
                
                if title and date_str:
                    concert_date = parse_date(date_str)
                    if concert_date and concert_date >= date.today():
                        concerts.append({
                            'title': title[:200],
                            'artist': title[:200],
                            'venue': 'Paribu Art',
                            'date': concert_date.isoformat(),
                            'time': '21:00',
                            'price': 'Fiyat TBA',
                            'genre': detect_genre(title),
                            'ticket_url': 'https://art.paribu.com',
                            'district': 'Kadıköy',
                            'going_count': 0,
                            'lat': 40.9906,
                            'lng': 29.0229,
                            'source': 'paribu',
                        })
            i += 1
            
    except Exception as e:
        print(f"  Paribu error: {e}")
    
    print(f"  ✅ Paribu: {len(concerts)} concerts")
    return concerts

def upsert_concerts(sb, concerts):
    inserted = 0
    updated = 0
    for concert in concerts:
        try:
            existing = sb.table('concerts').select('id').eq('title', concert['title']).eq('date', concert['date']).execute()
            if existing.data:
                sb.table('concerts').update({
                    'venue': concert['venue'],
                    'price': concert['price'],
                    'ticket_url': concert['ticket_url'],
                }).eq('id', existing.data[0]['id']).execute()
                updated += 1
            else:
                sb.table('concerts').insert(concert).execute()
                inserted += 1
        except Exception as e:
            print(f"  DB error: {concert.get('title','?')}: {e}")
    return inserted, updated

async def main():
    print("🚀 Setlist Scraper başlıyor...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)
    
    if not SUPABASE_KEY:
        print("❌ SUPABASE_SERVICE_KEY bulunamadı!")
        return
    
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 720},
            locale='tr-TR',
        )
        page = await context.new_page()
        
        biletix = await scrape_biletix(page)
        bubilet = await scrape_bubilet(page)
        paribu = await scrape_paribu(page)
        
        all_concerts = biletix + bubilet + paribu
        await browser.close()
    
    print("=" * 50)
    print(f"📊 Toplam {len(all_concerts)} konser bulundu")
    
    if all_concerts:
        inserted, updated = upsert_concerts(sb, all_concerts)
        print(f"✅ {inserted} yeni eklendi, {updated} güncellendi")
    else:
        print("⚠️  Konser bulunamadı — site yapıları değişmiş olabilir")
    
    print("✨ Tamamlandı!")

if __name__ == "__main__":
    asyncio.run(main())
