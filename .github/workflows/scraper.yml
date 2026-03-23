import asyncio
import json
import re
import os
from datetime import datetime, date
from typing import Optional
from playwright.async_api import async_playwright
from supabase import create_client, Client

# Supabase config — GitHub Secrets'tan gelecek
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://yjsuogvhpgxyooernpwp.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

GENRE_KEYWORDS = {
    'rock': ['rock', 'metal', 'punk', 'grunge', 'heavy', 'duman', 'pinhani', 'pentagram', 'yüksek sadakat', 'mor ve ötesi'],
    'elec': ['elektronik', 'electronic', 'dj', 'techno', 'house', 'edm', 'cortini', 'synthesizer', 'rave', 'avangart'],
    'alt': ['alternatif', 'indie', 'folk', 'akustik', 'caz', 'jazz', 'blues', 'emir can', 'can bonomo', 'cem adrian', 'paribu'],
    'pop': ['pop', 'türk pop', 'gökhan', 'yalın', 'sıla', 'tarkan', 'irem derici', 'melike şahin'],
    'rap': ['rap', 'hip hop', 'hip-hop', 'hiphop', 'şanışer', 'sokrat', 'bege', 'lvbel', 'sagopa', 'ceza'],
}

DISTRICT_KEYWORDS = {
    'Beşiktaş': ['beşiktaş', 'zorlu', 'vodafone park', 'if performance', 'akaretler'],
    'Beyoğlu': ['beyoğlu', 'babylon', 'klein', 'blind istanbul', 'jj arena', 'taksim', 'cihangir'],
    'Kadıköy': ['kadıköy', 'paribu art', 'terminal kadıköy', 'dorock', 'bant mag', 'moda'],
    'Şişli': ['şişli', 'volkswagen arena', 'cemil topuzlu', 'levent'],
    'Kartal': ['kartal', 'istmarina'],
    'Küçükçekmece': ['küçükçekmece', 'atakent tema'],
    'Ataşehir': ['ataşehir', 'jolly joker ataşehir'],
    'Bostancı': ['bostancı', 'bostancı gösteri'],
}

VENUE_COORDS = {
    'zorlu psm': (41.0602, 29.0103),
    'if performance hall': (41.0422, 28.9927),
    'jolly joker atakent': (41.0082, 28.7786),
    'jolly joker vadistanbul': (41.0784, 28.9879),
    'volkswagen arena': (41.0784, 28.9879),
    'paribu art': (40.9906, 29.0229),
    'babylon': (41.0335, 28.9795),
    'klein phönix': (41.0335, 28.9795),
    'blind istanbul': (41.0335, 28.9795),
    'nardis jazz club': (41.0335, 28.9795),
    'dorock xl': (40.9906, 29.0229),
    'bostancı gösteri merkezi': (40.9637, 29.0843),
    'jolly joker kartal': (40.9081, 29.1878),
    'jj arena': (41.0335, 28.9795),
}

def detect_genre(text: str) -> str:
    text_lower = text.lower()
    for genre, keywords in GENRE_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return genre
    return 'alt'

def detect_district(venue: str) -> str:
    venue_lower = venue.lower()
    for district, keywords in DISTRICT_KEYWORDS.items():
        if any(kw in venue_lower for kw in keywords):
            return district
    return 'İstanbul'

def get_coords(venue: str):
    venue_lower = venue.lower()
    for key, coords in VENUE_COORDS.items():
        if key in venue_lower:
            return coords
    return (41.0082, 28.9784)

def clean_price(price_str: str) -> str:
    if not price_str:
        return 'Fiyat TBA'
    numbers = re.findall(r'\d+(?:[.,]\d+)?', price_str.replace('.', ''))
    if numbers:
        try:
            return f"{int(float(numbers[0].replace(',', '.')))}₺"
        except:
            pass
    return price_str.strip() or 'Fiyat TBA'

def parse_turkish_date(text: str) -> Optional[date]:
    if not text:
        return None
    text = text.strip().lower()
    MONTHS = {
        'ocak': 1, 'jan': 1, 'şubat': 2, 'feb': 2,
        'mart': 3, 'mar': 3, 'nisan': 4, 'apr': 4,
        'mayıs': 5, 'may': 5, 'haziran': 6, 'jun': 6,
        'temmuz': 7, 'jul': 7, 'ağustos': 8, 'aug': 8,
        'eylül': 9, 'sep': 9, 'ekim': 10, 'oct': 10,
        'kasım': 11, 'nov': 11, 'aralık': 12, 'dec': 12,
    }
    # DD.MM.YYYY
    m = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', text)
    if m:
        try:
            return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except: pass
    # DD Month YYYY
    m = re.search(r'(\d{1,2})\s+([a-zşçığüö]+)\s*(\d{4})?', text)
    if m:
        day = int(m.group(1))
        month_str = m.group(2)
        year = int(m.group(3)) if m.group(3) else datetime.now().year
        for month_name, month_num in MONTHS.items():
            if month_name in month_str:
                try:
                    return date(year, month_num, day)
                except: pass
    # YYYY-MM-DD
    m = re.search(r'(\d{4})-(\d{2})-(\d{2})', text)
    if m:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    return None

async def scrape_biletix(page):
    print("🎫 Biletix scraping...")
    concerts = []
    try:
        await page.goto('https://www.biletix.com/category/MUSIC/ISTANBUL/tr',
                       wait_until='networkidle', timeout=30000)
        await asyncio.sleep(3)
        
        # Try multiple possible selectors
        selectors = ['.eventCard', '.event-card', '[data-testid="event-card"]', 
                    '.card', '.concert', 'article']
        
        events = []
        for sel in selectors:
            events = await page.query_selector_all(sel)
            if events:
                print(f"  Biletix: {len(events)} events with selector '{sel}'")
                break
        
        for event in events[:40]:
            try:
                texts = await event.inner_text()
                link_el = await event.query_selector('a')
                link = await link_el.get_attribute('href') if link_el else ''
                
                lines = [l.strip() for l in texts.split('\n') if l.strip()]
                if not lines:
                    continue
                    
                title = lines[0]
                venue = next((l for l in lines[1:] if any(v in l.lower() for v in ['hall', 'arena', 'psm', 'joker', 'sahne', 'merkezi'])), '')
                date_str = next((l for l in lines if re.search(r'\d{1,2}[./]\d{1,2}|\d{1,2}\s+[a-zşçığüö]+', l.lower())), '')
                price_str = next((l for l in lines if '₺' in l or 'tl' in l.lower()), '')
                
                concert_date = parse_turkish_date(date_str)
                if not concert_date or concert_date < date.today():
                    continue
                
                full_url = f"https://www.biletix.com{link}" if link and link.startswith('/') else (link or 'https://www.biletix.com')
                genre = detect_genre(title + ' ' + venue)
                district = detect_district(venue)
                lat, lng = get_coords(venue)
                
                concerts.append({
                    'title': title[:200],
                    'artist': title.split(' - ')[0][:200],
                    'venue': venue[:200],
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': clean_price(price_str),
                    'genre': genre,
                    'ticket_url': full_url[:500],
                    'district': district,
                    'going_count': 0,
                    'lat': lat,
                    'lng': lng,
                    'source': 'biletix',
                })
            except Exception as e:
                continue
    except Exception as e:
        print(f"  Biletix error: {e}")
    print(f"  ✅ Biletix: {len(concerts)} concerts")
    return concerts

async def scrape_bubilet(page):
    print("🎫 Bubilet scraping...")
    concerts = []
    try:
        await page.goto('https://www.bubilet.com.tr/istanbul/kategori/konser',
                       wait_until='networkidle', timeout=30000)
        await asyncio.sleep(3)
        
        selectors = ['.event-item', '.etkinlik-kart', '.concert-card', 
                    '[class*="event"]', '[class*="card"]', 'article']
        events = []
        for sel in selectors:
            events = await page.query_selector_all(sel)
            if len(events) > 2:
                print(f"  Bubilet: {len(events)} events with '{sel}'")
                break
        
        for event in events[:40]:
            try:
                texts = await event.inner_text()
                link_el = await event.query_selector('a')
                link = await link_el.get_attribute('href') if link_el else ''
                
                lines = [l.strip() for l in texts.split('\n') if l.strip()]
                if not lines:
                    continue
                    
                title = lines[0]
                venue = next((l for l in lines[1:] if any(v in l.lower() for v in ['hall', 'arena', 'psm', 'joker', 'sahne', 'merkezi', 'istanbul'])), '')
                date_str = next((l for l in lines if re.search(r'\d{1,2}[./]\d{1,2}|\d{1,2}\s+[a-zşçığüö]+', l.lower())), '')
                price_str = next((l for l in lines if '₺' in l or 'tl' in l.lower()), '')
                
                concert_date = parse_turkish_date(date_str)
                if not concert_date or concert_date < date.today():
                    continue
                
                full_url = f"https://www.bubilet.com.tr{link}" if link and link.startswith('/') else (link or 'https://www.bubilet.com.tr')
                genre = detect_genre(title + ' ' + venue)
                district = detect_district(venue)
                lat, lng = get_coords(venue)
                
                concerts.append({
                    'title': title[:200],
                    'artist': title[:200],
                    'venue': venue[:200],
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': clean_price(price_str),
                    'genre': genre,
                    'ticket_url': full_url[:500],
                    'district': district,
                    'going_count': 0,
                    'lat': lat,
                    'lng': lng,
                    'source': 'bubilet',
                })
            except:
                continue
    except Exception as e:
        print(f"  Bubilet error: {e}")
    print(f"  ✅ Bubilet: {len(concerts)} concerts")
    return concerts

async def scrape_bugece(page):
    print("🎫 Bugece scraping...")
    concerts = []
    try:
        await page.goto('https://bugece.co/tr/browse/istanbul/events',
                       wait_until='networkidle', timeout=30000)
        await asyncio.sleep(3)
        
        selectors = ['[class*="EventCard"]', '[class*="event-card"]', 
                    '[class*="Card"]', 'article', '[class*="event"]']
        events = []
        for sel in selectors:
            events = await page.query_selector_all(sel)
            if len(events) > 2:
                print(f"  Bugece: {len(events)} events with '{sel}'")
                break
        
        for event in events[:40]:
            try:
                texts = await event.inner_text()
                link_el = await event.query_selector('a')
                link = await link_el.get_attribute('href') if link_el else ''
                
                lines = [l.strip() for l in texts.split('\n') if l.strip()]
                if not lines:
                    continue
                    
                title = lines[0]
                venue = next((l for l in lines[1:] if len(l) > 3 and l != title), '')
                date_str = next((l for l in lines if re.search(r'\d{1,2}[./]\d{1,2}|\d{1,2}\s+[a-zşçığüö]+', l.lower())), '')
                price_str = next((l for l in lines if '₺' in l or 'tl' in l.lower()), '')
                
                concert_date = parse_turkish_date(date_str)
                if not concert_date or concert_date < date.today():
                    continue
                
                full_url = f"https://bugece.co{link}" if link and link.startswith('/') else (link or 'https://bugece.co')
                genre = detect_genre(title + ' ' + venue)
                district = detect_district(venue)
                lat, lng = get_coords(venue)
                
                concerts.append({
                    'title': title[:200],
                    'artist': title[:200],
                    'venue': venue[:200],
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': clean_price(price_str),
                    'genre': genre,
                    'ticket_url': full_url[:500],
                    'district': district,
                    'going_count': 0,
                    'lat': lat,
                    'lng': lng,
                    'source': 'bugece',
                })
            except:
                continue
    except Exception as e:
        print(f"  Bugece error: {e}")
    print(f"  ✅ Bugece: {len(concerts)} concerts")
    return concerts

def upsert_concerts(sb: Client, concerts: list) -> int:
    inserted = 0
    for concert in concerts:
        try:
            existing = sb.table('concerts').select('id').eq('title', concert['title']).eq('date', concert['date']).execute()
            if existing.data:
                sb.table('concerts').update({
                    'venue': concert['venue'],
                    'price': concert['price'],
                    'ticket_url': concert['ticket_url'],
                }).eq('id', existing.data[0]['id']).execute()
            else:
                sb.table('concerts').insert(concert).execute()
                inserted += 1
        except Exception as e:
            print(f"  DB error: {e}")
    return inserted

async def main():
    print("🚀 Setlist Scraper başlıyor...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)
    
    if not SUPABASE_KEY:
        print("❌ SUPABASE_SERVICE_KEY bulunamadı!")
        return
    
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    all_concerts = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 720}
        )
        page = await context.new_page()
        
        biletix = await scrape_biletix(page)
        bubilet = await scrape_bubilet(page)
        bugece = await scrape_bugece(page)
        
        all_concerts = biletix + bubilet + bugece
        await browser.close()
    
    print("=" * 50)
    print(f"📊 Toplam {len(all_concerts)} konser bulundu")
    
    if all_concerts:
        inserted = upsert_concerts(sb, all_concerts)
        print(f"✅ {inserted} yeni konser eklendi, {len(all_concerts)-inserted} güncellendi")
    else:
        print("⚠️  Konser bulunamadı")
    
    print("✨ Tamamlandı!")

if __name__ == "__main__":
    asyncio.run(main())
