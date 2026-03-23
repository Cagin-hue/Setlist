import asyncio
import json
import re
import os
import urllib.request
import urllib.parse
from datetime import datetime, date
from typing import Optional
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

GENRE_KEYWORDS = {
    'rock': ['rock', 'metal', 'punk', 'duman', 'pinhani', 'pentagram', 'yüksek sadakat', 'mor ve ötesi', 'kargo', 'athena'],
    'elec': ['elektronik', 'electronic', 'dj', 'techno', 'house', 'edm', 'cortini', 'synthesizer', 'rave', 'avangart'],
    'alt': ['alternatif', 'indie', 'folk', 'caz', 'jazz', 'blues', 'emir can', 'can bonomo', 'cem adrian', 'nardis'],
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
    nums = re.findall(r'\d+', str(s).replace('.', '').replace(',', ''))
    if nums:
        return f"{nums[0]}₺"
    return 'Fiyat TBA'

def fetch_url(url, headers=None):
    req = urllib.request.Request(url)
    req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    req.add_header('Accept', 'application/json, text/html')
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.read().decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"  Fetch error for {url}: {e}")
        return None

def scrape_biletix_rss():
    """Biletix RSS feed — most reliable"""
    print("🎫 Biletix RSS scraping...")
    concerts = []
    try:
        # Biletix RSS for Istanbul music
        url = "https://www.biletix.com/rss/category/MUSIC/ISTANBUL/tr"
        content = fetch_url(url)
        if not content:
            print("  Biletix RSS: no content")
            return []
        
        # Parse RSS items
        items = re.findall(r'<item>(.*?)</item>', content, re.DOTALL)
        print(f"  Found {len(items)} RSS items")
        
        for item in items:
            try:
                title = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', item)
                title = title.group(1).strip() if title else ''
                if not title:
                    title_m = re.search(r'<title>(.*?)</title>', item)
                    title = title_m.group(1).strip() if title_m else ''
                
                link = re.search(r'<link>(.*?)</link>', item)
                link = link.group(1).strip() if link else ''
                
                desc = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>', item, re.DOTALL)
                desc = desc.group(1) if desc else ''
                
                pub_date = re.search(r'<pubDate>(.*?)</pubDate>', item)
                pub_date = pub_date.group(1) if pub_date else ''
                
                # Extract venue from description
                venue = ''
                venue_m = re.search(r'(?:Mekan|Venue|Yer)[:\s]+([^\n<]+)', desc, re.IGNORECASE)
                if venue_m:
                    venue = venue_m.group(1).strip()
                
                # Extract date from description or pubDate
                date_str = ''
                date_m = re.search(r'(?:Tarih|Date)[:\s]+([^\n<]+)', desc, re.IGNORECASE)
                if date_m:
                    date_str = date_m.group(1).strip()
                
                if not date_str and pub_date:
                    # Parse RSS date format: "Thu, 21 Mar 2026 00:00:00 +0000"
                    date_m2 = re.search(r'\d{1,2}\s+\w+\s+\d{4}', pub_date)
                    if date_m2:
                        date_str = date_m2.group(0)
                
                # Extract price
                price_m = re.search(r'(\d+[\.,]?\d*)\s*[₺TL]', desc, re.IGNORECASE)
                price = f"{price_m.group(1)}₺" if price_m else 'Fiyat TBA'
                
                concert_date = None
                if date_str:
                    MONTHS_EN = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,
                                'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
                    m = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
                    if m:
                        day, month_str, year = int(m.group(1)), m.group(2), int(m.group(3))
                        month_num = MONTHS_EN.get(month_str[:3].title(), 0)
                        if month_num:
                            try:
                                concert_date = date(year, month_num, day)
                            except:
                                pass
                
                if not concert_date:
                    continue
                if concert_date < date.today():
                    continue
                
                concerts.append({
                    'title': title[:200],
                    'artist': title.split(' - ')[0][:200],
                    'venue': venue[:200] or 'İstanbul',
                    'date': concert_date.isoformat(),
                    'time': '21:00',
                    'price': price,
                    'genre': detect_genre(title + ' ' + venue),
                    'ticket_url': link[:500] or 'https://www.biletix.com',
                    'district': detect_district(venue),
                    'going_count': 0,
                    'lat': get_coords(venue)[0],
                    'lng': get_coords(venue)[1],
                    'source': 'biletix',
                })
            except Exception as e:
                continue
                
    except Exception as e:
        print(f"  Biletix RSS error: {e}")
    
    print(f"  ✅ Biletix RSS: {len(concerts)} concerts")
    return concerts

def scrape_ticketmaster():
    """Ticketmaster/Biletix API"""
    print("🎫 Ticketmaster API scraping...")
    concerts = []
    try:
        # Ticketmaster Discovery API - public endpoint for Turkey
        url = "https://app.ticketmaster.com/discovery/v2/events.json?classificationName=music&city=Istanbul&countryCode=TR&size=50&sort=date,asc"
        # Note: This requires API key but let's try without first
        content = fetch_url(url)
        if not content:
            return []
        
        data = json.loads(content)
        events = data.get('_embedded', {}).get('events', [])
        print(f"  Found {len(events)} Ticketmaster events")
        
        for event in events:
            try:
                title = event.get('name', '')
                dates = event.get('dates', {})
                start = dates.get('start', {})
                date_str = start.get('localDate', '')
                time_str = start.get('localTime', '21:00')[:5]
                
                venues = event.get('_embedded', {}).get('venues', [{}])
                venue = venues[0].get('name', '') if venues else ''
                
                price_ranges = event.get('priceRanges', [])
                price = f"{int(price_ranges[0].get('min', 0))}₺" if price_ranges else 'Fiyat TBA'
                
                url_link = event.get('url', '')
                
                concert_date = date.fromisoformat(date_str) if date_str else None
                if not concert_date or concert_date < date.today():
                    continue
                
                concerts.append({
                    'title': title[:200],
                    'artist': title[:200],
                    'venue': venue[:200],
                    'date': concert_date.isoformat(),
                    'time': time_str,
                    'price': price,
                    'genre': detect_genre(title + ' ' + venue),
                    'ticket_url': url_link[:500] or 'https://www.biletix.com',
                    'district': detect_district(venue),
                    'going_count': 0,
                    'lat': get_coords(venue)[0],
                    'lng': get_coords(venue)[1],
                    'source': 'ticketmaster',
                })
            except:
                continue
                
    except Exception as e:
        print(f"  Ticketmaster error: {e}")
    
    print(f"  ✅ Ticketmaster: {len(concerts)} concerts")
    return concerts

def scrape_setmore():
    """Try Setmore / Eventbrite style APIs"""
    print("🎫 Eventbrite API scraping...")
    concerts = []
    try:
        # Eventbrite public search
        url = "https://www.eventbriteapi.com/v3/events/search/?location.address=Istanbul&categories=103&sort_by=date&token=public"
        content = fetch_url(url)
        if not content:
            return []
        data = json.loads(content)
        events = data.get('events', [])
        print(f"  Found {len(events)} Eventbrite events")
    except Exception as e:
        print(f"  Eventbrite error: {e}")
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

def main():
    print("🚀 Setlist Scraper başlıyor...")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)
    
    if not SUPABASE_KEY:
        print("❌ SUPABASE_SERVICE_KEY bulunamadı!")
        return
    
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    biletix = scrape_biletix_rss()
    ticketmaster = scrape_ticketmaster()
    
    all_concerts = biletix + ticketmaster
    
    # Deduplicate
    seen = set()
    unique = []
    for c in all_concerts:
        key = (c['title'].lower(), c['date'])
        if key not in seen:
            seen.add(key)
            unique.append(c)
    
    print("=" * 50)
    print(f"📊 Toplam {len(unique)} benzersiz konser")
    
    if unique:
        inserted, updated = upsert_concerts(sb, unique)
        print(f"✅ {inserted} yeni eklendi, {updated} güncellendi")
    else:
        print("⚠️  Konser bulunamadı")
        print("💡 Manuel veri girişi için Supabase Table Editor kullanın")
    
    print("✨ Tamamlandı!")

if __name__ == "__main__":
    main()
