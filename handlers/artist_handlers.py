#!/usr/bin/env python3
"""
Handlers for the artist detail panel (triggered by clicking an artist in /list).

Callback data conventions (all start with "art_"):
  art_<id>           — open artist detail (new reply message)
  art_b_<id>         — back to artist detail (edit current message)
  art_i_<id>         — info panel
  art_a_<id>_<page>  — albums panel (paginated)
  art_c_<id>         — concerts panel
  art_l_<id>_<page>  — lyrics search panel (paginated)
  art_s_<id>_<pg>_<i>— show lyrics for song at index i on page pg
  art_noop           — no-op (page counter buttons)
"""

import os
import re
import logging
import asyncio
import requests
from typing import Optional, Dict, List, Tuple

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)

GENIUS_KEY = os.getenv('GENIUS_API_KEY', '')
LASTFM_KEY = os.getenv('LASTFM_API_KEY', '')

_LASTFM_BASE = 'http://ws.audioscrobbler.com/2.0/'
_GENIUS_BASE = 'https://api.genius.com'

# ── helpers ──────────────────────────────────────────────────────────────────

def _get_lastfm_artist_info(artist_name: str, mbid: str = '') -> Optional[Dict]:
    """Fetch artist info from Last.fm (image, bio, stats, genres)."""
    if not LASTFM_KEY:
        return None
    try:
        params = {
            'method': 'artist.getInfo',
            'api_key': LASTFM_KEY,
            'format': 'json',
            'autocorrect': 1,
        }
        if mbid:
            params['mbid'] = mbid
        else:
            params['artist'] = artist_name
        r = requests.get(_LASTFM_BASE, params=params, timeout=8)
        data = r.json()
        artist = data.get('artist')
        if not artist:
            return None

        # Image — pick largest non-placeholder
        image_url = None
        for img in reversed(artist.get('image', [])):
            url = img.get('#text', '')
            if url and '/noimage/' not in url and url.startswith('http'):
                image_url = url
                break

        # Stats
        stats = artist.get('stats', {})
        listeners = int(stats.get('listeners', 0))
        playcount = int(stats.get('playcount', 0))

        # Bio (strip HTML and "Read more" link)
        bio_raw = artist.get('bio', {}).get('summary', '')
        bio_raw = re.sub(r'<a [^>]*>Read more on Last\.fm</a>\.?', '', bio_raw, flags=re.I)
        bio_raw = re.sub(r'<[^>]+>', '', bio_raw).strip()
        bio = bio_raw[:400] if bio_raw else ''

        # Genres
        tags_data = artist.get('tags', {}).get('tag', [])
        if isinstance(tags_data, dict):
            tags_data = [tags_data]
        genres = [t['name'] for t in tags_data[:5] if t.get('name')]

        # Similar artists
        sim_data = artist.get('similar', {}).get('artist', [])
        if isinstance(sim_data, dict):
            sim_data = [sim_data]
        similar = [a['name'] for a in sim_data[:3] if a.get('name')]

        return {
            'image_url': image_url,
            'listeners': listeners,
            'playcount': playcount,
            'bio': bio,
            'genres': genres,
            'similar': similar,
            'lastfm_url': artist.get('url', ''),
        }
    except Exception as e:
        logger.debug(f"Last.fm info error for {artist_name!r}: {e}")
        return None


def _get_mb_release_groups(mbid: str, page: int, per_page: int = 8) -> Tuple[List[Dict], int]:
    """Fetch release groups from MusicBrainz for an artist."""
    try:
        import musicbrainzngs
        musicbrainzngs.set_useragent('tumtumpa-bot', '1.0', 'viciosmusicales@gmail.com')
        result = musicbrainzngs.get_artist_by_id(mbid, includes=['release-groups'])
        rgs = result['artist'].get('release-group-list', [])
        rgs.sort(key=lambda x: x.get('first-release-date', '0000'), reverse=True)
        total = len(rgs)
        start = page * per_page
        return rgs[start:start + per_page], total
    except Exception as e:
        logger.debug(f"MB release-groups error for {mbid!r}: {e}")
        return [], 0


def _search_genius(artist_name: str, page: int, per_page: int = 8) -> Tuple[List[Dict], bool]:
    """Search Genius for songs by artist name. Returns (songs, has_more)."""
    if not GENIUS_KEY:
        return [], False
    try:
        r = requests.get(
            f'{_GENIUS_BASE}/search',
            headers={'Authorization': f'Bearer {GENIUS_KEY}'},
            params={'q': artist_name, 'per_page': 20},
            timeout=10,
        )
        hits = r.json().get('response', {}).get('hits', [])
        name_lower = artist_name.lower()
        songs = []
        for hit in hits:
            result = hit.get('result', {})
            primary = result.get('primary_artist', {})
            pname = primary.get('name', '').lower()
            if name_lower in pname or pname in name_lower:
                songs.append({
                    'id': result['id'],
                    'title': result['title'],
                    'url': result['url'],
                })
        start = page * per_page
        has_more = len(songs) > start + per_page
        return songs[start:start + per_page], has_more
    except Exception as e:
        logger.debug(f"Genius search error for {artist_name!r}: {e}")
        return [], False


def _fetch_genius_lyrics(song_url: str) -> Optional[str]:
    """Scrape lyrics from a Genius song page."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0'}
        r = requests.get(song_url, headers=headers, timeout=12)
        html = r.text
        containers = re.findall(
            r'data-lyrics-container="true"[^>]*>(.*?)</div>',
            html, re.DOTALL
        )
        if not containers:
            return None
        text = '\n'.join(containers)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
        text = re.sub(r'<[^>]+>', '', text)
        for entity, char in [
            ('&amp;', '&'), ('&lt;', '<'), ('&gt;', '>'),
            ('&quot;', '"'), ('&#x27;', "'"), ('&nbsp;', ' '),
        ]:
            text = text.replace(entity, char)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        return text or None
    except Exception as e:
        logger.debug(f"Genius lyrics scrape error for {song_url}: {e}")
        return None


def _fmt_num(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.0f}K"
    return str(n)


def _main_buttons(artist_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ℹ️ Info", callback_data=f"art_i_{artist_id}"),
            InlineKeyboardButton("💿 Álbums", callback_data=f"art_a_{artist_id}_0"),
        ],
        [
            InlineKeyboardButton("🎤 Conciertos", callback_data=f"art_c_{artist_id}"),
            InlineKeyboardButton("📝 Letras", callback_data=f"art_l_{artist_id}_0"),
        ],
    ])


def _back_button(artist_id: int, label: str = "⬅️ Volver") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton(label, callback_data=f"art_b_{artist_id}")
    ]])


def _artist_detail_text(artist: Dict) -> str:
    lines = [f"*{artist['name']}*"]
    parts = []
    if artist.get('country'):
        parts.append(f"🌍 {artist['country']}")
    if artist.get('formed_year'):
        year = str(artist['formed_year'])
        if artist.get('ended_year'):
            year += f"–{artist['ended_year']}"
        parts.append(f"📅 {year}")
    if artist.get('artist_type'):
        parts.append(f"🎭 {artist['artist_type'].title()}")
    if parts:
        lines.append('  '.join(parts))
    if artist.get('musicbrainz_url'):
        lines.append(f"[MusicBrainz]({artist['musicbrainz_url']})")
    return '\n'.join(lines)


# ── handler class ─────────────────────────────────────────────────────────────

class ArtistHandlers:

    def __init__(self, database, services=None):
        self.db = database
        self.services = services or {}

    def _get_artist(self, artist_id: int) -> Optional[Dict]:
        return self.db.get_artist_by_id(artist_id)

    # ── dispatch ──────────────────────────────────────────────────────────────

    async def art_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Single entry point for all art_* callbacks."""
        query = update.callback_query
        data = query.data

        if data == 'art_noop':
            await query.answer()
            return

        if re.match(r'^art_\d+$', data):
            await self._detail_new(update, context)
        elif re.match(r'^art_b_\d+$', data):
            await self._detail_edit(update, context)
        elif re.match(r'^art_i_\d+$', data):
            await self._info(update, context)
        elif re.match(r'^art_a_\d+_\d+$', data):
            await self._albums(update, context)
        elif re.match(r'^art_c_\d+$', data):
            await self._concerts(update, context)
        elif re.match(r'^art_l_\d+_\d+$', data):
            await self._lyrics_list(update, context)
        elif re.match(r'^art_s_\d+_\d+_\d+$', data):
            await self._lyrics_song(update, context)
        else:
            await query.answer()

    # ── artist detail (new reply) ──────────────────────────────────────────────

    async def _detail_new(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_<id> — send artist detail as a NEW reply to the list message."""
        query = update.callback_query
        await query.answer()
        artist_id = int(query.data.split('_')[1])
        artist = self._get_artist(artist_id)
        if not artist:
            await query.answer("Artista no encontrado", show_alert=True)
            return
        await query.message.reply_text(
            _artist_detail_text(artist),
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=_main_buttons(artist_id),
        )

    # ── artist detail (edit in place — back from sub-panel) ──────────────────

    async def _detail_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_b_<id> — edit current message back to artist detail."""
        query = update.callback_query
        await query.answer()
        artist_id = int(query.data.split('_')[2])
        artist = self._get_artist(artist_id)
        if not artist:
            return
        try:
            await query.edit_message_text(
                _artist_detail_text(artist),
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=_main_buttons(artist_id),
            )
        except Exception:
            pass

    # ── info panel ────────────────────────────────────────────────────────────

    async def _info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_i_<id> — show artist info from Last.fm + DB."""
        query = update.callback_query
        await query.answer("Cargando info…")
        artist_id = int(query.data.split('_')[2])
        artist = self._get_artist(artist_id)
        if not artist:
            return

        name = artist['name']
        mbid = artist.get('mbid', '')

        loop = asyncio.get_event_loop()
        lfm = await loop.run_in_executor(None, _get_lastfm_artist_info, name, mbid)

        lines = [f"*{name}*\n"]

        if lfm:
            if lfm['listeners']:
                lines.append(f"👥 {_fmt_num(lfm['listeners'])} oyentes")
            if lfm['genres']:
                lines.append(f"🎸 {', '.join(lfm['genres'])}")
            if lfm['bio']:
                lines.append(f"\n_{lfm['bio']}_")
            if lfm['similar']:
                lines.append(f"\n🔗 Similares: {', '.join(lfm['similar'])}")

        db_parts = []
        if artist.get('country'):
            db_parts.append(f"🌍 {artist['country']}")
        if artist.get('formed_year'):
            y = str(artist['formed_year'])
            if artist.get('ended_year'):
                y += f"–{artist['ended_year']}"
            db_parts.append(f"📅 {y}")
        if db_parts:
            lines.append('\n' + '  '.join(db_parts))

        link_parts = []
        if artist.get('musicbrainz_url'):
            link_parts.append(f"[MusicBrainz]({artist['musicbrainz_url']})")
        if lfm and lfm.get('lastfm_url'):
            link_parts.append(f"[Last.fm]({lfm['lastfm_url']})")
        wp = name.replace(' ', '_')
        link_parts.append(f"[Wikipedia](https://en.wikipedia.org/wiki/{wp})")
        lines.append('\n' + ' · '.join(link_parts))

        text = '\n'.join(lines)[:4000]
        try:
            await query.edit_message_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=_back_button(artist_id),
            )
        except Exception:
            await query.message.reply_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=_back_button(artist_id),
            )

    # ── albums panel ──────────────────────────────────────────────────────────

    async def _albums(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_a_<id>_<page> — show release groups from MusicBrainz."""
        query = update.callback_query
        await query.answer("Cargando álbums…")
        parts = query.data.split('_')
        artist_id = int(parts[2])
        page = int(parts[3])
        artist = self._get_artist(artist_id)
        if not artist:
            return

        name = artist['name']
        mbid = artist.get('mbid', '')
        per_page = 8

        if not mbid:
            await query.edit_message_text(
                f"*{name}*\n\n❌ Sin MBID, no se pueden obtener álbums.",
                parse_mode='Markdown',
                reply_markup=_back_button(artist_id),
            )
            return

        loop = asyncio.get_event_loop()
        rgs, total = await loop.run_in_executor(None, _get_mb_release_groups, mbid, page, per_page)

        total_pages = max(1, (total + per_page - 1) // per_page)
        lines = [f"*{name}* — Discografía ({total})\n"]

        TYPE_EMOJI = {
            'Album': '💿', 'Single': '🎵', 'EP': '📀',
            'Live': '🎤', 'Compilation': '📦',
        }
        for rg in rgs:
            rg_type = rg.get('type', '')
            emoji = TYPE_EMOJI.get(rg_type, '🎶')
            title = rg.get('title', '?')
            year = rg.get('first-release-date', '')[:4] or '?'
            mb_url = f"https://musicbrainz.org/release-group/{rg.get('id', '')}"
            lines.append(f"{emoji} [{title}]({mb_url}) ({year})")

        discogs_url = f"https://www.discogs.com/search/?q={name.replace(' ', '+')}&type=release"
        lines.append(f"\n[🔍 Discogs]({discogs_url})")

        text = '\n'.join(lines)[:4000]

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"art_a_{artist_id}_{page-1}"))
        nav.append(InlineKeyboardButton(f"{page+1}/{total_pages}", callback_data="art_noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"art_a_{artist_id}_{page+1}"))

        keyboard = [nav] if len(nav) > 1 else []
        keyboard.append([InlineKeyboardButton("⬅️ Volver", callback_data=f"art_b_{artist_id}")])

        try:
            await query.edit_message_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception:
            await query.message.reply_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup(keyboard),
            )

    # ── concerts panel ────────────────────────────────────────────────────────

    async def _concerts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_c_<id> — show upcoming concerts from DB for this artist."""
        query = update.callback_query
        await query.answer()
        artist_id = int(query.data.split('_')[2])
        artist = self._get_artist(artist_id)
        if not artist:
            return

        name = artist['name']
        concerts = self.db.get_concerts_for_artist(name, upcoming_only=True)

        if not concerts:
            text = f"*{name}*\n\n🎫 Sin conciertos próximos en la base de datos."
        else:
            lines = [f"*{name}* — Conciertos próximos\n"]
            for c in concerts[:15]:
                d = c.get('date', '?')
                venue = c.get('venue') or '?'
                city = c.get('city') or ''
                url = c.get('url') or ''
                location = f"{venue}, {city}" if city else venue
                if url:
                    lines.append(f"📅 {d} — [{location}]({url})")
                else:
                    lines.append(f"📅 {d} — {location}")
            text = '\n'.join(lines)[:4000]

        try:
            await query.edit_message_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=_back_button(artist_id),
            )
        except Exception:
            await query.message.reply_text(
                text, parse_mode='Markdown', disable_web_page_preview=True,
                reply_markup=_back_button(artist_id),
            )

    # ── lyrics list panel ─────────────────────────────────────────────────────

    async def _lyrics_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_l_<id>_<page> — search Genius and show clickable song list."""
        query = update.callback_query
        await query.answer("Buscando canciones…")
        parts = query.data.split('_')
        artist_id = int(parts[2])
        page = int(parts[3])
        artist = self._get_artist(artist_id)
        if not artist:
            return

        name = artist['name']

        if not GENIUS_KEY:
            await query.edit_message_text(
                f"*{name}*\n\n❌ GENIUS_API_KEY no configurada.",
                parse_mode='Markdown',
                reply_markup=_back_button(artist_id),
            )
            return

        loop = asyncio.get_event_loop()
        songs, has_more = await loop.run_in_executor(None, _search_genius, name, page, 8)

        if not songs:
            await query.edit_message_text(
                f"*{name}*\n\n❌ No se encontraron canciones en Genius.",
                parse_mode='Markdown',
                reply_markup=_back_button(artist_id),
            )
            return

        # Cache songs in user_data for the song lyric handler
        if context.user_data is None:
            context._user_data = {}
        context.user_data[f"art_songs_{artist_id}_{page}"] = songs

        lines = [f"*{name}* — Letras\n"]
        keyboard = []
        for i, song in enumerate(songs):
            lines.append(f"{i+1}. {song['title']}")
            keyboard.append([InlineKeyboardButton(
                song['title'][:50],
                callback_data=f"art_s_{artist_id}_{page}_{i}",
            )])

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("⬅️", callback_data=f"art_l_{artist_id}_{page-1}"))
        if has_more:
            nav.append(InlineKeyboardButton("➡️", callback_data=f"art_l_{artist_id}_{page+1}"))
        if nav:
            keyboard.append(nav)
        keyboard.append([InlineKeyboardButton("⬅️ Volver", callback_data=f"art_b_{artist_id}")])

        text = '\n'.join(lines)
        try:
            await query.edit_message_text(
                text, parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
        except Exception:
            await query.message.reply_text(
                text, parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup(keyboard),
            )

    # ── song lyrics ───────────────────────────────────────────────────────────

    async def _lyrics_song(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """art_s_<id>_<page>_<idx> — fetch and display song lyrics."""
        query = update.callback_query
        await query.answer("Cargando letra…")
        parts = query.data.split('_')
        artist_id = int(parts[2])
        page = int(parts[3])
        idx = int(parts[4])

        songs = (context.user_data or {}).get(f"art_songs_{artist_id}_{page}")
        back_songs = _back_button(artist_id, f"⬅️ Canciones")
        # Override back to go to lyrics list
        back_songs = InlineKeyboardMarkup([[
            InlineKeyboardButton("⬅️ Canciones", callback_data=f"art_l_{artist_id}_{page}")
        ]])

        if not songs or idx >= len(songs):
            await query.edit_message_text(
                "❌ Sesión expirada. Pulsa *Letras* de nuevo.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("📝 Letras", callback_data=f"art_l_{artist_id}_0")
                ]]),
            )
            return

        song = songs[idx]
        song_title = song['title']
        song_url = song['url']

        loop = asyncio.get_event_loop()
        lyrics = await loop.run_in_executor(None, _fetch_genius_lyrics, song_url)

        if not lyrics:
            await query.edit_message_text(
                f"*{song_title}*\n\n❌ No se pudo obtener la letra.\n[Ver en Genius]({song_url})",
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=back_songs,
            )
            return

        text = f"*{song_title}*\n\n{lyrics}"
        if len(text) > 4000:
            text = text[:3980] + '\n\n_[…continúa en Genius]_'

        try:
            await query.edit_message_text(
                text, parse_mode='Markdown',
                reply_markup=back_songs,
            )
        except Exception:
            await query.message.reply_text(
                text, parse_mode='Markdown',
                reply_markup=back_songs,
            )
