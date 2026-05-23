#!/usr/bin/env python3
"""
YouTube video search for new releases.

Strategy:
  - Singles / EPs  → search directly: "{artist} {title} official"
  - Future albums  → query MusicBrainz for the most recent lead single, then
                     fall back to "{artist} {album}" if none found.
  - Past albums    → search "{artist} {title}" directly.

Uses yt-dlp (ytsearch) to resolve the final YouTube URL without downloading.
"""

import time
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def search_youtube(query: str) -> Optional[str]:
    """Return the first YouTube URL matching *query*, or None on failure."""
    try:
        import yt_dlp
    except ImportError:
        logger.error("yt-dlp not found — install it with: pacman -S yt-dlp")
        return None

    ydl_opts = {
        'quiet': True,
        'no_warnings': True,
        'extract_flat': True,
        'noplaylist': True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch1:{query}", download=False)
            if result and 'entries' in result and result['entries']:
                video_id = result['entries'][0].get('id')
                if video_id:
                    return f"https://www.youtube.com/watch?v={video_id}"
    except Exception as e:
        logger.warning(f"YT search failed for '{query}': {e}")
    return None


def get_lead_single_from_mb(artist_mbid: str, before_date: str) -> Optional[str]:
    """
    Find the title of the most recent single released by *artist_mbid*
    strictly before *before_date* (ISO YYYY-MM-DD).

    Respects the MusicBrainz 1-request-per-second rate limit via time.sleep.
    Returns None if nothing is found or on any error.
    """
    if not artist_mbid:
        return None

    import requests

    try:
        headers = {'User-Agent': 'tumtumpa-bot/1.0 (viciosmusicales@gmail.com)'}
        params = {
            'artist': artist_mbid,
            'type': 'single',
            'fmt': 'json',
            'limit': 25,
        }
        resp = requests.get(
            'https://musicbrainz.org/ws/2/release-group',
            params=params,
            headers=headers,
            timeout=10,
        )
        time.sleep(1)   # MB rate limit: 1 req/sec

        if resp.status_code != 200:
            logger.warning(f"MB returned {resp.status_code} for artist {artist_mbid}")
            return None

        rgs = resp.json().get('release-groups', [])
        candidates = [
            (rg['first-release-date'], rg.get('title', ''))
            for rg in rgs
            if rg.get('first-release-date') and rg['first-release-date'] < before_date
        ]
        if not candidates:
            return None

        candidates.sort(reverse=True)          # most recent first
        return candidates[0][1] or None

    except Exception as e:
        logger.warning(f"MB lead-single lookup failed for {artist_mbid}: {e}")
        return None


def find_youtube_for_release(
    artist_name: str,
    release_title: str,
    release_date: str,
    release_type: str,
    artist_mbid: Optional[str] = None,
) -> tuple:
    """
    Determine the best YouTube video for a release and return (url, query).

    - Singles/EPs  → direct search with 'official'.
    - Future albums → try lead single via MusicBrainz first.
    - Past albums   → search album title directly.

    Always returns a 2-tuple; url is None when nothing is found.
    """
    import datetime

    release_type_lower = (release_type or '').lower()
    today = datetime.date.today().isoformat()

    # Singles and EPs: search directly
    if release_type_lower in ('single', 'ep'):
        query = f"{artist_name} {release_title} official"
        return search_youtube(query), query

    # Future albums: try lead single via MusicBrainz
    is_future = bool(release_date) and release_date > today
    if is_future and artist_mbid:
        lead_single = get_lead_single_from_mb(artist_mbid, release_date)
        if lead_single:
            query = f"{artist_name} {lead_single} official"
            url = search_youtube(query)
            if url:
                logger.info(f"Found YT via lead single '{lead_single}' for album '{release_title}'")
                return url, query

    # Fallback: search for the album/release title
    query = f"{artist_name} {release_title}"
    return search_youtube(query), query
