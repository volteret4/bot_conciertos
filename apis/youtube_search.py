#!/usr/bin/env python3
"""
YouTube video search for new releases.

Strategy (on/after release date only — never before):
  - Singles / EPs  → "{artist} {title} official"
  - Albums         → 1) "{artist} {title} full album"
                     2) MusicBrainz tracklist → first track found on YT
                     3) "{artist} {title}" generic fallback

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


def get_tracklist_from_mb(mb_release_id: str) -> list:
    """
    Fetch track titles for a MusicBrainz release ID.
    Returns a list of track title strings (empty on failure).
    Respects the 1 req/sec MB rate limit.
    """
    if not mb_release_id:
        return []

    import requests

    try:
        headers = {'User-Agent': 'tumtumpa-bot/1.0 (viciosmusicales@gmail.com)'}
        resp = requests.get(
            f'https://musicbrainz.org/ws/2/release/{mb_release_id}',
            params={'inc': 'recordings', 'fmt': 'json'},
            headers=headers,
            timeout=10,
        )
        time.sleep(1)  # MB rate limit: 1 req/sec

        if resp.status_code != 200:
            logger.warning(f"MB returned {resp.status_code} for release {mb_release_id}")
            return []

        data = resp.json()
        tracks = []
        for medium in data.get('media', []):
            for track in medium.get('tracks', []):
                title = track.get('title') or track.get('recording', {}).get('title', '')
                if title:
                    tracks.append(title)
        return tracks

    except Exception as e:
        logger.warning(f"MB tracklist lookup failed for {mb_release_id}: {e}")
        return []


def find_youtube_for_release(
    artist_name: str,
    release_title: str,
    release_date: str,
    release_type: str,
    artist_mbid: Optional[str] = None,
    mb_release_id: Optional[str] = None,
) -> tuple:
    """
    Return (url, query) for the best YouTube video for this release.

    Only searches on/after the release date — returns (None, None) for future releases
    so stale videos from previous works are never cached.

    Album search order:
      1. "{artist} {title} full album"
      2. MusicBrainz tracklist → first track found on YouTube
      3. "{artist} {title}" generic fallback
    """
    import datetime

    today = datetime.date.today().isoformat()

    # Never search before the release date
    if release_date and release_date > today:
        return None, None

    release_type_lower = (release_type or '').lower()

    # Singles and EPs: direct "official" search
    if release_type_lower in ('single', 'ep'):
        query = f"{artist_name} {release_title} official"
        return search_youtube(query), query

    # Albums: try full album video first
    query = f"{artist_name} {release_title} full album"
    url = search_youtube(query)
    if url:
        logger.info(f"Found full album YT for '{release_title}': {url}")
        return url, query

    # Fall back to MusicBrainz tracklist
    if mb_release_id:
        tracks = get_tracklist_from_mb(mb_release_id)
        for track in tracks[:3]:
            track_query = f"{artist_name} {track} official"
            url = search_youtube(track_query)
            if url:
                logger.info(f"Found YT via MB track '{track}' for album '{release_title}'")
                return url, track_query

    # Generic fallback
    query = f"{artist_name} {release_title}"
    return search_youtube(query), query
