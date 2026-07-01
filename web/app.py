#!/usr/bin/env python3
import os
import re
import sqlite3
import requests
from pathlib import Path
from flask import Flask, jsonify, request, render_template

# Load .env from parent directory
_env = Path(__file__).parent.parent / '.env'
if _env.exists():
    for line in _env.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            k, _, v = line.partition('=')
            os.environ.setdefault(k.strip(), v.strip())

DB_PATH = os.environ.get('DB_PATH', str(Path(__file__).parent.parent / 'artist_tracker.db'))
LASTFM_API_KEY = os.environ.get('LASTFM_API_KEY', '')

app = Flask(__name__)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/users')
def api_users():
    conn = get_db()
    try:
        rows = conn.execute(
            'SELECT id, username, chat_id FROM users ORDER BY username'
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/user/<int:user_id>/events')
def api_user_events(user_id):
    conn = get_db()
    try:
        concerts = conn.execute("""
            SELECT DISTINCT c.id, c.artist_name, c.concert_name, c.venue, c.city,
                   c.country, c.date, c.time, c.url, c.source
            FROM concerts c
            JOIN artists a ON LOWER(c.artist_name) = LOWER(a.name)
            JOIN user_followed_artists ufa ON a.id = ufa.artist_id
            WHERE ufa.user_id = ?
              AND c.date >= date('now', '-7 days')
            ORDER BY c.date ASC
        """, (user_id,)).fetchall()

        releases = conn.execute("""
            SELECT r.id, r.artist_name, r.artist_mbid, r.release_title,
                   r.release_date, r.release_type, r.mb_release_id,
                   r.yt_url, r.yt_query
            FROM releases r
            JOIN user_releases ur ON ur.release_id = r.id
            WHERE ur.user_id = ?
              AND r.release_date >= date('now', '-30 days')
            ORDER BY r.release_date ASC
        """, (user_id,)).fetchall()

        return jsonify({
            'concerts': [dict(r) for r in concerts],
            'releases': [dict(r) for r in releases],
        })
    finally:
        conn.close()


@app.route('/api/artist/info')
def api_artist_info():
    name = request.args.get('name', '').strip()
    mbid = request.args.get('mbid', '').strip()
    result = {'name': name, 'bio': None, 'image_url': None, 'tags': [], 'listeners': 0}

    if not LASTFM_API_KEY or not name:
        return jsonify(result)

    try:
        params = {
            'method': 'artist.getinfo',
            'api_key': LASTFM_API_KEY,
            'format': 'json',
            'autocorrect': 1,
            'artist': name,
        }
        if mbid:
            params['mbid'] = mbid

        resp = requests.get('http://ws.audioscrobbler.com/2.0/', params=params, timeout=6)
        data = resp.json()

        if 'artist' not in data:
            return jsonify(result)

        artist = data['artist']

        bio_raw = artist.get('bio', {}).get('summary', '')
        bio_clean = re.sub(r'<a[^>]*>.*?</a>', '', bio_raw, flags=re.DOTALL)
        bio_clean = re.sub(r'<[^>]+>', '', bio_clean).strip()
        result['bio'] = bio_clean[:700] if bio_clean else None

        for img in reversed(artist.get('image', [])):
            if img.get('#text'):
                result['image_url'] = img['#text']
                break

        tags = artist.get('tags', {}).get('tag', [])
        if isinstance(tags, dict):
            tags = [tags]
        result['tags'] = [t['name'] for t in tags[:6]]

        stats = artist.get('stats', {})
        result['listeners'] = int(stats.get('listeners', 0) or 0)

    except Exception:
        pass

    return jsonify(result)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8747))
    debug = os.environ.get('FLASK_DEBUG', '0') == '1'
    app.run(host='0.0.0.0', port=port, debug=debug)
