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

# ── Panel de configuración (⚙) ───────────────────────────────────────────────
# Mismo patrón que el resto de apps. NOTA: _env (arriba) apunta a
# parent.parent, que en Docker no resuelve a nada real — Dockerfile.web solo
# copia web/ a /app, así que "la carpeta padre" dentro del contenedor es "/"
# y ese .env nunca existió ahí (por eso el loader manual de arriba es en la
# práctica un no-op en Docker; las vars llegan igualmente vía env_file). El
# panel usa su propia ruta, correcta y montada de verdad: /app/.env.
SETTINGS_ENV_PATH = Path(__file__).parent / '.env'
SETTINGS_PASSWORD = os.environ.get("SETTINGS_PASSWORD", "")
VARS_SPEC = [
    {"name": "TELEGRAM_BOT_TOKEN", "secret": True, "help": "Token del bot de Telegram principal"},
    {"name": "TELEGRAM_BOT_CONCIERTOS", "secret": True, "help": "Token del bot de Telegram de conciertos (alternativa a TELEGRAM_BOT_TOKEN)"},
    {"name": "ADMIN_BOT_TOKEN", "secret": True, "help": "Token del bot de notificaciones a admin"},
    {"name": "ADMIN_CHAT_ID", "secret": False, "help": "Chat ID de Telegram del admin"},
    {"name": "TICKETMASTER_API_KEY", "secret": True, "help": "API key de Ticketmaster"},
    {"name": "SPOTIFY_CLIENT_ID", "secret": False, "help": "Client ID de Spotify"},
    {"name": "SPOTIFY_CLIENT_SECRET", "secret": True, "help": "Client secret de Spotify"},
    {"name": "GOOGLE_CLIENT_ID", "secret": False, "help": "Client ID de Google"},
    {"name": "GOOGLE_CLIENT_SECRET", "secret": True, "help": "Client secret de Google"},
    {"name": "COUNTRY_CITY_API_KEY", "secret": True, "help": "API key de geolocalización país/ciudad"},
    {"name": "LASTFM_API_KEY", "secret": True, "help": "API key de Last.fm"},
    {"name": "CACHE_DIR", "secret": False, "help": "Directorio de caché"},
]
_HAS_SECRETS = any(v.get("secret") for v in VARS_SPEC)


def _read_env_file(path):
    values = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            values[k.strip()] = v
    return values


def _write_env_file(path, updates):
    lines = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    seen = set()
    out = []
    for line in lines:
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k = s.split("=", 1)[0].strip()
            if k in updates:
                out.append(f"{k}={updates[k]}\n")
                seen.add(k)
                continue
        out.append(line)
    for k, v in updates.items():
        if k not in seen:
            if out and not out[-1].endswith("\n"):
                out[-1] += "\n"
            out.append(f"{k}={v}\n")
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(out)


def _current_value(spec):
    file_vals = _read_env_file(SETTINGS_ENV_PATH)
    if spec["name"] in file_vals:
        return file_vals[spec["name"]]
    return os.environ.get(spec["name"], spec.get("default", ""))


def _check_auth(password):
    if not SETTINGS_PASSWORD:
        return not _HAS_SECRETS
    return password == SETTINGS_PASSWORD


@app.route("/api/settings", methods=["POST"])
def api_settings():
    d = request.get_json(silent=True) or {}
    password = d.get("password") or ""
    requires = bool(SETTINGS_PASSWORD) or _HAS_SECRETS
    authorized = _check_auth(password)
    if requires and not authorized:
        error = "Contraseña incorrecta" if password else None
        if not SETTINGS_PASSWORD:
            error = "Este servicio tiene credenciales pero no hay SETTINGS_PASSWORD configurada. Añádela al .env y reinicia el contenedor."
        return jsonify({"requires_password": True, "authorized": False, "error": error})
    vars_out = [
        {"name": v["name"], "value": _current_value(v), "secret": v["secret"], "help": v.get("help", "")}
        for v in VARS_SPEC
    ]
    return jsonify({"requires_password": requires, "authorized": True, "vars": vars_out})


@app.route("/api/settings/save", methods=["POST"])
def api_settings_save():
    d = request.get_json(silent=True) or {}
    if not _check_auth(d.get("password") or ""):
        return jsonify({"error": "Contraseña incorrecta"}), 403
    known = {v["name"] for v in VARS_SPEC}
    updates = {k: v for k, v in (d.get("values") or {}).items() if k in known}
    if not updates:
        return jsonify({"error": "Nada que guardar"}), 400
    _write_env_file(SETTINGS_ENV_PATH, updates)
    return jsonify({"ok": True, "message": "Guardado. Reinicia el/los contenedor(es) (bot_conciertos y bot_conciertos_web) para aplicar los cambios."})


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
