# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**tumtumpá** is a Telegram bot for tracking concerts and album releases for favourite artists. It integrates Ticketmaster (concerts), Muspy (album releases), Last.fm (artist metadata enrichment), and MusicBrainz (artist MBID lookup). Users can push their calendar events to a Radicale CalDAV server.

## Running

```bash
python telegram_bot.py      # Main bot process (long-polling)
python notifications.py     # Weekly notification scheduler (separate process)
```

Required `.env` variables: `TELEGRAM_BOT_CONCIERTOS`, `TICKETMASTER_API_KEY`, `LASTFM_API_KEY`, `COUNTRY_CITY_API_KEY` (optional).

Production uses two systemd services: `bot_conciertos.service` and `bot_notifications.service`.

## Architecture

Two independent processes:

1. **`telegram_bot.py`** — Main entry point. Registers all Telegram command handlers and runs the async polling loop (up to 256 concurrent updates).

2. **`notifications.py`** — Weekly scheduler. Every minute checks if any user's configured day+time matches. When it does, searches Ticketmaster for their artists (per country), fetches Muspy releases (next 90 days), and sends one summary message. Tracks last-notified week per user to avoid duplicates within the same week.

### Key Modules

| Module | Role |
|--------|------|
| `database.py` | SQLite via `ArtistTrackerDatabase`, wrapped in `DatabaseConcurrentWrapper` for thread safety. Holds users, artists, concerts, notifications_sent, muspy creds, Radicale config. |
| `concert_search.py` | Async Ticketmaster searches per country, deduplication, DB persistence. |
| `user_services.py` | Singleton initializers for Ticketmaster, Last.fm, and country services. `UserServices` class for per-user config. |
| `apis/ticketmaster.py` | Concert search with 24h file-based JSON cache in `./cache/ticketmaster/`. |
| `apis/muspy_service.py` | Muspy REST API client (artist list, releases, sync). Basic HTTP auth. |
| `apis/radicale.py` | CalDAV client for Radicale. `RadicaleClient` pushes ICS events via HTTP PUT. `push_events_bulk()` for batch upload. |
| `apis/lastfm.py` | Last.fm API — used only for artist metadata enrichment (genres, listeners), **not** for importing artists. |
| `apis/mb_artist_info.py` | MusicBrainz artist lookup and metadata (MBID, formed year, etc.). 30-day persistent JSON cache. |
| `handlers/muspy_handlers.py` | Muspy login ConversationHandler (3-step: email → password → userid) and Muspy panel callbacks. |
| `handlers/calendar_handlers.py` | `/cal` command: generates ICS files for concerts and releases, and pushes events to Radicale via `RadicaleClient`. |

### User Flow

```
/adduser          → register
/addartist name   → MusicBrainz lookup → save artist with MBID
/muspy            → connect Muspy account → import/export artists
/country          → configure countries for concert filtering
/notify HH:MM     → set weekly notification time
/notify day N     → set notification day (0=Mon … 6=Sun)
/radicale         → configure Radicale CalDAV server (URL, user, pass, calendar)
/cal              → download ICS or push to Radicale
```

### Weekly Notification Logic (`notifications.py`)

- Runs as a loop sleeping ~60s between iterations.
- On match of `(weekday, HH:MM)` for a user → `process_user()`:
  1. Fetch followed artists from DB.
  2. Search Ticketmaster per artist × per configured country.
  3. Fetch Muspy releases for next 90 days (if Muspy configured).
  4. Send one Markdown summary message.
  5. Record `_last_notified_week[user_id]` to skip the rest of the week.

### Database Schema (key tables)

- `users`: id, chat_id, notification_time, notification_day (0-6), notification_enabled, country_filter, muspy_email/password/userid, radicale_url/username/password/calendar
- `artists`: id, name, mbid, country, formed_year, ...
- `user_followed_artists`: user_id, artist_id, muspy (bool)
- `concerts`: artist_name, venue, city, country, date, time, url, source, concert_hash (MD5 dedup key)
- `muspy_artists_cache`: per-user Muspy artist cache

### Radicale Integration

`apis/radicale.py` → `RadicaleClient` uses HTTP Basic Auth + `PROPFIND`/`PUT` (WebDAV).
`test_connection()` sends a `PROPFIND` to the calendar URL.
`push_events_bulk(events, event_type)` generates individual VCALENDAR/VEVENT ICS strings and PUTs each one.
Calendar URL format: `{base_url}/{username}/{calendar}/`.

### Caching

File-based JSON cache in `./cache/`:
- `ticketmaster/`: per-artist per-country, 24h TTL
- `lastfm/`: per-username per-period, 24h TTL
- MusicBrainz: single persistent JSON file, 30-day TTL

## `email_concerts.py` — conciertos por correo (Bandsintown/Dice.fm/RA)

Tercer proceso independiente (además de `telegram_bot.py`/`notifications.py`),
pensado para ejecutarse periódicamente vía Ofelia. Revisa el `INBOX` del
mismo buzón de disroot que usa `bandcamp_imap`/`discogs_wantlist_notifier`
en busca de correos de notificación de conciertos de artistas seguidos, y
por cada uno nuevo (`UNSEEN`):

1. Envía un aviso por Telegram vía `admin_notify.notify()` (el bot admin ya
   configurado en este mismo `.env` — no el bot de usuarios del propio
   `telegram_bot.py`).
2. Si el evento tiene recinto físico (no es un livestream/anuncio), lo sube
   al calendario "conciertos" de Radicale con `RadicaleClient.push_events_bulk()`
   — el mismo mecanismo que usa `/cal` para Ticketmaster, pero con
   credenciales propias en `.env` (`RADICALE_URL`/`RADICALE_USERNAME`/
   `RADICALE_PASSWORD`/`RADICALE_CALENDAR`), no las de la tabla `users`
   (que a día de escribir esto no tiene ningún usuario con Radicale
   configurado vía `/radicale`).
3. Marca el correo como leído — única deduplicación, igual que
   `discogs_wantlist_notifier`.

**Solo Bandsintown está implementado** (`apis/bandsintown_email_parser.py`).
Dice.fm y Resident Advisor no están enganchados: a fecha de creación de
este script, el buzón no tiene ningún correo de notificación de conciertos
de ninguno de los dos (0 de Dice.fm, y el único de RA es un aviso de login,
no de concierto) — sin una muestra real de HTML no hay nada que parsear.
Cuando lleguen correos reales de esas fuentes, añadir su parser a
`SOURCE_PARSERS` en `email_concerts.py` siguiendo el mismo patrón que
Bandsintown (devolver un dict con `artist`, `raw_date`, `raw_time`, `venue`,
`city`, `country`, `is_livestream`, `url`, `source`).

Variables de entorno adicionales (mismo `.env` del bot):
```
IMAP_SERVER=...
IMAP_PORT=993
IMAP_EMAIL=...
IMAP_PASSWORD=...
RADICALE_URL=...
RADICALE_USERNAME=...
RADICALE_PASSWORD=...
RADICALE_CALENDAR=...   # UUID del calendario "conciertos", no el nombre
```
Si `RADICALE_PASSWORD` (o cualquiera de las otras tres) no está configurada,
el push a Radicale se omite con un aviso en el log, pero el correo se sigue
marcando como leído y el aviso de Telegram se envía igual — el fallo de
Radicale nunca bloquea la notificación.

```bash
python email_concerts.py --dry-run   # imprime por stdout, no envía/marca nada
```
