# tumtumpá

Bot de Telegram para seguir conciertos y lanzamientos de álbumes de tus artistas favoritos.

## Funcionalidades

- **Conciertos**: busca próximos conciertos via Ticketmaster, filtrando por país
- **Lanzamientos**: sincroniza con Muspy para recibir alertas de nuevos álbumes
- **Metadatos de artistas**: enriquece con Last.fm (géneros, oyentes) y MusicBrainz (MBID, año de formación)
- **Calendario**: exporta eventos como archivo `.ics` o los empuja directamente a un servidor CalDAV (Radicale) o a Google Calendar
- **Notificaciones semanales**: envía un resumen semanal configurable por día y hora
- **Búsqueda en YouTube**: enlaza vídeos de artistas

## Requisitos

- Python 3.10+
- Una cuenta de Telegram y un bot creado con [@BotFather](https://t.me/BotFather)
- API key de [Ticketmaster](https://developer.ticketmaster.com/)
- API key de [Last.fm](https://www.last.fm/api)
- *(Opcional)* Cuenta en [Muspy](https://muspy.com/) para lanzamientos
- *(Opcional)* Servidor [Radicale](https://radicale.org/) para CalDAV
- *(Opcional)* Credenciales OAuth2 de Google para Google Calendar

## Instalación

```bash
git clone <repo>
cd bot_conciertos
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Crea un archivo `.env` en la raíz del proyecto:

```env
TELEGRAM_BOT_CONCIERTOS=<token del bot>
TICKETMASTER_API_KEY=<api key>
LASTFM_API_KEY=<api key>

# Opcional — API de países/ciudades
COUNTRY_CITY_API_KEY=<api key>

# Opcional — Google Calendar OAuth2
GOOGLE_CLIENT_ID=<client id>
GOOGLE_CLIENT_SECRET=<client secret>
```

## Ejecución

El proyecto corre como **dos procesos independientes**:

```bash
# Proceso principal del bot (long-polling)
python telegram_bot.py

# Scheduler de notificaciones semanales (proceso separado)
python notifications.py
```

### Producción con systemd

El repositorio incluye los archivos de servicio `bot_conciertos.service` y `bot_notifications.service`. Cópialos a `/etc/systemd/system/` y ajusta las rutas si es necesario:

```bash
sudo cp bot_conciertos.service bot_notifications.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bot_conciertos bot_notifications
```

## Comandos del bot

| Comando | Descripción |
|---------|-------------|
| `/adduser` | Registrar usuario |
| `/addartist <nombre>` | Añadir artista (busca MBID en MusicBrainz) |
| `/listartists` | Ver artistas seguidos |
| `/country` | Configurar países para filtrar conciertos |
| `/notify HH:MM` | Establecer hora de notificación semanal |
| `/notify day N` | Establecer día (0=Lun … 6=Dom) |
| `/muspy` | Conectar cuenta Muspy (importar/exportar artistas y lanzamientos) |
| `/cal` | Descargar `.ics` o empujar eventos al calendario |
| `/radicale` | Configurar servidor CalDAV Radicale |

## Estructura del proyecto

```
telegram_bot.py       # Punto de entrada; registra handlers y arranca el polling
notifications.py      # Scheduler de notificaciones semanales
database.py           # SQLite con ArtistTrackerDatabase + wrapper thread-safe
concert_search.py     # Búsqueda en Ticketmaster y deduplicación
user_services.py      # Inicialización de servicios por usuario
config.py             # Constantes y configuración global
apis/
  ticketmaster.py     # Cliente Ticketmaster con caché JSON de 24h
  muspy_service.py    # Cliente REST de Muspy
  radicale.py         # Cliente CalDAV (WebDAV PUT/PROPFIND)
  google_calendar.py  # Integración Google Calendar via OAuth2
  lastfm.py           # Metadatos de artistas (géneros, oyentes)
  mb_artist_info.py   # Lookup MusicBrainz con caché de 30 días
  youtube_search.py   # Búsqueda de vídeos en YouTube
handlers/
  artist_handlers.py  # Handlers de /addartist, /listartists, etc.
  calendar_handlers.py# Handler de /cal (ICS + Radicale + Google Cal)
  muspy_handlers.py   # ConversationHandler para login de Muspy
```

## Caché

Los datos se cachean en `./cache/` para evitar llamadas innecesarias a las APIs:

- `cache/ticketmaster/` — por artista y país, TTL de 24 horas
- `cache/lastfm/` — por usuario y período, TTL de 24 horas
- MusicBrainz — archivo JSON único, TTL de 30 días
