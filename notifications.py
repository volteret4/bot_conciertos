#!/usr/bin/env python3
"""
Servicio de notificaciones semanales para tumtumpá.

Lógica:
  - Una vez a la semana, para cada usuario con notificaciones habilitadas,
    busca en Ticketmaster los conciertos de sus artistas (filtrados por país)
    y obtiene los próximos discos de Muspy.
  - Envía un único mensaje resumen semanal a cada usuario.
  - El día y la hora de notificación son configurables por usuario.
    Por defecto: lunes a las 09:00.

El proceso principal ejecuta un bucle que:
  1. Cada minuto comprueba si es la hora de algún usuario.
  2. Si coincide día+hora, genera y envía el resumen semanal.
  3. Rastrea el último envío por usuario para no repetir en la misma semana.
"""

import os
import asyncio
import logging
import hashlib
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Set, Tuple

import requests

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

DAYS_ES = ['lunes', 'martes', 'miércoles', 'jueves', 'viernes', 'sábado', 'domingo']


class WeeklyNotificationService:
    """Servicio de notificaciones semanales."""

    def __init__(self, db_path: str, telegram_token: str):
        self.db_path = db_path
        self.telegram_api_url = f"https://api.telegram.org/bot{telegram_token}"

        self.ticketmaster = None
        self.muspy_service = None

        # Rastrea última semana procesada por usuario
        self._last_notified_week: Dict[int, str] = {}
        self._last_searched_week: Dict[int, str] = {}

        self._init_services()

    def _init_services(self):
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CACHE_DIR = os.path.join(BASE_DIR, "cache")
        os.makedirs(CACHE_DIR, exist_ok=True)

        tm_key = os.environ.get("TICKETMASTER_API_KEY")
        if tm_key:
            try:
                from apis.ticketmaster import TicketmasterService
                self.ticketmaster = TicketmasterService(
                    api_key=tm_key,
                    cache_dir=os.path.join(CACHE_DIR, "ticketmaster")
                )
                logger.info("✅ Ticketmaster inicializado")
            except Exception as e:
                logger.error(f"Error inicializando Ticketmaster: {e}")
        else:
            logger.warning("⚠️ TICKETMASTER_API_KEY no configurada")

        try:
            from apis.muspy_service import MuspyService
            self.muspy_service = MuspyService()
            logger.info("✅ MuspyService inicializado")
        except Exception as e:
            logger.error(f"Error inicializando MuspyService: {e}")

    # ─── Base de datos ────────────────────────────────────────────────────────

    def _get_db(self):
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_users_for_time(self, day: int, time_str: str) -> List[Dict]:
        """Usuarios con notificaciones habilitadas para este día y hora."""
        conn = self._get_db()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT * FROM users
                WHERE notification_enabled = 1
                  AND notification_day = ?
                  AND notification_time = ?
            """, (day, time_str))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error obteniendo usuarios para notificación: {e}")
            return []
        finally:
            conn.close()

    def get_followed_artists(self, user_id: int) -> List[Dict]:
        """Artistas seguidos por el usuario."""
        conn = self._get_db()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT a.id, a.name, a.mbid
                FROM artists a
                JOIN user_followed_artists ufa ON ufa.artist_id = a.id
                WHERE ufa.user_id = ?
                ORDER BY a.name
            """, (user_id,))
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"Error obteniendo artistas de usuario {user_id}: {e}")
            return []
        finally:
            conn.close()

    def get_user_countries(self, user: Dict) -> Set[str]:
        """Países configurados para el usuario."""
        try:
            from apis.country_state_city import CountryCityService
            ccs_key = os.environ.get("COUNTRY_CITY_API_KEY")
            if ccs_key:
                ccs = CountryCityService(api_key=ccs_key, db_path=self.db_path)
                codes = ccs.get_user_country_codes(user['id'])
                if codes:
                    return codes
        except Exception:
            pass
        cf = user.get('country_filter', 'ES') or 'ES'
        return {cf}

    def get_muspy_credentials(self, user_id: int) -> Optional[Tuple[str, str, str]]:
        """Devuelve (email, password, userid) de Muspy o None."""
        conn = self._get_db()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT muspy_email, muspy_password, muspy_userid FROM users WHERE id = ?",
                (user_id,)
            )
            row = cur.fetchone()
            if row and row[0]:
                return row[0], row[1], row[2]
            return None
        except Exception as e:
            logger.error(f"Error obteniendo credenciales Muspy: {e}")
            return None
        finally:
            conn.close()

    def save_concert(self, concert: Dict) -> Optional[int]:
        """Guarda un concierto en la BD, devuelve su ID."""
        concert_hash = _make_hash(concert)
        conn = self._get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM concerts WHERE concert_hash = ?", (concert_hash,))
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute("""
                INSERT INTO concerts
                  (artist_name, concert_name, venue, city, country, date, time, url, source, concert_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                concert.get('artist_name', ''),
                concert.get('concert_name', concert.get('name', '')),
                concert.get('venue', ''),
                concert.get('city', ''),
                concert.get('country', ''),
                concert.get('date', ''),
                concert.get('time', ''),
                concert.get('url', ''),
                concert.get('source', 'ticketmaster'),
                concert_hash,
            ))
            conn.commit()
            return cur.lastrowid
        except Exception as e:
            logger.error(f"Error guardando concierto: {e}")
            return None
        finally:
            conn.close()

    # ─── Búsqueda de conciertos ───────────────────────────────────────────────

    def search_concerts_for_artist(self, artist_name: str, country_codes: Set[str]) -> List[Dict]:
        """Busca en Ticketmaster por artista y países del usuario."""
        if not self.ticketmaster:
            return []

        all_concerts = []
        seen: Set[Tuple] = set()

        for code in country_codes:
            try:
                concerts, _ = self.ticketmaster.search_concerts(artist_name, code)
                for c in concerts:
                    key = (c.get('venue', ''), c.get('date', ''), c.get('city', ''))
                    if key not in seen:
                        seen.add(key)
                        all_concerts.append(c)
            except Exception as e:
                logger.error(f"Error buscando {artist_name} en {code}: {e}")

        return all_concerts

    # ─── Formateo de mensajes ─────────────────────────────────────────────────

    def format_weekly_summary(
        self,
        user: Dict,
        concerts_by_artist: Dict[str, List[Dict]],
        releases: List[Dict],
    ) -> str:
        """Genera el mensaje resumen semanal."""
        today = date.today()
        week_num = today.isocalendar()[1]
        notification_time = user.get('notification_time', '09:00')
        notification_day = user.get('notification_day', 0)
        day_name = DAYS_ES[notification_day]

        lines = [f"🎵 *Resumen semanal* — semana {week_num}\n"]

        # ── Conciertos ────────────────────────────────────────────────────────
        total_concerts = sum(len(v) for v in concerts_by_artist.values())
        if total_concerts:
            lines.append("🎤 *Conciertos próximos:*")
            for artist, cons in sorted(concerts_by_artist.items()):
                if not cons:
                    continue
                lines.append(f"\n*{_esc(artist)}*")
                for c in cons:
                    date_str = c.get('date', '')
                    venue = c.get('venue', '')
                    city = c.get('city', '')
                    url = c.get('url', '')
                    try:
                        d = datetime.strptime(date_str[:10], '%Y-%m-%d')
                        formatted_date = d.strftime('%d/%m/%Y')
                    except (ValueError, IndexError):
                        formatted_date = date_str
                    location_parts = [p for p in [_esc(venue), _esc(city)] if p]
                    location = ', '.join(location_parts) or 'Lugar por confirmar'
                    if url and url.startswith('http'):
                        lines.append(f"  • {formatted_date}: [{location}]({url.replace(')', '\\)')})")
                    else:
                        lines.append(f"  • {formatted_date}: {location}")
        else:
            lines.append("🎤 *Conciertos:* No hay conciertos en tus países esta semana.")

        # ── Discos ────────────────────────────────────────────────────────────
        if releases:
            lines.append("\n💿 *Próximos lanzamientos:*")
            for rel in releases:
                if not self.muspy_service:
                    continue
                artist_name = self.muspy_service.extract_artist_name(rel)
                title = self.muspy_service.extract_title(rel)
                rel_type = self.muspy_service.extract_release_type(rel)
                rel_date = rel.get('date', '')
                try:
                    d = datetime.strptime(rel_date[:10], '%Y-%m-%d')
                    formatted_date = d.strftime('%d/%m/%Y')
                except (ValueError, IndexError):
                    formatted_date = rel_date
                type_str = f" [{rel_type}]" if rel_type else ""
                lines.append(
                    f"  • {formatted_date}: *{_esc(artist_name)}* — {_esc(title)}{_esc(type_str)}"
                )
        else:
            lines.append("\n💿 *Lanzamientos:* Sin lanzamientos próximos en Muspy.")

        lines.append(
            f"\n_Próxima actualización: el {day_name} que viene a las {notification_time}_"
        )
        return "\n".join(lines)

    # ─── Envío Telegram ───────────────────────────────────────────────────────

    async def send_message(self, chat_id: int, text: str) -> bool:
        try:
            resp = requests.post(
                f"{self.telegram_api_url}/sendMessage",
                json={
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': 'Markdown',
                    'disable_web_page_preview': True,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                return True
            logger.error(f"Error enviando mensaje a {chat_id}: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"Excepción enviando mensaje: {e}")
            return False

    # ─── Búsqueda de datos (fase 1, 2h antes) ────────────────────────────────

    async def search_for_user(self, user: Dict):
        """
        Fase 1: busca conciertos en Ticketmaster y guarda en BD.
        Se ejecuta 2 horas antes de la hora de notificación.
        """
        user_id = user['id']
        artists = self.get_followed_artists(user_id)
        if not artists:
            return

        countries = self.get_user_countries(user)
        logger.info(f"[Búsqueda] Usuario {user_id}: {len(artists)} artistas, países: {countries}")

        for artist in artists:
            concerts = self.search_concerts_for_artist(artist['name'], countries)
            for c in concerts:
                self.save_concert(c)
            await asyncio.sleep(0.5)

        logger.info(f"[Búsqueda] Completada para usuario {user_id}")

    # ─── Notificación (fase 2, a la hora configurada) ─────────────────────────

    async def process_user(self, user: Dict):
        """
        Fase 2: lee los conciertos ya guardados en BD, obtiene discos de Muspy
        y envía el resumen semanal al usuario.
        """
        user_id = user['id']
        chat_id = user['chat_id']

        artists = self.get_followed_artists(user_id)
        if not artists:
            logger.info(f"Usuario {user_id} sin artistas seguidos, omitiendo")
            return

        countries = self.get_user_countries(user)

        # Leer conciertos de la BD (ya buscados en la fase 1)
        concerts_by_artist: Dict[str, List[Dict]] = {}
        artist_names = {a['name'] for a in artists}
        conn = self._get_db()
        try:
            today_str = date.today().isoformat()
            cur = conn.cursor()
            cur.execute("""
                SELECT artist_name, concert_name, venue, city, country, date, time, url, source
                FROM concerts
                WHERE date >= ? AND artist_name IN ({})
                ORDER BY date
            """.format(','.join('?' * len(artist_names))),
            [today_str] + list(artist_names))

            for row in cur.fetchall():
                r = dict(row)
                # Filtrar por países del usuario
                if countries and r.get('country', '').upper() not in {c.upper() for c in countries}:
                    continue
                concerts_by_artist.setdefault(r['artist_name'], []).append(r)
        except Exception as e:
            logger.error(f"Error leyendo conciertos de BD para usuario {user_id}: {e}")
        finally:
            conn.close()

        # Discos de Muspy (próximos 90 días)
        releases: List[Dict] = []
        muspy_creds = self.get_muspy_credentials(user_id)
        if muspy_creds and self.muspy_service:
            email, password, userid = muspy_creds
            try:
                future_releases, _ = self.muspy_service.get_user_releases(email, password, userid)
                today = date.today()
                cutoff = today + timedelta(days=90)
                for rel in future_releases:
                    rel_date_str = rel.get('date', '')
                    try:
                        rel_date = datetime.strptime(rel_date_str[:10], '%Y-%m-%d').date()
                        if today <= rel_date <= cutoff:
                            releases.append(rel)
                    except (ValueError, IndexError):
                        pass
                releases.sort(key=lambda r: r.get('date', ''))
            except Exception as e:
                logger.error(f"Error obteniendo lanzamientos Muspy para usuario {user_id}: {e}")

        if not concerts_by_artist and not releases:
            logger.info(f"Sin novedades para usuario {user_id}, no se envía mensaje")
            return

        message = self.format_weekly_summary(user, concerts_by_artist, releases)

        if len(message) > 4000:
            for chunk in _split_message(message):
                await self.send_message(chat_id, chunk)
                await asyncio.sleep(0.3)
        else:
            await self.send_message(chat_id, message)

        logger.info(f"Resumen semanal enviado a usuario {user_id}")

    # ─── Bucle principal ──────────────────────────────────────────────────────

    async def run(self):
        """
        Bucle principal. Cada minuto:
        - Comprueba si toca la BÚSQUEDA (hora_notificación - 2h) → fase 1
        - Comprueba si toca la NOTIFICACIÓN → fase 2
        Solo actúa con usuarios que tienen notificaciones habilitadas.
        No repite ninguna fase más de una vez por semana por usuario.
        """
        logger.info("🚀 Servicio de notificaciones semanales iniciado")

        while True:
            now = datetime.now()
            current_day = now.weekday()   # 0=lunes … 6=domingo
            current_time = now.strftime('%H:%M')
            current_week = now.strftime('%Y-W%W')

            # ── Fase 1: búsqueda 2 horas antes ──────────────────────────────
            search_users = self._users_for_search_phase(current_day, current_time)
            for user in search_users:
                uid = user['id']
                if self._last_searched_week.get(uid) == current_week:
                    continue
                try:
                    await self.search_for_user(user)
                    self._last_searched_week[uid] = current_week
                except Exception as e:
                    logger.error(f"Error en búsqueda para usuario {uid}: {e}")

            # ── Fase 2: notificación a la hora configurada ───────────────────
            notify_users = self.get_users_for_time(current_day, current_time)
            for user in notify_users:
                uid = user['id']
                if self._last_notified_week.get(uid) == current_week:
                    continue
                try:
                    await self.process_user(user)
                    self._last_notified_week[uid] = current_week
                except Exception as e:
                    logger.error(f"Error procesando usuario {uid}: {e}")

            await asyncio.sleep(60 - datetime.now().second)

    def _users_for_search_phase(self, current_day: int, current_time: str) -> List[Dict]:
        """
        Devuelve los usuarios cuya hora de notificación menos 2 horas coincide
        con current_day y current_time.
        """
        try:
            search_dt = datetime.strptime(current_time, '%H:%M') + timedelta(hours=2)
            # Si pasar las 24h, el día de búsqueda es el anterior al de notificación
            notif_day = current_day
            notif_time = search_dt.strftime('%H:%M')
            # Si la suma supera el día, ajustar
            if search_dt.day != datetime.strptime(current_time, '%H:%M').day:
                notif_day = (current_day + 1) % 7
        except Exception:
            return []

        return self.get_users_for_time(notif_day, notif_time)


# ─── Utilidades ──────────────────────────────────────────────────────────────

def _make_hash(concert: Dict) -> str:
    raw = f"{concert.get('artist_name','')}-{concert.get('venue','')}-{concert.get('date','')}"
    return hashlib.md5(raw.encode()).hexdigest()


def _esc(text: str) -> str:
    return str(text).replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')


def _split_message(message: str, max_length: int = 4000) -> List[str]:
    chunks, current, current_len = [], [], 0
    for line in message.split('\n'):
        line_len = len(line) + 1
        if current_len + line_len > max_length and current:
            chunks.append('\n'.join(current))
            current, current_len = [line], line_len
        else:
            current.append(line)
            current_len += line_len
    if current:
        chunks.append('\n'.join(current))
    return chunks


# ─── Entrypoint ───────────────────────────────────────────────────────────────

def main():
    from dotenv import load_dotenv
    load_dotenv()

    db_path = os.environ.get("DB_PATH", "artist_tracker.db")
    token = os.environ.get("TELEGRAM_BOT_TOKEN")

    if not token:
        logger.error("TELEGRAM_BOT_TOKEN no configurado")
        return

    service = WeeklyNotificationService(db_path=db_path, telegram_token=token)
    asyncio.run(service.run())


if __name__ == "__main__":
    main()
