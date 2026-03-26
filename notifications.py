#!/usr/bin/env python3
"""
Script mejorado para el sistema de notificaciones del bot de artistas
- 08:00: Búsqueda global de TODOS los artistas
- Cada minuto: Notificaciones filtradas por países del usuario
"""

import os
import sys
import asyncio
import sqlite3
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Set
import requests
import json

# Añadir el directorio principal al path para importar los módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Importar servicios de búsqueda de conciertos
try:
    from apis.ticketmaster import TicketmasterService
    from apis.spotify import SpotifyService
    from apis.setlistfm import SetlistfmService
    from apis.country_state_city import CountryCityService, ArtistTrackerDatabaseExtended
except ImportError as e:
    print(f"Error importando servicios: {e}")
    sys.exit(1)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('notifications.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class NotificationService:
    """Servicio mejorado para manejar notificaciones"""

    def __init__(self, db_path: str, telegram_token: str):
        self.db_path = db_path
        self.telegram_token = telegram_token
        self.telegram_api_url = f"https://api.telegram.org/bot{telegram_token}"

        # Inicializar servicios
        self.init_concert_services()
        self.init_country_service()

        # Control de búsqueda diaria
        self.last_global_search = None

    def init_concert_services(self):
        """Inicializa los servicios de búsqueda de conciertos"""
        BASE_DIR = Path(__file__).resolve().parent
        CACHE_DIR = BASE_DIR / "cache"
        CACHE_DIR.mkdir(exist_ok=True)

        # Variables de entorno
        TICKETMASTER_API_KEY = os.environ.get("TICKETMASTER_API_KEY")
        SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID")
        SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET")
        SPOTIFY_REDIRECT_URI = os.environ.get("SPOTIFY_REDIRECT_URI", "http://localhost:8888/callback")
        SETLISTFM_API_KEY = os.environ.get("SETLISTFM_API_KEY")

        self.services = {}

        try:
            if TICKETMASTER_API_KEY:
                self.services['ticketmaster'] = TicketmasterService(
                    api_key=TICKETMASTER_API_KEY,
                    cache_dir=CACHE_DIR / "ticketmaster"
                )
                logger.info("✅ Ticketmaster service inicializado")

            if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
                self.services['spotify'] = SpotifyService(
                    client_id=SPOTIFY_CLIENT_ID,
                    client_secret=SPOTIFY_CLIENT_SECRET,
                    redirect_uri=SPOTIFY_REDIRECT_URI,
                    cache_dir=CACHE_DIR / "spotify"
                )
                logger.info("✅ Spotify service inicializado")

            if SETLISTFM_API_KEY:
                self.services['setlistfm'] = SetlistfmService(
                    api_key=SETLISTFM_API_KEY,
                    cache_dir=CACHE_DIR / "setlistfm",
                    db_path=None
                )
                logger.info("✅ Setlist.fm service inicializado")

        except Exception as e:
            logger.error(f"Error inicializando servicios: {e}")

    def init_country_service(self):
        """Inicializa el servicio de países"""
        COUNTRY_API_KEY = os.environ.get("COUNTRY_CITY_API_KEY")

        if COUNTRY_API_KEY:
            try:
                self.country_city_service = CountryCityService(
                    api_key=COUNTRY_API_KEY,
                    db_path=self.db_path
                )
                logger.info("✅ Servicio de países inicializado")
            except Exception as e:
                logger.error(f"Error inicializando servicio de países: {e}")
                self.country_city_service = None
        else:
            logger.warning("⚠️ COUNTRY_CITY_API_KEY no configurada")
            self.country_city_service = None

    def get_db_connection(self):
        """Obtiene conexión a la base de datos"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    async def search_concerts_for_artist_global(self, artist_name: str) -> List[Dict]:
        """
        Busca conciertos para un artista GLOBALMENTE (todos los países)
        Usa la misma lógica que el bot pero sin filtrar por usuario
        """
        all_concerts = []

        # Buscar en Ticketmaster GLOBALMENTE
        if 'ticketmaster' in self.services:
            try:
                # Usar búsqueda global si está disponible, sino usar sin país específico
                if hasattr(self.services['ticketmaster'], 'search_concerts_global'):
                    concerts, _ = self.services['ticketmaster'].search_concerts_global(artist_name)
                else:
                    # Fallback: buscar sin país específico
                    concerts, _ = self.services['ticketmaster'].search_concerts(artist_name, size=200)

                # Asegurar que tengan fuente
                for concert in concerts:
                    if not concert.get('source'):
                        concert['source'] = 'Ticketmaster'

                all_concerts.extend(concerts)
                logger.info(f"Ticketmaster global: {len(concerts)} conciertos para {artist_name}")
            except Exception as e:
                logger.error(f"Error buscando en Ticketmaster: {e}")

        # Buscar en Spotify
        if 'spotify' in self.services:
            try:
                concerts, _ = self.services['spotify'].search_artist_and_concerts(artist_name)

                # Asegurar que tengan fuente
                for concert in concerts:
                    if not concert.get('source'):
                        concert['source'] = 'Spotify'

                all_concerts.extend(concerts)
                logger.info(f"Spotify: {len(concerts)} conciertos para {artist_name}")
            except Exception as e:
                logger.error(f"Error buscando en Spotify: {e}")

        # Buscar en Setlist.fm (países principales para no sobrecargar)
        if 'setlistfm' in self.services:
            try:
                main_countries = ['ES', 'US', 'FR', 'DE', 'IT', 'GB', 'AR', 'MX', 'BR', 'CA']
                for country_code in main_countries:
                    concerts, _ = self.services['setlistfm'].search_concerts(artist_name, country_code)

                    # Asegurar que tengan fuente
                    for concert in concerts:
                        if not concert.get('source'):
                            concert['source'] = 'Setlist.fm'

                    all_concerts.extend(concerts)

                logger.info(f"Setlist.fm: {len([c for c in all_concerts if c.get('source') == 'Setlist.fm'])} conciertos para {artist_name}")
            except Exception as e:
                logger.error(f"Error buscando en Setlist.fm: {e}")

        return all_concerts

    def create_concert_hash(self, concert_data: Dict) -> str:
        """Crea un hash único para un concierto"""
        import hashlib
        key_data = f"{concert_data.get('artist', '')}-{concert_data.get('venue', '')}-{concert_data.get('date', '')}-{concert_data.get('source', '')}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def save_concert(self, concert_data: Dict) -> int:
        """Guarda un concierto en la base de datos"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            # Crear hash único para el concierto
            concert_hash = self.create_concert_hash(concert_data)

            # Verificar si ya existe
            cursor.execute("SELECT id FROM concerts WHERE concert_hash = ?", (concert_hash,))
            existing = cursor.fetchone()
            if existing:
                return existing[0]

            # Insertar nuevo concierto
            cursor.execute("""
                INSERT INTO concerts (
                    artist_name, concert_name, venue, city, country,
                    date, time, url, source, concert_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                concert_data.get('artist', ''),
                concert_data.get('name', ''),
                concert_data.get('venue', ''),
                concert_data.get('city', ''),
                concert_data.get('country', ''),
                concert_data.get('date', ''),
                concert_data.get('time', ''),
                concert_data.get('url', ''),
                concert_data.get('source', ''),
                concert_hash
            ))

            concert_id = cursor.lastrowid
            conn.commit()
            return concert_id

        except sqlite3.Error as e:
            logger.error(f"Error al guardar concierto: {e}")
            return None
        finally:
            conn.close()

    def get_all_artists(self) -> List[str]:
        """Obtiene TODOS los artistas únicos de la base de datos"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT DISTINCT name FROM artists ORDER BY name")
            rows = cursor.fetchall()
            return [row[0] for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo todos los artistas: {e}")
            return []
        finally:
            conn.close()

    async def perform_daily_global_search(self):
        """
        Realiza búsqueda global diaria de TODOS los artistas
        Se ejecuta a las 08:00
        """
        logger.info("🌍 INICIANDO BÚSQUEDA GLOBAL DIARIA DE CONCIERTOS")

        # Obtener todos los artistas únicos
        all_artists = self.get_all_artists()

        if not all_artists:
            logger.warning("⚠️ No hay artistas en la base de datos")
            return

        logger.info(f"📋 Buscando conciertos para {len(all_artists)} artistas únicos")

        total_new_concerts = 0
        total_processed = 0

        for artist_name in all_artists:
            try:
                logger.info(f"🔍 [{total_processed + 1}/{len(all_artists)}] Buscando: {artist_name}")

                # Buscar conciertos globalmente
                concerts = await self.search_concerts_for_artist_global(artist_name)

                # Guardar TODOS los conciertos encontrados
                artist_new_concerts = 0
                for concert in concerts:
                    # Asegurar que el nombre del artista sea consistente
                    concert['artist'] = artist_name

                    concert_id = self.save_concert(concert)
                    if concert_id:
                        artist_new_concerts += 1
                        total_new_concerts += 1

                if artist_new_concerts > 0:
                    logger.info(f"✅ {artist_name}: {artist_new_concerts} nuevos conciertos de {len(concerts)} encontrados")
                else:
                    logger.info(f"ℹ️ {artist_name}: 0 nuevos conciertos (ya existían)")

                total_processed += 1

                # Pausa para no sobrecargar las APIs
                await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"❌ Error procesando {artist_name}: {e}")
                total_processed += 1
                continue

        logger.info(f"🎉 BÚSQUEDA GLOBAL COMPLETADA:")
        logger.info(f"   📊 Artistas procesados: {total_processed}/{len(all_artists)}")
        logger.info(f"   🆕 Nuevos conciertos guardados: {total_new_concerts}")

        # Marcar que se realizó la búsqueda hoy
        self.last_global_search = datetime.now().date()

    def get_user_services(self, user_id: int) -> Dict[str, any]:
        """Obtiene la configuración de servicios para un usuario (VERSIÓN EXTENDIDA)"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT service_ticketmaster, service_spotify, service_setlistfm, country_filter
                FROM users WHERE id = ?
            """, (user_id,))

            row = cursor.fetchone()
            services = {
                'ticketmaster': bool(row[0]) if row else True,
                'spotify': bool(row[1]) if row else True,
                'setlistfm': bool(row[2]) if row else True,
                'country_filter': row[3] if row else 'ES'
            }

            # Añadir información de países múltiples
            if self.country_city_service:
                user_countries = self.country_city_service.get_user_country_codes(user_id)
                services['countries'] = user_countries

                # Mantener compatibilidad con country_filter
                if user_countries:
                    services['country_filter'] = list(user_countries)[0]
                elif not services['country_filter']:
                    services['country_filter'] = 'ES'
            else:
                # Solo country_filter legacy
                services['countries'] = {services['country_filter']}

            return services

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo configuración de servicios: {e}")
            return {
                'ticketmaster': True,
                'spotify': True,
                'setlistfm': True,
                'country_filter': 'ES',
                'countries': {'ES'}
            }
        finally:
            conn.close()

    def get_users_for_time(self, notification_time: str) -> List[Dict]:
        """Obtiene usuarios que deben recibir notificación a una hora específica"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM users
                WHERE notification_enabled = 1
                AND notification_time = ?
            """, (notification_time,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo usuarios para hora {notification_time}: {e}")
            return []
        finally:
            conn.close()

    def get_future_concerts_by_artist_for_user(self, user_id: int, user_countries: Set[str] = None) -> Dict[str, List[Dict]]:
        """Obtiene TODOS los conciertos futuros agrupados por artista para un usuario"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            # Obtener todos los conciertos futuros del usuario
            today = datetime.now().strftime('%Y-%m-%d')
            cursor.execute("""
                SELECT DISTINCT c.*
                FROM concerts c
                JOIN artists a ON LOWER(c.artist_name) = LOWER(a.name)
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                WHERE ufa.user_id = ?
                AND (c.date >= ? OR c.date = '' OR c.date IS NULL)
                ORDER BY c.artist_name, c.date ASC
            """, (user_id, today))

            rows = cursor.fetchall()
            all_concerts = [dict(row) for row in rows]

            # Filtrar por países del usuario
            if user_countries and self.country_city_service:
                extended_db = ArtistTrackerDatabaseExtended(self.db_path, self.country_city_service)
                filtered_concerts = extended_db.filter_concerts_by_countries(all_concerts, user_countries)
            else:
                # Filtrado básico por país si no hay servicio de países
                if user_countries:
                    filtered_concerts = []
                    for concert in all_concerts:
                        concert_country = concert.get('country', '').upper()
                        if not concert_country or concert_country in {c.upper() for c in user_countries}:
                            filtered_concerts.append(concert)
                else:
                    filtered_concerts = all_concerts

            # Agrupar por artista
            concerts_by_artist = {}
            for concert in filtered_concerts:
                artist_name = concert.get('artist_name', 'Artista desconocido')
                if artist_name not in concerts_by_artist:
                    concerts_by_artist[artist_name] = []
                concerts_by_artist[artist_name].append(concert)

            return concerts_by_artist

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo conciertos futuros por artista: {e}")
            return {}
        finally:
            conn.close()

    def get_unnotified_concerts_by_artist_for_user(self, user_id: int, user_countries: Set[str] = None) -> Dict[str, List[Dict]]:
        """Obtiene conciertos NO NOTIFICADOS agrupados por artista para un usuario"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            # Obtener conciertos no notificados del usuario
            cursor.execute("""
                SELECT DISTINCT c.*
                FROM concerts c
                JOIN artists a ON LOWER(c.artist_name) = LOWER(a.name)
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                WHERE ufa.user_id = ?
                AND NOT EXISTS (
                    SELECT 1 FROM notifications_sent ns
                    WHERE ns.user_id = ? AND ns.concert_id = c.id
                )
                ORDER BY c.artist_name, c.date ASC
            """, (user_id, user_id))

            rows = cursor.fetchall()
            all_concerts = [dict(row) for row in rows]

            # Filtrar por países del usuario
            if user_countries and self.country_city_service:
                extended_db = ArtistTrackerDatabaseExtended(self.db_path, self.country_city_service)
                filtered_concerts = extended_db.filter_concerts_by_countries(all_concerts, user_countries)
            else:
                # Filtrado básico por país si no hay servicio de países
                if user_countries:
                    filtered_concerts = []
                    for concert in all_concerts:
                        concert_country = concert.get('country', '').upper()
                        if not concert_country or concert_country in {c.upper() for c in user_countries}:
                            filtered_concerts.append(concert)
                else:
                    filtered_concerts = all_concerts

            # Para cada artista con conciertos no notificados, obtener TODOS sus conciertos futuros
            artists_with_new_concerts = {concert['artist_name'] for concert in filtered_concerts}

            # Obtener todos los conciertos futuros de estos artistas
            all_future_concerts = self.get_future_concerts_by_artist_for_user(user_id, user_countries)

            # Filtrar solo los artistas que tienen conciertos nuevos
            concerts_by_artist = {}
            for artist_name in artists_with_new_concerts:
                if artist_name in all_future_concerts:
                    concerts_by_artist[artist_name] = all_future_concerts[artist_name]

            return concerts_by_artist

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo conciertos no notificados por artista: {e}")
            return {}
        finally:
            conn.close()

    def mark_artist_concerts_notified(self, user_id: int, artist_concerts: List[Dict]) -> bool:
        """Marca todos los conciertos de un artista como notificados para un usuario"""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            for concert in artist_concerts:
                cursor.execute("""
                    INSERT OR IGNORE INTO notifications_sent (user_id, concert_id)
                    VALUES (?, ?)
                """, (user_id, concert['id']))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error marcando conciertos del artista como notificados: {e}")
            return False
        finally:
            conn.close()

    def format_artist_concerts_message(self, artist_name: str, concerts: List[Dict], user_countries: Set[str]) -> str:
        """Formatea un mensaje con todos los conciertos futuros de un artista"""
        if not concerts:
            return ""

        # Escapar caracteres especiales para Markdown
        safe_artist = artist_name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        message_lines = [f"🎵 *{safe_artist}*"]
        message_lines.append(f"📍 Conciertos en {', '.join(sorted(user_countries))}\n")

        # Ordenar conciertos por fecha
        concerts_sorted = sorted(concerts, key=lambda x: x.get('date', '9999-12-31'))

        for concert in concerts_sorted:
            venue = concert.get('venue', 'Lugar desconocido')
            city = concert.get('city', '')
            country = concert.get('country', '')
            date = concert.get('date', 'Fecha desconocida')
            url = concert.get('url', '')
            source = concert.get('source', '')

            # Formatear fecha
            if date and len(date) >= 10 and '-' in date:
                try:
                    date_obj = datetime.strptime(date[:10], '%Y-%m-%d')
                    date = date_obj.strftime('%d/%m/%Y')
                except ValueError:
                    pass

            # Escapar caracteres especiales
            safe_venue = str(venue).replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")
            safe_city = str(city).replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

            # Construir ubicación
            location_parts = []
            if safe_venue:
                location_parts.append(safe_venue)
            if safe_city:
                location_parts.append(safe_city)
            if country:
                location_parts.append(f"({country})")

            location = ", ".join(location_parts) if location_parts else "Ubicación desconocida"

            concert_line = f"📅 {date}: "

            if url and url.startswith(('http://', 'https://')):
                # Escapar paréntesis en URL
                escaped_url = url.replace(")", "\\)")
                concert_line += f"[{location}]({escaped_url})"
            else:
                concert_line += location

            if source:
                concert_line += f" _{source}_"

            message_lines.append(concert_line)

        message_lines.append(f"\n📊 Total: {len(concerts)} conciertos")
        message_lines.append(f"💡 Usa /search {artist_name} para más detalles")

        return "\n".join(message_lines)

    async def send_telegram_message(self, chat_id: int, message: str) -> bool:
        """Envía un mensaje de Telegram"""
        try:
            data = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': True
            }

            response = requests.post(
                f"{self.telegram_api_url}/sendMessage",
                data=data,
                timeout=30
            )

            if response.status_code == 200:
                return True
            else:
                logger.error(f"Error enviando mensaje Telegram: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error enviando mensaje Telegram: {e}")
            return False

    async def process_notifications_for_time(self, notification_time: str):
        """Procesa notificaciones para una hora específica - ENVÍA UN MENSAJE POR ARTISTA"""
        logger.info(f"🔔 Procesando notificaciones para las {notification_time}")

        # Obtener usuarios para esta hora
        users = self.get_users_for_time(notification_time)

        if not users:
            logger.info(f"No hay usuarios configurados para las {notification_time}")
            return

        logger.info(f"Encontrados {len(users)} usuarios para notificación a las {notification_time}")

        for user in users:
            try:
                logger.info(f"Procesando notificaciones para {user['username']}")

                # Obtener configuración de servicios del usuario (incluye países)
                user_services = self.get_user_services(user['id'])
                user_countries = user_services.get('countries', {'ES'})

                logger.info(f"Países configurados para {user['username']}: {user_countries}")

                # Obtener conciertos no notificados agrupados por artista
                # Esto incluye TODOS los conciertos futuros de artistas que tienen nuevos conciertos
                artists_concerts = self.get_unnotified_concerts_by_artist_for_user(user['id'], user_countries)

                if artists_concerts:
                    logger.info(f"Artistas con nuevos conciertos para {user['username']}: {list(artists_concerts.keys())}")

                    # Enviar un mensaje por cada artista
                    total_messages_sent = 0
                    for artist_name, concerts in artists_concerts.items():
                        try:
                            # Formatear mensaje para este artista
                            message = self.format_artist_concerts_message(artist_name, concerts, user_countries)

                            if message:
                                # Enviar mensaje del artista
                                if await self.send_telegram_message(user['chat_id'], message):
                                    # Marcar TODOS los conciertos de este artista como notificados
                                    self.mark_artist_concerts_notified(user['id'], concerts)
                                    total_messages_sent += 1
                                    logger.info(f"✅ Mensaje enviado para {artist_name}: {len(concerts)} conciertos")

                                    # Pausa entre mensajes para evitar spam
                                    await asyncio.sleep(1)
                                else:
                                    logger.error(f"❌ Falló el envío del mensaje para {artist_name}")

                        except Exception as e:
                            logger.error(f"❌ Error enviando mensaje para {artist_name}: {e}")

                    logger.info(f"✅ Notificaciones completadas para {user['username']}: {total_messages_sent} mensajes enviados")
                else:
                    logger.info(f"ℹ️ No hay nuevos conciertos para {user['username']} en sus países")

            except Exception as e:
                logger.error(f"❌ Error procesando notificaciones para {user['username']}: {e}")

        logger.info(f"🎉 Notificaciones completadas para las {notification_time}")

    def should_perform_global_search(self) -> bool:
        """Verifica si debe realizar la búsqueda global diaria"""
        current_time = datetime.now()
        current_hour = current_time.hour
        today = current_time.date()

        # Solo a las 08:00 y si no se ha hecho hoy
        return (current_hour == 8 and
                current_time.minute == 0 and
                self.last_global_search != today)

def main():
    """Función principal del script de notificaciones mejorado"""
    # Configuración desde variables de entorno
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_CONCIERTOS')
    DB_PATH = os.getenv('DB_PATH', 'artist_tracker.db')

    if not TELEGRAM_TOKEN:
        logger.error("❌ No se ha configurado TELEGRAM_BOT_CONCIERTOS en las variables de entorno")
        sys.exit(1)

    if not os.path.exists(DB_PATH):
        logger.error(f"❌ No se encuentra la base de datos en {DB_PATH}")
        sys.exit(1)

    # Crear servicio de notificaciones
    notification_service = NotificationService(DB_PATH, TELEGRAM_TOKEN)

    logger.info("🔔 Script de notificaciones mejorado iniciado")
    logger.info("🌍 Búsqueda global: 08:00 diaria")
    logger.info("⏰ Notificaciones: Cada minuto según configuración de usuarios")

    try:
        while True:
            current_time = datetime.now()
            time_str = current_time.strftime('%H:%M')

            # 1. Verificar si es hora de búsqueda global (08:00)
            if notification_service.should_perform_global_search():
                logger.info("🌅 Es hora de la búsqueda global diaria (08:00)")
                asyncio.run(notification_service.perform_daily_global_search())

            # 2. Procesar notificaciones para la hora actual
            asyncio.run(notification_service.process_notifications_for_time(time_str))

            # Esperar hasta el siguiente minuto
            time.sleep(60)

    except KeyboardInterrupt:
        logger.info("🛑 Script de notificaciones detenido por el usuario")
    except Exception as e:
        logger.error(f"❌ Error crítico en el script de notificaciones: {e}")

if __name__ == "__main__":
    main()
