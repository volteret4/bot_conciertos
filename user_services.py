#!/usr/bin/env python3
"""
Módulo de servicios de usuario para el sistema de seguimiento de artistas
Contiene la configuración de usuarios y inicialización de servicios
"""

import os
import logging
from typing import Dict, Set
from datetime import datetime

logger = logging.getLogger(__name__)

# Variables globales para los servicios
country_state_city = None
ticketmaster_service = None
spotify_service = None
setlistfm_service = None
lastfm_service = None

def initialize_concert_services():
    """Inicializa los servicios de búsqueda de conciertos"""
    global ticketmaster_service, spotify_service, setlistfm_service

    # Configuración desde variables de entorno
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CACHE_DIR = os.path.join(BASE_DIR, "cache")
    os.makedirs(CACHE_DIR, exist_ok=True)

    TICKETMASTER_API_KEY = os.environ.get("TICKETMASTER_API_KEY")
    SPOTIFY_CLIENT_ID = os.environ.get("SPOTIFY_CLIENT_ID_VVMM")
    SPOTIFY_CLIENT_SECRET = os.environ.get("SPOTIFY_CLIENT_SECRET_VVMM")
    SPOTIFY_REDIRECT_URI = os.environ.get("SPOTIFY_REDIRECT_URI_VVMM")
    SETLISTFM_API_KEY = os.environ.get("SETLISTFM_API_KEY")

    try:
        if TICKETMASTER_API_KEY:
            from apis.ticketmaster import TicketmasterService
            ticketmaster_service = TicketmasterService(
                api_key=TICKETMASTER_API_KEY,
                cache_dir=os.path.join(CACHE_DIR, "ticketmaster")
            )
            logger.info("✅ Ticketmaster service inicializado")
        else:
            logger.warning("⚠️ TICKETMASTER_API_KEY no configurada")

        if SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET:
            from apis.spotify import SpotifyService
            spotify_service = SpotifyService(
                client_id=SPOTIFY_CLIENT_ID,
                client_secret=SPOTIFY_CLIENT_SECRET,
                redirect_uri=SPOTIFY_REDIRECT_URI,
                cache_dir=os.path.join(CACHE_DIR, "spotify")
            )
            logger.info("✅ Spotify service inicializado")
        else:
            logger.warning("⚠️ Credenciales de Spotify incompletas")

        if SETLISTFM_API_KEY:
            from apis.setlistfm import SetlistfmService
            setlistfm_service = SetlistfmService(
                api_key=SETLISTFM_API_KEY,
                cache_dir=os.path.join(CACHE_DIR, "setlistfm"),
                db_path=None
            )
            logger.info("✅ Setlist.fm service inicializado")
        else:
            logger.warning("⚠️ SETLISTFM_API_KEY no configurada")

    except Exception as e:
        logger.error(f"Error inicializando servicios: {e}")

def initialize_country_service(db_path: str = "artist_tracker.db"):
    """Inicializa el servicio de países y ciudades"""
    global country_state_city

    COUNTRY_API_KEY = os.environ.get("COUNTRY_CITY_API_KEY")

    if not COUNTRY_API_KEY:
        logger.warning("⚠️ COUNTRY_CITY_API_KEY no configurada")
        logger.warning("⚠️ Funcionalidad de países múltiples deshabilitada")
        return False

    try:
        from apis.country_state_city import CountryCityService

        country_state_city = CountryCityService(
            api_key=COUNTRY_API_KEY,
            db_path=db_path
        )

        logger.info("✅ Servicio de países y ciudades inicializado")
        return True

    except Exception as e:
        logger.error(f"❌ Error inicializando servicio de países: {e}")
        return False

def initialize_lastfm_service():
    """Inicializa el servicio de Last.fm"""
    global lastfm_service

    LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

    if not LASTFM_API_KEY:
        logger.warning("⚠️ LASTFM_API_KEY no configurada")
        logger.warning("⚠️ Funcionalidad de Last.fm deshabilitada")
        return False

    try:
        from apis.lastfm import LastFmService
    except ImportError:
        logger.warning("⚠️ LastFmService no disponible")
        return False

    try:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        CACHE_DIR = os.path.join(BASE_DIR, "cache")

        lastfm_service = LastFmService(
            api_key=LASTFM_API_KEY,
            cache_dir=os.path.join(CACHE_DIR, "lastfm")
        )

        if lastfm_service.setup():
            logger.info("✅ Servicio de Last.fm inicializado")
            return True
        else:
            logger.error("❌ Error configurando Last.fm")
            lastfm_service = None
            return False

    except Exception as e:
        logger.error(f"❌ Error inicializando servicio de Last.fm: {e}")
        lastfm_service = None
        return False

def validate_services():
    """Valida que los servicios están configurados correctamente"""
    issues = []

    try:
        from apis.mb_artist_info import search_artist_in_musicbrainz
        logger.info("✅ MusicBrainz configurado correctamente")
    except ImportError:
        issues.append("⚠️ MusicBrainz (mb_artist_info.py) no disponible")

    if not ticketmaster_service:
        issues.append("⚠️ Ticketmaster service no inicializado")

    if not spotify_service:
        issues.append("⚠️ Spotify service no inicializado")

    if not setlistfm_service:
        issues.append("⚠️ Setlist.fm service no inicializado")

    if not lastfm_service:
        issues.append("⚠️ Last.fm service no inicializado")

    if issues:
        logger.warning("Problemas de configuración detectados:")
        for issue in issues:
            logger.warning(issue)
    else:
        logger.info("✅ Todos los servicios están configurados")

    return len(issues) == 0

class UserServices:
    """Clase para manejar los servicios de configuración de usuarios"""

    def __init__(self, database):
        """
        Inicializa la clase con referencia a la base de datos

        Args:
            database: Instancia de ArtistTrackerDatabase
        """
        self.db = database

    def set_notification_time(self, user_id: int, notification_time: str) -> bool:
        """
        Establece la hora de notificación para un usuario

        Args:
            user_id: ID del usuario
            notification_time: Hora en formato HH:MM

        Returns:
            True si se actualizó correctamente
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE users SET notification_time = ? WHERE id = ?
            """, (notification_time, user_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"Error al establecer hora de notificación: {e}")
            return False
        finally:
            conn.close()

    def toggle_notifications(self, user_id: int) -> bool:
        """
        Activa/desactiva las notificaciones para un usuario

        Args:
            user_id: ID del usuario

        Returns:
            True si están activadas después del cambio
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # Obtener estado actual
            cursor.execute("SELECT notification_enabled FROM users WHERE id = ?", (user_id,))
            current_state = cursor.fetchone()[0]

            # Cambiar estado
            new_state = not current_state
            cursor.execute("""
                UPDATE users SET notification_enabled = ? WHERE id = ?
            """, (new_state, user_id))

            conn.commit()
            return new_state

        except Exception as e:
            logger.error(f"Error al cambiar estado de notificaciones: {e}")
            return False
        finally:
            conn.close()

    def set_country_filter(self, user_id: int, country_code: str) -> bool:
        """
        VERSIÓN LEGACY - Mantener compatibilidad
        Ahora redirige al sistema de países múltiples
        """
        if country_state_city:
            # Limpiar países existentes y añadir el nuevo
            user_countries = country_state_city.get_user_countries(user_id)
            for country in user_countries:
                country_state_city.remove_user_country(user_id, country['code'])

            # Añadir el nuevo país
            return country_state_city.add_user_country(user_id, country_code)
        else:
            # Fallback al sistema original
            conn = self.db.get_connection()
            cursor = conn.cursor()

            try:
                cursor.execute("""
                    UPDATE users SET country_filter = ? WHERE id = ?
                """, (country_code.upper(), user_id))

                conn.commit()
                return cursor.rowcount > 0

            except Exception as e:
                logger.error(f"Error al establecer filtro de país: {e}")
                return False
            finally:
                conn.close()

    def set_service_status(self, user_id: int, service: str, enabled: bool) -> bool:
        """
        Activa o desactiva un servicio para un usuario

        Args:
            user_id: ID del usuario
            service: Nombre del servicio (ticketmaster, spotify, setlistfm)
            enabled: True para activar, False para desactivar

        Returns:
            True si se actualizó correctamente
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()

        try:
            # Validar nombre del servicio
            valid_services = ['ticketmaster', 'spotify', 'setlistfm']
            if service.lower() not in valid_services:
                return False

            column_name = f"service_{service.lower()}"

            cursor.execute(f"""
                UPDATE users SET {column_name} = ? WHERE id = ?
            """, (enabled, user_id))

            conn.commit()
            return cursor.rowcount > 0

        except Exception as e:
            logger.error(f"Error al establecer estado del servicio: {e}")
            return False
        finally:
            conn.close()

    def get_user_services(self, user_id: int) -> Dict[str, any]:
        """
        VERSIÓN EXTENDIDA - Incluye países múltiples
        """
        conn = self.db.get_connection()
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
            if country_state_city:
                user_countries = country_state_city.get_user_country_codes(user_id)
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

        except Exception as e:
            logger.error(f"Error al obtener servicios del usuario: {e}")
            return {
                'ticketmaster': True,
                'spotify': True,
                'setlistfm': True,
                'country_filter': 'ES',
                'countries': {'ES'}
            }
        finally:
            conn.close()

def get_user_services_extended(user_services_instance, user_id: int) -> Dict[str, any]:
    """
    Versión extendida que incluye países múltiples

    Args:
        user_services_instance: Instancia de UserServices
        user_id: ID del usuario

    Returns:
        Dict con servicios y países del usuario
    """
    # Obtener configuración original
    original_services = user_services_instance.get_user_services(user_id)

    # Añadir información de países
    if country_state_city:
        user_countries = country_state_city.get_user_country_codes(user_id)
        original_services['countries'] = user_countries
        original_services['country_filter'] = list(user_countries)[0] if user_countries else 'ES'  # Compatibilidad
    else:
        original_services['countries'] = {original_services.get('country_filter', 'ES')}

    return original_services

def get_services():
    """
    Obtiene referencias a todos los servicios inicializados

    Returns:
        Dict con referencias a los servicios
    """
    return {
        'country_state_city': country_state_city,
        'ticketmaster_service': ticketmaster_service,
        'spotify_service': spotify_service,
        'setlistfm_service': setlistfm_service,
        'lastfm_service': lastfm_service
    }
