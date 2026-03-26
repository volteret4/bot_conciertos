#!/usr/bin/env python3
"""
Módulo de servicios de usuario para el sistema de seguimiento de artistas.
"""

import os
import logging
from typing import Dict, Set

logger = logging.getLogger(__name__)

# Referencias globales a servicios
country_state_city = None
ticketmaster_service = None
lastfm_service = None


def initialize_concert_services():
    """Inicializa Ticketmaster (único servicio de conciertos)."""
    global ticketmaster_service

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CACHE_DIR = os.path.join(BASE_DIR, "cache")
    os.makedirs(CACHE_DIR, exist_ok=True)

    TICKETMASTER_API_KEY = os.environ.get("TICKETMASTER_API_KEY")

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
    except Exception as e:
        logger.error(f"Error inicializando Ticketmaster: {e}")


def initialize_country_service(db_path: str = "artist_tracker.db"):
    """Inicializa el servicio de países y ciudades."""
    global country_state_city

    COUNTRY_API_KEY = os.environ.get("COUNTRY_CITY_API_KEY")

    if not COUNTRY_API_KEY:
        logger.warning("⚠️ COUNTRY_CITY_API_KEY no configurada — filtro por país deshabilitado")
        return False

    try:
        from apis.country_state_city import CountryCityService
        country_state_city = CountryCityService(api_key=COUNTRY_API_KEY, db_path=db_path)
        logger.info("✅ Servicio de países inicializado")
        return True
    except Exception as e:
        logger.error(f"❌ Error inicializando servicio de países: {e}")
        return False


def initialize_lastfm_service():
    """Inicializa Last.fm (solo para enriquecimiento de metadatos, no para importar artistas)."""
    global lastfm_service

    LASTFM_API_KEY = os.environ.get("LASTFM_API_KEY")

    if not LASTFM_API_KEY:
        logger.warning("⚠️ LASTFM_API_KEY no configurada — enriquecimiento Last.fm deshabilitado")
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
        logger.error(f"❌ Error inicializando Last.fm: {e}")
        lastfm_service = None
        return False


def validate_services():
    """Valida que los servicios están configurados."""
    issues = []

    try:
        from apis.mb_artist_info import search_artist_in_musicbrainz
        logger.info("✅ MusicBrainz disponible")
    except ImportError:
        issues.append("⚠️ MusicBrainz (mb_artist_info.py) no disponible")

    if not ticketmaster_service:
        issues.append("⚠️ Ticketmaster no inicializado")

    if not lastfm_service:
        issues.append("⚠️ Last.fm no inicializado (metadatos limitados)")

    for issue in issues:
        logger.warning(issue)

    return len([i for i in issues if 'Ticketmaster' in i]) == 0


def get_services():
    """Devuelve referencias a todos los servicios inicializados."""
    return {
        'country_state_city': country_state_city,
        'ticketmaster_service': ticketmaster_service,
        'lastfm_service': lastfm_service,
    }


class UserServices:
    """Gestión de configuración de usuario."""

    def __init__(self, database):
        self.db = database

    def set_notification_time(self, user_id: int, notification_time: str) -> bool:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "UPDATE users SET notification_time = ? WHERE id = ?",
                (notification_time, user_id)
            )
            conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error estableciendo hora de notificación: {e}")
            return False
        finally:
            conn.close()

    def set_notification_day(self, user_id: int, day: int) -> bool:
        """day: 0=lunes … 6=domingo"""
        return self.db.set_notification_day(user_id, day)

    def toggle_notifications(self, user_id: int) -> bool:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT notification_enabled FROM users WHERE id = ?", (user_id,))
            row = cursor.fetchone()
            if not row:
                return False
            new_state = not row[0]
            cursor.execute(
                "UPDATE users SET notification_enabled = ? WHERE id = ?",
                (new_state, user_id)
            )
            conn.commit()
            return new_state
        except Exception as e:
            logger.error(f"Error cambiando estado de notificaciones: {e}")
            return False
        finally:
            conn.close()

    def set_country_filter(self, user_id: int, country_code: str) -> bool:
        if country_state_city:
            user_countries = country_state_city.get_user_countries(user_id)
            for country in user_countries:
                country_state_city.remove_user_country(user_id, country['code'])
            return country_state_city.add_user_country(user_id, country_code)
        else:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "UPDATE users SET country_filter = ? WHERE id = ?",
                    (country_code.upper(), user_id)
                )
                conn.commit()
                return cursor.rowcount > 0
            except Exception as e:
                logger.error(f"Error estableciendo país: {e}")
                return False
            finally:
                conn.close()

    def get_user_services(self, user_id: int) -> Dict:
        conn = self.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT service_ticketmaster, country_filter FROM users WHERE id = ?",
                (user_id,)
            )
            row = cursor.fetchone()
            services = {
                'ticketmaster': bool(row[0]) if row else True,
                'country_filter': row[1] if row else 'ES',
            }

            if country_state_city:
                user_countries = country_state_city.get_user_country_codes(user_id)
                services['countries'] = user_countries
                if user_countries:
                    services['country_filter'] = list(user_countries)[0]
            else:
                services['countries'] = {services['country_filter']}

            return services
        except Exception as e:
            logger.error(f"Error obteniendo servicios del usuario: {e}")
            return {'ticketmaster': True, 'country_filter': 'ES', 'countries': {'ES'}}
        finally:
            conn.close()
