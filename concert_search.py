#!/usr/bin/env python3
"""
Búsqueda de conciertos vía Ticketmaster.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)


async def search_concerts_for_artist(
    artist_name: str,
    user_services_config: Dict,
    user_id: int = None,
    services: Dict = None,
    database=None,
) -> List[Dict]:
    """
    Busca conciertos para un artista en Ticketmaster, filtrando por países del usuario.

    Args:
        artist_name: Nombre del artista
        user_services_config: Dict con claves 'countries' (set) y 'country_filter' (str)
        services: Dict con clave 'ticketmaster_service'
        database: Instancia thread-safe de BD (opcional, para guardar resultados)

    Returns:
        Lista de conciertos encontrados
    """
    ticketmaster = (services or {}).get('ticketmaster_service')
    if not ticketmaster:
        logger.warning(f"Ticketmaster no disponible para buscar {artist_name}")
        return []

    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        cf = user_services_config.get('country_filter')
        user_countries = {cf} if cf else set()

    # Eliminar valores None o vacíos
    user_countries = {c for c in user_countries if c}
    if not user_countries:
        logger.warning(f"Sin países configurados para buscar {artist_name}")
        return []

    logger.info(f"Buscando conciertos para {artist_name} en países: {user_countries}")

    tasks = [
        _search_ticketmaster(artist_name, code, ticketmaster)
        for code in user_countries
    ]

    all_concerts = []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error en búsqueda Ticketmaster para {artist_name}: {result}")
        elif isinstance(result, list):
            all_concerts.extend(result)

    # Deduplicar por (venue, date)
    seen = set()
    unique = []
    for c in all_concerts:
        key = (c.get('venue', ''), c.get('date', ''), c.get('city', ''))
        if key not in seen:
            seen.add(key)
            unique.append(c)

    if database and unique:
        await _save_concerts(database, unique)
    if database:
        try:
            database.cleanup_old_concerts(days=7)
        except Exception as e:
            logger.warning(f"cleanup_old_concerts: {e}")

    logger.info(f"Búsqueda completada para {artist_name}: {len(unique)} conciertos únicos")
    return unique


async def _search_ticketmaster(artist_name: str, country_code: str, ticketmaster_service) -> List[Dict]:
    loop = asyncio.get_event_loop()
    try:
        concerts, _ = await loop.run_in_executor(
            None, lambda: ticketmaster_service.search_concerts(artist_name, country_code)
        )
        return concerts
    except Exception as e:
        logger.error(f"Error Ticketmaster {country_code} para {artist_name}: {e}")
        return []


async def search_concerts_global(artist_name: str, ticketmaster_service) -> List[Dict]:
    """Búsqueda global (sin filtro de país) para el scheduler semanal."""
    loop = asyncio.get_event_loop()
    try:
        concerts, _ = await loop.run_in_executor(
            None, lambda: ticketmaster_service.search_concerts_global(artist_name)
        )
        return concerts
    except Exception as e:
        logger.error(f"Error Ticketmaster global para {artist_name}: {e}")
        return []


async def _save_concerts(database, concerts: List[Dict]):
    loop = asyncio.get_event_loop()
    try:
        def _save():
            count = 0
            for c in concerts:
                try:
                    # Normalizar clave: Ticketmaster usa 'artist', DB espera 'artist_name'
                    if 'artist_name' not in c or not c['artist_name']:
                        c = dict(c, artist_name=c.get('artist', ''))
                    database.save_concert(c)
                    count += 1
                except Exception as e:
                    logger.error(f"Error guardando concierto: {e}")
            return count
        saved = await loop.run_in_executor(None, _save)
        logger.debug(f"Guardados {saved}/{len(concerts)} conciertos en BD")
    except Exception as e:
        logger.error(f"Error guardando conciertos: {e}")


def format_concerts_message(
    concerts: List[Dict],
    title: str = "🎵 Conciertos encontrados",
    show_notified: bool = False,
    show_expand_buttons: bool = False,
    user_id: int = None,
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    """Formatea lista de conciertos para Telegram."""
    if not concerts:
        return f"{title}\n\n❌ No se encontraron conciertos.", None

    lines = [f"{title}\n"]
    by_artist: Dict[str, List[Dict]] = {}
    for c in concerts:
        artist = c.get('artist_name', 'Artista desconocido')
        by_artist.setdefault(artist, []).append(c)

    for artist, artist_concerts in by_artist.items():
        safe = _esc(artist)
        lines.append(f"*{safe}*:")
        for c in artist_concerts[:5]:
            lines.append(_format_concert_line(c, show_notified))
        if len(artist_concerts) > 5:
            lines.append(f"_...y {len(artist_concerts) - 5} más_")
        lines.append("")

    lines.append(f"📊 Total: {len(concerts)} conciertos")
    return "\n".join(lines), None


def format_single_artist_concerts_complete(
    concerts: List[Dict], artist_name: str, show_notified: bool = False
) -> str:
    """Formatea todos los conciertos futuros de un artista."""
    today = datetime.now().date()
    future = [c for c in concerts if _is_future(c, today)]

    if not future:
        return f"🎵 *{_esc(artist_name)}*\n\n📅 No hay conciertos futuros programados."

    future.sort(key=lambda x: x.get('date', '9999-12-31'))
    lines = [f"🎵 *{_esc(artist_name)} - Próximos conciertos*\n"]

    for i, c in enumerate(future, 1):
        date_str = c.get('date', '')
        time_str = c.get('time', '')
        venue = c.get('venue', 'Lugar desconocido')
        city = c.get('city', '')
        country = c.get('country', '')
        url = c.get('url', '')
        source = c.get('source', '')

        formatted_date = _format_date_with_countdown(date_str, today)
        location_parts = [p for p in [_esc(venue), _esc(city), f"({country})" if country else ''] if p]
        location = ', '.join(location_parts) or 'Ubicación desconocida'

        line = f"*{i}.* {formatted_date}"
        if time_str:
            line += f" a las {time_str}"
        line += "\n"

        if url and url.startswith(('http://', 'https://')):
            line += f"   📍 [{location}]({url.replace(')', '\\)')})"
        else:
            line += f"   📍 {location}"

        if show_notified and c.get('notified'):
            line += " ✅"

        lines.append(line)
        lines.append("")

    lines.append(f"📊 *Total: {len(future)} conciertos futuros*")
    return "\n".join(lines)


def split_long_message(message: str, max_length: int = 4000) -> List[str]:
    """Divide un mensaje largo en chunks."""
    if len(message) <= max_length:
        return [message]

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


# ─── Helpers internos ────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    return str(text).replace('*', '\\*').replace('_', '\\_').replace('`', '\\`').replace('[', '\\[')


def _is_future(concert: Dict, today) -> bool:
    date_str = concert.get('date', '')
    if not date_str or len(date_str) < 10:
        return True
    try:
        return datetime.strptime(date_str[:10], '%Y-%m-%d').date() >= today
    except ValueError:
        return True


def _format_date_with_countdown(date_str: str, today) -> str:
    if not date_str or len(date_str) < 10 or '-' not in date_str:
        return date_str or 'Fecha desconocida'
    try:
        d = datetime.strptime(date_str[:10], '%Y-%m-%d')
        formatted = d.strftime('%d/%m/%Y')
        days = (d.date() - today).days
        if days == 0:
            formatted += " (¡HOY!)"
        elif days == 1:
            formatted += " (mañana)"
        elif days <= 7:
            formatted += f" (en {days} días)"
        return formatted
    except ValueError:
        return date_str


def _format_concert_line(concert: Dict, show_notified: bool = False) -> str:
    venue = concert.get('venue', 'Lugar desconocido')
    city = concert.get('city', '')
    date = concert.get('date', 'Fecha desconocida')
    url = concert.get('url', '')
    source = concert.get('source', '')

    if date and len(date) >= 10 and '-' in date:
        try:
            date = datetime.strptime(date[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
        except ValueError:
            pass

    safe_venue = _esc(venue)
    safe_city = _esc(city)
    location = f"{safe_venue}, {safe_city}" if safe_city else safe_venue
    line = f"• {date}: "

    if url and url.startswith(('http://', 'https://')):
        line += f"[{location}]({url.replace(')', '\\)')})"
    else:
        line += location

    if show_notified and concert.get('notified'):
        line += " ✅"

    return line
