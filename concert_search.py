#!/usr/bin/env python3
"""
MÃ³dulo de bÃºsqueda y gestiÃ³n de conciertos
Contiene todas las funciones relacionadas con la bÃºsqueda y formateo de conciertos
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from telegram import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

async def search_concerts_for_artist(artist_name, user_services_config, user_id=None, services=None, database=None):
    """
    VersiÃ³n asÃ­ncrona de bÃºsqueda de conciertos con mejor manejo de base de datos

    Args:
        artist_name (str): Nombre del artista
        user_services_config (dict): ConfiguraciÃ³n de servicios del usuario
        user_id (int): ID del usuario
        services (dict): Servicios disponibles
        database: Instancia de base de datos (thread-safe)

    Returns:
        list: Lista de conciertos encontrados
    """
    if not services:
        logger.warning(f"No hay servicios disponibles para buscar {artist_name}")
        return []

    all_concerts = []

    # Configurar paÃ­ses para la bÃºsqueda
    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        country_filter = user_services_config.get('country_filter', 'ES')
        user_countries = {country_filter}

    logger.info(f"Buscando conciertos para {artist_name} en paÃ­ses: {user_countries}")

    # Crear tareas asÃ­ncronas para cada servicio
    tasks = []

    # TICKETMASTER
    if user_services_config.get('ticketmaster', True) and services.get('ticketmaster_service'):
        for country_code in user_countries:
            task = search_ticketmaster_async(artist_name, country_code, services['ticketmaster_service'])
            tasks.append(('ticketmaster', task))

    # SPOTIFY
    if user_services_config.get('spotify', True) and services.get('spotify_service'):
        for country_code in user_countries:
            task = search_spotify_async(artist_name, country_code, services['spotify_service'])
            tasks.append(('spotify', task))

    # SETLISTFM (si estÃ¡ disponible)
    if user_services_config.get('setlistfm', True) and services.get('setlistfm_service'):
        task = search_setlistfm_async(artist_name, services['setlistfm_service'])
        tasks.append(('setlistfm', task))

    # Ejecutar todas las bÃºsquedas concurrentemente
    if tasks:
        logger.info(f"Ejecutando {len(tasks)} bÃºsquedas concurrentes para {artist_name}")

        # Extraer solo las tareas
        task_list = [task for service_name, task in tasks]
        service_names = [service_name for service_name, task in tasks]

        try:
            results = await asyncio.gather(*task_list, return_exceptions=True)

            # Procesar resultados
            for i, result in enumerate(results):
                service_name = service_names[i]

                if isinstance(result, Exception):
                    logger.error(f"Error en {service_name} para {artist_name}: {result}")
                elif isinstance(result, list):
                    logger.info(f"{service_name}: {len(result)} conciertos para {artist_name}")
                    all_concerts.extend(result)
                else:
                    logger.warning(f"Resultado inesperado de {service_name}: {type(result)}")

        except Exception as e:
            logger.error(f"Error en bÃºsqueda concurrente para {artist_name}: {e}")

    # Guardar conciertos en base de datos de forma thread-safe
    if database and all_concerts:
        logger.info(f"Guardando {len(all_concerts)} conciertos para {artist_name}")
        await save_concerts_thread_safe(database, all_concerts)

    logger.info(f"BÃºsqueda completada para {artist_name}: {len(all_concerts)} conciertos")
    return all_concerts


async def search_ticketmaster_async(artist_name, country_code, ticketmaster_service):
    """BÃºsqueda asÃ­ncrona en Ticketmaster"""
    try:
        # Ejecutar en thread pool para no bloquear el loop
        loop = asyncio.get_event_loop()

        def search_sync():
            return ticketmaster_service.search_concerts(artist_name, country_code)

        concerts, message = await loop.run_in_executor(None, search_sync)
        logger.debug(f"Ticketmaster {country_code}: {len(concerts)} conciertos para {artist_name}")
        return concerts

    except Exception as e:
        logger.error(f"Error en Ticketmaster para {artist_name} ({country_code}): {e}")
        return []


async def search_spotify_async(artist_name, country_code, spotify_service):
    """Búsqueda asíncrona en Spotify con filtro de país"""
    try:
        # Ejecutar en thread pool para no bloquear el loop
        loop = asyncio.get_event_loop()

        def search_sync():
            return spotify_service.search_artist_and_concerts(artist_name, country_code)

        concerts, message = await loop.run_in_executor(None, search_sync)
        logger.debug(f"Spotify {country_code}: {len(concerts)} conciertos para {artist_name}")
        return concerts

    except Exception as e:
        logger.error(f"Error en Spotify para {artist_name} ({country_code}): {e}")
        return []


async def search_setlistfm_async(artist_name, setlistfm_service):
    """BÃºsqueda asÃ­ncrona en SetlistFM"""
    try:
        # Ejecutar en thread pool para no bloquear el loop
        loop = asyncio.get_event_loop()

        def search_sync():
            return setlistfm_service.search_concerts(artist_name)

        concerts, message = await loop.run_in_executor(None, search_sync)
        logger.debug(f"SetlistFM: {len(concerts)} conciertos para {artist_name}")
        return concerts

    except Exception as e:
        logger.error(f"Error en SetlistFM para {artist_name}: {e}")
        return []


async def save_concerts_thread_safe(database, concerts):
    """Guarda conciertos en base de datos de forma thread-safe"""
    try:
        # Ejecutar en thread pool para no bloquear el loop
        loop = asyncio.get_event_loop()

        def save_sync():
            saved_count = 0
            for concert in concerts:
                try:
                    database.save_concert(concert)
                    saved_count += 1
                except Exception as e:
                    logger.error(f"Error guardando concierto individual: {e}")
            return saved_count

        saved_count = await loop.run_in_executor(None, save_sync)
        logger.debug(f"Guardados {saved_count}/{len(concerts)} conciertos en base de datos")

    except Exception as e:
        logger.error(f"Error guardando conciertos en base de datos: {e}")




async def update_concerts_database(database, services: Dict = None):
    """
    Actualiza la base de datos con nuevos conciertos
    VERSIÃ“N MEJORADA: Guarda todos los conciertos globalmente con pausas

    Args:
        database: Instancia de la base de datos
        services: Diccionario con referencias a los servicios
    """
    if services is None:
        from user_services import get_services
        services = get_services()

    logger.info("Actualizando base de datos de conciertos...")

    # Obtener todos los artistas Ãºnicos de la base de datos
    conn = database.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT DISTINCT name FROM artists")
        artists = [row[0] for row in cursor.fetchall()]

        total_new_concerts = 0
        total_all_concerts = 0
        processed_artists = 0

        logger.info(f"Iniciando actualizaciÃ³n para {len(artists)} artistas")

        for artist_name in artists:
            processed_artists += 1

            if processed_artists % 10 == 0:
                logger.info(f"Progreso: {processed_artists}/{len(artists)} artistas procesados")

            logger.debug(f"Buscando conciertos globalmente para {artist_name}")

            # Buscar con configuraciÃ³n global (todos los servicios activos)
            global_services = {
                'ticketmaster': True,
                'spotify': True,
                'setlistfm': True,
                'countries': {'ES', 'US', 'FR', 'DE', 'IT', 'GB', 'CA', 'AU', 'JP', 'BR'}  # PaÃ­ses principales
            }

            concerts = await search_concerts_for_artist(artist_name, global_services, services=services, database=database)
            total_all_concerts += len(concerts)

            # Los conciertos ya se guardan dentro de search_concerts_for_artist
            # Solo necesitamos contar los nuevos
            for concert in concerts:
                # Verificar si es nuevo (esto es aproximado ya que save_concert devuelve ID o None)
                concert_id = database.save_concert(concert)
                if concert_id:
                    total_new_concerts += 1

            # Pausa de 1 segundo para no sobrecargar las APIs
            await asyncio.sleep(1.0)

        logger.info(f"ActualizaciÃ³n completada: {total_new_concerts} nuevos conciertos de {total_all_concerts} encontrados")
        logger.info(f"Total artistas procesados: {processed_artists}")

    except Exception as e:
        logger.error(f"Error actualizando base de datos de conciertos: {e}")
    finally:
        conn.close()

def format_concerts_message(concerts: List[Dict], title: str = "ðŸŽµ Conciertos encontrados",
                          show_notified: bool = False, show_expand_buttons: bool = False,
                          user_id: int = None) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    """
    Formatea una lista de conciertos para mostrar en Telegram
    MANTIENE LA FUNCIONALIDAD ORIGINAL pero con opciÃ³n de botones
    """
    if not concerts:
        return f"{title}\n\nâŒ No se encontraron conciertos.", None

    message_lines = [f"{title}\n"]

    # Agrupar por artista
    concerts_by_artist = {}
    for concert in concerts:
        artist = concert.get('artist_name', 'Artista desconocido')
        if artist not in concerts_by_artist:
            concerts_by_artist[artist] = []
        concerts_by_artist[artist].append(concert)

    # Mostrar conciertos como antes (formato original)
    for artist, artist_concerts in concerts_by_artist.items():
        # Escapar caracteres especiales
        safe_artist = artist.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")
        message_lines.append(f"*{safe_artist}*:")

        for concert in artist_concerts[:5]:  # Limitar a 5 por artista como antes
            venue = concert.get('venue', 'Lugar desconocido')
            city = concert.get('city', '')
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

            location = f"{safe_venue}, {safe_city}" if safe_city else safe_venue

            concert_line = f"â€¢ {date}: "

            if url and url.startswith(('http://', 'https://')):
                url = url.replace(")", "\\)")
                concert_line += f"[{location}]({url})"
            else:
                concert_line += location

            if source:
                concert_line += f" _{source}_"

            if show_notified and concert.get('notified'):
                concert_line += " âœ…"

            message_lines.append(concert_line)

        if len(artist_concerts) > 5:
            remaining = len(artist_concerts) - 5
            message_lines.append(f"_...y {remaining} mÃ¡s_")

        message_lines.append("")

    message_lines.append(f"ðŸ“Š Total: {len(concerts)} conciertos")

    # Crear botones solo si se solicita Y hay mÃ¡s de 5 conciertos por artista
    keyboard = None
    if show_expand_buttons and user_id:
        buttons = []

        # BotÃ³n para expandir todos los conciertos
        buttons.append([InlineKeyboardButton("ðŸ“‹ Ver todos los conciertos", callback_data=f"expand_all_{user_id}")])

        # Botones para artistas con mÃ¡s de 5 conciertos
        for artist, artist_concerts in concerts_by_artist.items():
            if len(artist_concerts) > 5:
                button_text = f"ðŸŽµ Ver todos los de {artist}"
                if len(button_text) > 35:
                    button_text = f"ðŸŽµ {artist}"
                    if len(button_text) > 35:
                        button_text = button_text[:32] + "..."

                # Usar el mismo sistema de callback que ya existe
                buttons.append([InlineKeyboardButton(button_text, callback_data=f"expand_artist_{artist}_{user_id}")])

        if len(buttons) > 1:  # Solo crear teclado si hay mÃ¡s que el botÃ³n "ver todos"
            keyboard = InlineKeyboardMarkup(buttons)

    return "\n".join(message_lines), keyboard

def format_single_artist_concerts_complete(concerts: List[Dict], artist_name: str, show_notified: bool = False) -> str:
    """
    Formatea todos los conciertos de un artista especÃ­fico
    VERSIÃ“N MEJORADA: Filtra y muestra solo conciertos futuros (SIN filtrar por notificaciones)

    Args:
        concerts: Lista de conciertos del artista
        artist_name: Nombre del artista
        show_notified: Si mostrar informaciÃ³n de notificaciÃ³n (no filtra, solo muestra)

    Returns:
        Mensaje formateado con todos los conciertos futuros del artista
    """
    if not concerts:
        return f"ðŸŽµ *{artist_name}*\n\nâŒ No se encontraron conciertos."

    # Filtrar solo conciertos futuros (NO filtrar por notificaciones)
    today = datetime.now().date()
    future_concerts = []

    for concert in concerts:
        concert_date = concert.get('date', '')
        if concert_date and len(concert_date) >= 10:
            try:
                concert_date_obj = datetime.strptime(concert_date[:10], '%Y-%m-%d').date()
                if concert_date_obj >= today:
                    future_concerts.append(concert)
            except ValueError:
                # Si no se puede parsear la fecha, incluir el concierto por seguridad
                future_concerts.append(concert)
        else:
            # Si no hay fecha, incluir por seguridad
            future_concerts.append(concert)

    if not future_concerts:
        return f"ðŸŽµ *{artist_name}*\n\nðŸ“… No hay conciertos futuros programados."

    # Escapar caracteres especiales del nombre del artista
    safe_artist = artist_name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

    message_lines = [f"ðŸŽµ *{safe_artist} - PrÃ³ximos conciertos*\n"]

    # Ordenar conciertos por fecha (mÃ¡s prÃ³ximos primero)
    sorted_concerts = sorted(future_concerts, key=lambda x: x.get('date', '9999-12-31'))

    for i, concert in enumerate(sorted_concerts, 1):
        venue = concert.get('venue', 'Lugar desconocido')
        city = concert.get('city', '')
        country = concert.get('country', '')
        date = concert.get('date', 'Fecha desconocida')
        time = concert.get('time', '')
        url = concert.get('url', '')
        source = concert.get('source', '')

        # Formatear fecha
        formatted_date = date
        if date and len(date) >= 10 and '-' in date:
            try:
                date_obj = datetime.strptime(date[:10], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d/%m/%Y')

                # Calcular dÃ­as hasta el concierto
                days_until = (date_obj.date() - today).days
                if days_until == 0:
                    formatted_date += " (Â¡HOY!)"
                elif days_until == 1:
                    formatted_date += " (maÃ±ana)"
                elif days_until <= 7:
                    formatted_date += f" (en {days_until} dÃ­as)"
            except ValueError:
                pass

        # Escapar caracteres especiales
        safe_venue = str(venue).replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")
        safe_city = str(city).replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        # Construir lÃ­nea del concierto
        concert_line = f"*{i}.* {formatted_date}"

        if time:
            concert_line += f" a las {time}"

        concert_line += "\n"

        # UbicaciÃ³n con enlace si estÃ¡ disponible
        location_parts = []
        if safe_venue:
            location_parts.append(safe_venue)
        if safe_city:
            location_parts.append(safe_city)
        if country:
            location_parts.append(f"({country})")

        location = ", ".join(location_parts) if location_parts else "UbicaciÃ³n desconocida"

        if url and url.startswith(('http://', 'https://')):
            # Escapar parÃ©ntesis en URL
            escaped_url = url.replace(")", "\\)")
            concert_line += f"   ðŸ“ [{location}]({escaped_url})"
        else:
            concert_line += f"   ðŸ“ {location}"

        # InformaciÃ³n adicional
        if source:
            concert_line += f"\n   ðŸ”— _{source}_"

        # OPCIONAL: Mostrar informaciÃ³n de notificaciÃ³n (solo informativo, no filtra)
        if show_notified:
            if concert.get('notified'):
                concert_line += " âœ…"  # Ya notificado
            # No mostrar nada si no estÃ¡ notificado (evitar spam visual)

        message_lines.append(concert_line)
        message_lines.append("")  # LÃ­nea en blanco entre conciertos

    # EstadÃ­sticas finales
    total_concerts = len(future_concerts)
    message_lines.append(f"ðŸ“Š *Total: {total_concerts} conciertos futuros*")

    # OPCIONAL: Mostrar estadÃ­sticas de notificaciÃ³n solo si se solicita y hay datos
    if show_notified:
        notified_count = sum(1 for c in future_concerts if c.get('notified'))
        if notified_count > 0:
            message_lines.append(f"âœ… Previamente notificados: {notified_count}")

    return "\n".join(message_lines)

def format_expanded_concerts_message_original(concerts: List[Dict], title: str) -> str:
    """Formatea todos los conciertos usando el formato ORIGINAL pero sin lÃ­mite"""
    if not concerts:
        return f"{title}\n\nâŒ No se encontraron conciertos."

    message_lines = [f"{title}\n"]

    # Agrupar por artista
    concerts_by_artist = {}
    for concert in concerts:
        artist = concert.get('artist_name', 'Artista desconocido')
        if artist not in concerts_by_artist:
            concerts_by_artist[artist] = []
        concerts_by_artist[artist].append(concert)

    for artist, artist_concerts in concerts_by_artist.items():
        # Escapar caracteres especiales
        safe_artist = artist.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")
        message_lines.append(f"*{safe_artist}*:")

        # Mostrar TODOS los conciertos (sin lÃ­mite de 5)
        for concert in artist_concerts:
            venue = concert.get('venue', 'Lugar desconocido')
            city = concert.get('city', '')
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

            location = f"{safe_venue}, {safe_city}" if safe_city else safe_venue

            concert_line = f"â€¢ {date}: "

            if url and url.startswith(('http://', 'https://')):
                url = url.replace(")", "\\)")
                concert_line += f"[{location}]({url})"
            else:
                concert_line += location

            if source:
                concert_line += f" _{source}_"

            message_lines.append(concert_line)

        message_lines.append("")

    message_lines.append(f"ðŸ“Š Total: {len(concerts)} conciertos")

    return "\n".join(message_lines)

def format_expanded_concerts_message(concerts: List[Dict], title: str) -> str:
    """Formatea todos los conciertos sin lÃ­mite"""
    if not concerts:
        return f"{title}\n\nâŒ No se encontraron conciertos."

    message_lines = [f"{title}\n"]

    # Agrupar por artista
    concerts_by_artist = {}
    for concert in concerts:
        artist = concert.get('artist_name', 'Artista desconocido')
        if artist not in concerts_by_artist:
            concerts_by_artist[artist] = []
        concerts_by_artist[artist].append(concert)

    for artist, artist_concerts in concerts_by_artist.items():
        # Escapar caracteres especiales
        safe_artist = artist.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")
        message_lines.append(f"*{safe_artist}* ({len(artist_concerts)} conciertos):")

        # Mostrar TODOS los conciertos
        for concert in artist_concerts:
            venue = concert.get('venue', 'Lugar desconocido')
            city = concert.get('city', '')
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

            location = f"{safe_venue}, {safe_city}" if safe_city else safe_venue

            concert_line = f"â€¢ {date}: "

            if url and url.startswith(('http://', 'https://')):
                url = url.replace(")", "\\)")
                concert_line += f"[{location}]({url})"
            else:
                concert_line += location

            if source:
                concert_line += f" _{source}_"

            message_lines.append(concert_line)

        message_lines.append("")

    message_lines.append(f"ðŸ“Š Total: {len(concerts)} conciertos")

    return "\n".join(message_lines)

def format_artist_concerts_detailed(concerts: List[Dict], artist_name: str, show_notified: bool = False) -> str:
    """Formatea conciertos de un artista de manera detallada - FUNCIÃ“N FALTANTE"""
    if not concerts:
        return f"ðŸ“­ No hay conciertos para {artist_name}"

    # Filtrar solo conciertos futuros
    today = datetime.now().date()
    future_concerts = []

    for concert in concerts:
        concert_date = concert.get('date', '')
        if concert_date and len(concert_date) >= 10:
            try:
                concert_date_obj = datetime.strptime(concert_date[:10], '%Y-%m-%d').date()
                if concert_date_obj >= today:
                    future_concerts.append(concert)
            except ValueError:
                future_concerts.append(concert)
        else:
            future_concerts.append(concert)

    if not future_concerts:
        return f"ðŸ“­ No hay conciertos futuros para {artist_name}"

    # Ordenar por fecha
    future_concerts.sort(key=lambda x: x.get('date', ''))

    message_lines = [f"ðŸŽµ *Conciertos de {artist_name}*\n"]

    for i, concert in enumerate(future_concerts, 1):
        venue = concert.get('venue', 'Venue TBA')
        city = concert.get('city', '')
        country = concert.get('country', '')
        date = concert.get('date', '')
        url = concert.get('url', '')
        source = concert.get('source', '')

        # Formatear fecha
        if date and len(date) >= 10:
            try:
                date_obj = datetime.strptime(date[:10], '%Y-%m-%d')
                formatted_date = date_obj.strftime('%d/%m/%Y')
            except ValueError:
                formatted_date = date
        else:
            formatted_date = 'Fecha TBA'

        # Crear lÃ­nea del concierto
        line = f"{i}. *{venue}*"

        # AÃ±adir ubicaciÃ³n
        location_parts = []
        if city:
            location_parts.append(city)
        if country:
            location_parts.append(country)

        if location_parts:
            line += f" - {', '.join(location_parts)}"

        line += f" ({formatted_date})"

        # AÃ±adir enlace si estÃ¡ disponible
        if url:
            line += f" [ðŸŽ«]({url})"

        # Mostrar fuente
        if source:
            line += f" _{source}_"

        # Mostrar si ya se notificÃ³
        if show_notified and concert.get('notified'):
            line += " âœ…"

        message_lines.append(line)

    message_lines.append(f"\nðŸ“Š Total: {len(future_concerts)} conciertos futuros")

    return "\n".join(message_lines)


def split_long_message(message: str, max_length: int = 4000) -> List[str]:
    """Divide un mensaje largo en chunks mÃ¡s pequeÃ±os"""
    if len(message) <= max_length:
        return [message]

    chunks = []
    lines = message.split('\n')
    current_chunk = []
    current_length = 0

    for line in lines:
        line_length = len(line) + 1  # +1 para el salto de lÃ­nea

        if current_length + line_length > max_length and current_chunk:
            # Guardar chunk actual y empezar uno nuevo
            chunks.append('\n'.join(current_chunk))
            current_chunk = [line]
            current_length = line_length
        else:
            current_chunk.append(line)
            current_length += line_length

    # AÃ±adir el Ãºltimo chunk
    if current_chunk:
        chunks.append('\n'.join(current_chunk))

    return chunks
