#!/usr/bin/env python3
"""
Handlers para funcionalidades de calendario (/cal)
Genera archivos ICS para conciertos y discos
"""

import logging
import tempfile
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import re

logger = logging.getLogger(__name__)

class CalendarHandlers:
    """Clase que contiene todos los handlers de calendario"""

    def __init__(self, database, muspy_service):
        self.db = database
        self.muspy_service = muspy_service

        # Importar servicio de países cuando sea necesario
        self.country_service = None

        # Inicializar concert_services como None - se obtendrán cuando sea necesario
        self.concert_services = None

    def _get_concert_services(self):
        """Obtiene los servicios de conciertos cuando sea necesario"""
        if self.concert_services is None:
            try:
                from user_services import get_services
                services = get_services()
                self.concert_services = {
                    'ticketmaster': services.get('ticketmaster_service'),
                    'lastfm': services.get('lastfm_service'),
                    'bandsintown': None,  # No implementado aún
                    'setlistfm': services.get('setlistfm_service')
                }
                logger.info("✅ Servicios de conciertos obtenidos para calendario")
            except Exception as e:
                logger.error(f"❌ Error obteniendo servicios de conciertos: {e}")
                self.concert_services = {}

        return self.concert_services

    async def cal_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Comando /cal - Panel principal de generación de calendarios"""
        user_id = self._get_or_create_user_id(update)
        if not user_id:
            await update.message.reply_text(
                "❌ Primero debes registrarte con `/adduser <tu_nombre>`",
                parse_mode='Markdown'
            )
            return

        text = (
            "📅 *Generador de Calendarios*\n\n"
            "Selecciona qué tipo de calendario quieres generar:\n\n"
            "🎵 **Conciertos**: Incluye todos los conciertos de tus artistas seguidos\n"
            "💿 **Discos**: Incluye todos los próximos lanzamientos de álbumes\n\n"
            "Los archivos .ics generados son compatibles con Google Calendar, "
            "Apple Calendar, Outlook y la mayoría de aplicaciones de calendario."
        )

        keyboard = [
            [InlineKeyboardButton("🎵 Conciertos", callback_data=f"cal_concerts_{user_id}")],
            [InlineKeyboardButton("💿 Discos", callback_data=f"cal_releases_{user_id}")],
        ]

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

    async def cal_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Maneja los callbacks del generador de calendarios"""
        query = update.callback_query
        await query.answer()

        callback_data = query.data
        parts = callback_data.split("_")

        if len(parts) != 3 or parts[0] != "cal":
            await query.edit_message_text("❌ Error en el callback.")
            return

        action = parts[1]
        user_id = int(parts[2])

        # Verificar usuario
        if not self._verify_user(update, user_id):
            await query.edit_message_text("❌ Error de autenticación.")
            return

        try:
            if action == "concerts":
                await self._handle_concerts_calendar(query, user_id)
            elif action == "releases":
                await self._handle_releases_calendar(query, user_id)
            else:
                await query.edit_message_text("❌ Acción no reconocida.")

        except Exception as e:
            logger.error(f"Error en cal_callback_handler: {e}")
            await query.edit_message_text("❌ Error generando el calendario.")

    async def _handle_concerts_calendar(self, query, user_id: int):
        """Genera calendario de conciertos"""
        await query.edit_message_text("🔍 Obteniendo conciertos para generar calendario...")

        try:
            # Obtener servicios de conciertos
            concert_services = self._get_concert_services()

            if not concert_services:
                await query.edit_message_text(
                    "❌ No hay servicios de conciertos disponibles.\n"
                    "Contacta al administrador para configurar las APIs."
                )
                return

            # Obtener artistas seguidos
            followed_artists = self.db.get_user_followed_artists(user_id)
            if not followed_artists:
                await query.edit_message_text(
                    "📭 No tienes artistas seguidos.\n"
                    "Usa `/addartist <nombre>` para empezar a seguir artistas."
                )
                return

            # Obtener configuración de países del usuario
            from user_services import get_services
            services = get_services()

            # Obtener países del usuario usando UserServices
            from user_services import UserServices
            user_services_instance = UserServices(self.db)
            user_config = user_services_instance.get_user_services(user_id)
            user_countries = user_config.get('countries', {'ES'})

            logger.info(f"Países del usuario {user_id}: {user_countries}")

            # Obtener conciertos de todas las fuentes
            all_concerts = []
            processed_artists = 0
            total_artists = len(followed_artists)

            for artist in followed_artists:
                processed_artists += 1

                # Actualizar progreso cada 5 artistas
                if processed_artists % 5 == 0 or processed_artists == total_artists:
                    await query.edit_message_text(
                        f"🔍 Obteniendo conciertos... {processed_artists}/{total_artists}"
                    )

                artist_name = artist['name']
                artist_concerts = []

                # Buscar en todos los servicios disponibles
                for service_name, service in concert_services.items():
                    if not service:
                        continue

                    try:
                        if service_name == 'ticketmaster':
                            concerts, _ = service.search_concerts_global(artist_name)
                            # Los conciertos de Ticketmaster ya vienen con 'artist', 'source', etc.
                            artist_concerts.extend(concerts)

                        elif service_name == 'spotify':
                            # Spotify no tiene API de conciertos, skip
                            continue

                        elif service_name == 'setlistfm':
                            # Setlist.fm no tiene búsqueda de conciertos futuros, solo setlists pasados
                            continue

                        # Añadir aquí otros servicios según estén disponibles

                    except Exception as e:
                        logger.error(f"Error buscando conciertos en {service_name} para {artist_name}: {e}")
                        continue

                # Filtrar conciertos futuros y eliminar duplicados
                today = date.today()
                future_concerts = []
                seen_concerts = set()

                for concert in artist_concerts:
                    concert_date = concert.get('date', '')
                    if concert_date and len(concert_date) >= 10:
                        try:
                            concert_date_obj = datetime.strptime(concert_date[:10], '%Y-%m-%d').date()
                            if concert_date_obj >= today:
                                # Crear clave única para evitar duplicados
                                concert_key = (
                                    artist_name.lower(),
                                    concert.get('venue', '').lower(),
                                    concert.get('city', '').lower(),
                                    concert_date[:10]
                                )

                                if concert_key not in seen_concerts:
                                    seen_concerts.add(concert_key)
                                    future_concerts.append(concert)
                        except ValueError:
                            continue

                all_concerts.extend(future_concerts)

            # Filtrar conciertos por países del usuario
            if services.get('country_state_city'):
                # Usar el sistema de filtrado por países
                from apis.country_state_city import ArtistTrackerDatabaseExtended
                extended_db = ArtistTrackerDatabaseExtended(self.db.db_path, services['country_state_city'])
                filtered_concerts = extended_db.filter_concerts_by_countries(all_concerts, user_countries)
            else:
                # Filtrado básico por país
                filtered_concerts = []
                for concert in all_concerts:
                    concert_country = concert.get('country', '').upper()
                    if not concert_country or concert_country in {c.upper() for c in user_countries}:
                        filtered_concerts.append(concert)

            logger.info(f"Conciertos después del filtrado: {len(filtered_concerts)} de {len(all_concerts)} originales")

            if not filtered_concerts:
                countries_text = ", ".join(sorted(user_countries))
                await query.edit_message_text(
                    f"📭 No se encontraron conciertos futuros para tus artistas seguidos en tus países configurados ({countries_text})."
                )
                return

            # Generar archivo ICS
            await query.edit_message_text(f"📅 Generando calendario con {len(filtered_concerts)} conciertos...")

            ics_content = self._generate_concerts_ics(filtered_concerts)

            # Crear archivo temporal
            with tempfile.NamedTemporaryFile(mode='w', suffix='.ics', delete=False, encoding='utf-8') as temp_file:
                temp_file.write(ics_content)
                temp_file_path = temp_file.name

            # Contar fuentes
            sources = {}
            for concert in filtered_concerts:
                source = concert.get('source', 'Desconocido')
                sources[source] = sources.get(source, 0) + 1

            sources_text = ", ".join([f"{source}: {count}" for source, count in sources.items()])
            countries_text = ", ".join(sorted(user_countries))

            # Enviar archivo
            with open(temp_file_path, 'rb') as file:
                await query.message.reply_document(
                    document=file,
                    filename=f"conciertos_{datetime.now().strftime('%Y%m%d')}.ics",
                    caption=(
                        f"📅 *Calendario de Conciertos*\n\n"
                        f"🎵 {len(filtered_concerts)} conciertos incluidos\n"
                        f"📊 De {len(followed_artists)} artistas seguidos\n"
                        f"🌍 Países: {countries_text}\n"
                        f"🔍 Fuentes: {sources_text}\n\n"
                        f"💡 Importa este archivo en tu aplicación de calendario favorita."
                    ),
                    parse_mode='Markdown'
                )

            # Limpiar archivo temporal
            os.unlink(temp_file_path)

            await query.edit_message_text(
                "✅ ¡Calendario de conciertos generado correctamente!\n"
                "Revisa el archivo ICS que te he enviado."
            )

        except Exception as e:
            logger.error(f"Error generando calendario de conciertos: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            await query.edit_message_text("❌ Error generando el calendario de conciertos.")

    async def _handle_releases_calendar(self, query, user_id: int):
        """Genera calendario de lanzamientos de discos"""
        await query.edit_message_text("🔍 Obteniendo lanzamientos para generar calendario...")

        try:
            all_releases = []

            # Obtener releases de Muspy si está configurado
            credentials = self.db.get_muspy_credentials(user_id)
            if credentials:
                email, password, userid = credentials
                muspy_releases, _ = self.muspy_service.get_user_releases(email, password, userid)

                if muspy_releases:
                    # Filtrar releases futuros
                    today = date.today().strftime("%Y-%m-%d")
                    future_releases = [r for r in muspy_releases if r.get('date', '0000-00-00') >= today]
                    all_releases.extend(future_releases)

            # También podrías añadir otras fuentes de releases aquí
            # (LastFM, Spotify, etc.)

            if not all_releases:
                message = "📭 No se encontraron próximos lanzamientos."
                if not credentials:
                    message += "\n\n💡 Configura tu cuenta de Muspy con `/muspy` para acceder a lanzamientos."
                await query.edit_message_text(message)
                return

            # Generar archivo ICS
            await query.edit_message_text(f"📅 Generando calendario con {len(all_releases)} lanzamientos...")

            ics_content = self._generate_releases_ics(all_releases)

            # Crear archivo temporal con encoding UTF-8
            with tempfile.NamedTemporaryFile(mode='w', suffix='.ics', delete=False, encoding='utf-8') as temp_file:
                temp_file.write(ics_content)
                temp_file_path = temp_file.name

            # Enviar archivo
            with open(temp_file_path, 'rb') as file:
                await query.message.reply_document(
                    document=file,
                    filename=f"lanzamientos_{datetime.now().strftime('%Y%m%d')}.ics",
                    caption=(
                        f"📅 *Calendario de Lanzamientos*\n\n"
                        f"💿 {len(all_releases)} lanzamientos incluidos\n\n"
                        f"💡 Importa este archivo en tu aplicación de calendario favorita.\n"
                        f"Los eventos son de todo el día para evitar problemas de zona horaria."
                    ),
                    parse_mode='Markdown'
                )

            # Limpiar archivo temporal
            os.unlink(temp_file_path)

            await query.edit_message_text(
                "✅ ¡Calendario de lanzamientos generado correctamente!\n"
                "Revisa el archivo ICS que te he enviado."
            )

        except Exception as e:
            logger.error(f"Error generando calendario de releases: {e}")
            await query.edit_message_text("❌ Error generando el calendario de lanzamientos.")

    def _get_artist_name_from_concert(self, concert: Dict, fallback_name: str = 'Artista desconocido') -> str:
        """Extrae el nombre del artista de un concierto probando diferentes campos"""
        # Probar diferentes campos donde puede estar el nombre del artista
        # Ticketmaster usa 'artist', otros servicios pueden usar 'artist_name', 'name', etc.
        possible_fields = ['artist', 'artist_name', 'name', 'performer', 'headliner']

        for field in possible_fields:
            if field in concert and concert[field]:
                return concert[field]

        return fallback_name

    def _generate_concerts_ics(self, concerts: List[Dict]) -> str:
        """Genera contenido ICS para conciertos de la base de datos"""
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Concert Bot//Concert Calendar//EN",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Conciertos",
            "X-WR-CALDESC:Calendario de conciertos de artistas seguidos"
        ]

        for concert in concerts:
            event_id = f"concert-{concert.get('id', hash(str(concert)))}"
            artist_name = self._get_artist_name_from_concert(concert, 'Artista desconocido')
            venue = concert.get('venue', 'Venue desconocido')
            city = concert.get('city', '')
            country = concert.get('country', '')
            date_str = concert.get('date', '')
            time_str = concert.get('time', '')

            # Construir título del evento
            title = f"{artist_name}"

            # Construir ubicación
            location_parts = [venue]
            if city:
                location_parts.append(city)
            if country:
                location_parts.append(country)
            location = ", ".join(location_parts)

            # Construir descripción
            description = f"Concierto de {artist_name}"
            if venue != 'Venue desconocido':
                description += f" en {venue}"
            if city:
                description += f", {city}"

            # Añadir fuente
            source = concert.get('source', '')
            if source:
                description += f"\\n\\nFuente: {source}"

            # URL si está disponible
            url = concert.get('url', '')
            if url:
                description += f"\\n\\nMás información: {url}"

            # Parsear fecha y hora
            if date_str and len(date_str) >= 10:
                try:
                    # Formato de fecha
                    date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')

                    # Determinar si tenemos hora válida
                    has_valid_time = False
                    start_datetime = None
                    end_datetime = None

                    if time_str and time_str.strip():
                        # Intentar parsear hora si está disponible
                        try:
                            # Manejar diferentes formatos de hora
                            if len(time_str) == 8:  # HH:MM:SS
                                time_obj = datetime.strptime(time_str, '%H:%M:%S').time()
                            elif len(time_str) == 5:  # HH:MM
                                time_obj = datetime.strptime(time_str, '%H:%M').time()
                            else:
                                # Intentar formato HH:MM:SS como fallback
                                time_obj = datetime.strptime(time_str, '%H:%M:%S').time()

                            start_datetime = datetime.combine(date_obj.date(), time_obj)
                            end_datetime = start_datetime + timedelta(hours=3)  # Duración estimada de 3 horas
                            has_valid_time = True

                        except ValueError:
                            # Si no se puede parsear la hora, usar evento de todo el día
                            has_valid_time = False

                    # Crear el evento según si tenemos hora o no
                    if has_valid_time and start_datetime and end_datetime:
                        # Evento con hora específica
                        dtstart = f"DTSTART:{start_datetime.strftime('%Y%m%dT%H%M%S')}"
                        dtend = f"DTEND:{end_datetime.strftime('%Y%m%dT%H%M%S')}"
                    else:
                        # Evento de todo el día solo si no hay hora válida
                        dtstart = f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}"
                        dtend = f"DTEND;VALUE=DATE:{(date_obj + timedelta(days=1)).strftime('%Y%m%d')}"

                    # Crear evento (solo UNO, no dos)
                    event_lines = [
                        "BEGIN:VEVENT",
                        f"UID:{event_id}@concertbot.local",
                        f"SUMMARY:{self._escape_ics_text(title)}",
                        f"DESCRIPTION:{self._escape_ics_text(description)}",
                        f"LOCATION:{self._escape_ics_text(location)}",
                        dtstart,
                        dtend,
                        f"DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}",
                        "STATUS:CONFIRMED",
                        "CATEGORIES:Concierto",
                        "END:VEVENT"
                    ]

                    ics_lines.extend(event_lines)

                except ValueError as e:
                    logger.error(f"Error parseando fecha {date_str}: {e}")
                    continue

        ics_lines.append("END:VCALENDAR")
        return "\r\n".join(ics_lines)

    def _generate_releases_ics(self, releases: List[Dict]) -> str:
        """Genera contenido ICS para lanzamientos (eventos de todo el día)"""
        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Concert Bot//Releases Calendar//EN",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Lanzamientos",
            "X-WR-CALDESC:Calendario de lanzamientos de álbumes y discos"
        ]

        for release in releases:
            event_id = f"release-{release.get('id', hash(str(release)))}"
            artist_name = self.muspy_service.extract_artist_name(release) if self.muspy_service else release.get('artist', 'Artista desconocido')
            title = self.muspy_service.extract_title(release) if self.muspy_service else release.get('title', 'Lanzamiento')
            release_type = self.muspy_service.extract_release_type(release) if self.muspy_service else release.get('type', 'Release')
            date_str = release.get('date', '')

            # Construir título del evento
            event_title = f"🎵 {artist_name} - {title}"
            if release_type and release_type != 'Release':
                event_title += f" ({release_type})"

            # Construir descripción
            description = f"Lanzamiento de {release_type.lower()} de {artist_name}"
            description += f"\\n\\nTítulo: {title}"
            if release_type:
                description += f"\\nTipo: {release_type}"

            # Parsear fecha
            if date_str and len(date_str) >= 10:
                try:
                    date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')

                    # Evento de todo el día
                    dtstart = f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}"
                    dtend = f"DTEND;VALUE=DATE:{(date_obj + timedelta(days=1)).strftime('%Y%m%d')}"

                    # Crear evento
                    ics_lines.extend([
                        "BEGIN:VEVENT",
                        f"UID:{event_id}@concertbot.local",
                        f"SUMMARY:{self._escape_ics_text(event_title)}",
                        f"DESCRIPTION:{self._escape_ics_text(description)}",
                        dtstart,
                        dtend,
                        f"DTSTAMP:{datetime.now().strftime('%Y%m%dT%H%M%SZ')}",
                        "STATUS:CONFIRMED",
                        "CATEGORIES:Lanzamiento,Música",
                        "END:VEVENT"
                    ])

                except ValueError as e:
                    logger.error(f"Error parseando fecha {date_str}: {e}")
                    continue

        ics_lines.append("END:VCALENDAR")
        return "\r\n".join(ics_lines)

    def _escape_ics_text(self, text: str) -> str:
        """Escapa texto para formato ICS"""
        if not text:
            return ""

        # Escapar caracteres especiales según RFC 5545
        text = text.replace('\\', '\\\\')  # Backslash primero
        text = text.replace('\n', '\\n')   # Saltos de línea
        text = text.replace('\r', '')      # Remover retornos de carro
        text = text.replace(',', '\\,')    # Comas
        text = text.replace(';', '\\;')    # Punto y coma
        text = text.replace('"', '\\"')    # Comillas

        return text

    def _get_or_create_user_id(self, update: Update) -> Optional[int]:
        """Obtiene el user_id del usuario actual"""
        if hasattr(update, 'callback_query') and update.callback_query:
            chat_id = update.callback_query.message.chat_id
        else:
            chat_id = update.effective_chat.id

        user = self.db.get_user_by_chat_id(chat_id)
        return user['id'] if user else None

    def _verify_user(self, update: Update, expected_user_id: int) -> bool:
        """Verifica que el usuario sea el esperado"""
        actual_user_id = self._get_or_create_user_id(update)
        return actual_user_id == expected_user_id
