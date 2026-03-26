#!/usr/bin/env python3
"""
Bot de Telegram para el sistema de seguimiento de artistas
Contiene todos los handlers de comandos y callbacks específicos de Telegram
"""

import os
import logging
import asyncio
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import traceback

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler
)



# Importar módulos propios
# APIS
from apis.muspy_service import MuspyService
from apis.country_state_city import CountryCityService
# MÓDULOS
from database import ArtistTrackerDatabase
from user_services import UserServices, initialize_concert_services, initialize_country_service, initialize_lastfm_service, validate_services, get_services
from concert_search import search_concerts_for_artist, format_concerts_message, format_single_artist_concerts_complete, split_long_message
# cal
from handlers.calendar_handlers import CalendarHandlers
# muspy
from handlers.muspy_handlers import MuspyHandlers, MUSPY_EMAIL, MUSPY_PASSWORD, MUSPY_USERID
# telegram handlers
from handlers.handlers_helpers import (
handle_notification_callback, handle_country_callback, handle_service_callback,
handle_lastfm_period_selection, handle_lastfm_do_sync, handle_lastfm_change_limit, handle_lastfm_change_user,
handle_spotify_authentication, handle_spotify_real_artists, handle_spotify_show_artists,
handle_spotify_add_artists, handle_spotify_change_limit, handle_spotify_change_user,
show_artists_page, show_artists_without_pagination, show_lastfm_artists_page, show_spotify_artists_page,
extract_auth_code_from_input, escape_markdown_v2, handle_spotify_playlists, show_spotify_playlists_page, handle_spotify_playlist_view,
show_spotify_playlist_artists_page, handle_spotify_playlist_follow_all
)

# Configuración de logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Variables globales
db = None
user_services = None
application = None
muspy_service = None
muspy_handlers = None
calendar_handlers = None

async def spotify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /spotify - gestión de sincronización con Spotify - VERSIÓN CORREGIDA"""
    services = get_services()

    # DIAGNÓSTICO DETALLADO
    spotify_service = services.get('spotify_service')

    if not spotify_service:
        # Verificar por qué no está disponible
        client_id = os.getenv('SPOTIFY_CLIENT_ID')
        client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')

        error_details = []
        if not client_id:
            error_details.append("❌ SPOTIFY_CLIENT_ID no configurado")
        if not client_secret:
            error_details.append("❌ SPOTIFY_CLIENT_SECRET no configurado")

        if error_details:
            await update.message.reply_text(
                "❌ *Servicio de Spotify no disponible*\n\n"
                "*Problemas encontrados:*\n" + "\n".join(error_details) + "\n\n"
                "*Para solucionarlo:*\n"
                "1. Crea una aplicación en https://developer.spotify.com\n"
                "2. Configura las variables de entorno:\n"
                "   `SPOTIFY_CLIENT_ID=tu_client_id`\n"
                "   `SPOTIFY_CLIENT_SECRET=tu_client_secret`\n"
                "3. Reinicia el bot\n\n"
                "💡 *Estado actual:*\n"
                f"Client ID: {'✅ Configurado' if client_id else '❌ Falta'}\n"
                f"Client Secret: {'✅ Configurado' if client_secret else '❌ Falta'}",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "❌ *Servicio de Spotify no disponible*\n\n"
                "Las credenciales están configuradas pero el servicio no se inicializó.\n"
                "Posibles causas:\n"
                "• El archivo `apis/spotify.py` no existe\n"
                "• Error en la inicialización del servicio\n"
                "• Problemas de importación\n\n"
                "Revisa los logs del bot para más detalles.",
                parse_mode='Markdown'
            )
        return

    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Verificar si ya tiene usuario de Spotify configurado
    spotify_user = db.get_user_spotify(user['id'])

    if not spotify_user:
        # No tiene usuario configurado, pedirlo
        context.user_data['waiting_for_spotify_user'] = user['id']
        await show_spotify_setup(update, user, context)
    else:
        # Ya tiene usuario, mostrar menú principal
        await show_spotify_menu(update, user, spotify_user)




async def show_spotify_setup(update, user: Dict, context = None):
    """Muestra el setup inicial de Spotify con autenticación OAuth"""
    message = (
        "🎵 *Configuración de Spotify*\n\n"
        "Para acceder a tus artistas seguidos y poder seguir nuevos artistas, "
        "necesitas autenticarte con tu cuenta de Spotify.\n\n"
        "Selecciona cómo quieres proceder:"
    )

    keyboard = [
        [InlineKeyboardButton("🔐 Autenticación completa", callback_data=f"spotify_auth_{user['id']}")],
        [InlineKeyboardButton("👤 Solo nombre de usuario", callback_data=f"spotify_username_{user['id']}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"spotify_cancel_{user['id']}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


async def show_spotify_menu(update, user: Dict, spotify_user: Dict):
    """Muestra el menú principal de Spotify con botón de Playlists - VERSIÓN ACTUALIZADA"""
    services = get_services()

    username = spotify_user['spotify_username']
    display_name = spotify_user.get('spotify_display_name', username)
    followers = spotify_user.get('spotify_followers', 0)
    playlists = spotify_user.get('spotify_playlists', 0)
    artists_limit = spotify_user.get('artists_limit', 20)

    # Verificar estado del servicio
    user_services_config = user_services.get_user_services(user['id'])
    service_status = "✅ Activado" if user_services_config.get('spotify', True) else "❌ Desactivado"

    # Verificar autenticación OAuth
    is_authenticated = services['spotify_service'].is_user_authenticated(user['id']) if services.get('spotify_service') else False
    auth_status = "🔐 Autenticado" if is_authenticated else "👤 Solo usuario"

    message = (
        f"🎵 *Spotify - {display_name}*\n\n"
        f"👤 Usuario: @{username}\n"
        f"👥 Seguidores: {followers:,}\n"
        f"🎵 Playlists: {playlists}\n"
        f"🔢 Límite de artistas: {artists_limit}\n"
        f"⚙️ Estado del servicio: {service_status}\n"
        f"🔐 Autenticación: {auth_status}\n\n"
        f"Selecciona una opción:"
    )

    # Botones según el estado de autenticación
    if is_authenticated:
        keyboard = [
            [
                InlineKeyboardButton("🎵 Artistas seguidos", callback_data=f"spotify_real_artists_{user['id']}"),
                InlineKeyboardButton("📋 Playlists", callback_data=f"spotify_playlists_{user['id']}")
            ],
            [
                InlineKeyboardButton("➕ Añadir artistas", callback_data=f"spotify_add_{user['id']}"),
                InlineKeyboardButton("🔢 Cambiar cantidad", callback_data=f"spotify_limit_{user['id']}")
            ],
            [
                InlineKeyboardButton("🔗 Seguir en Spotify", callback_data=f"spotify_follow_{user['id']}"),
                InlineKeyboardButton("👤 Cambiar usuario", callback_data=f"spotify_changeuser_{user['id']}")
            ],
            [
                InlineKeyboardButton("🚫 Revocar acceso", callback_data=f"spotify_revoke_{user['id']}")
            ]
        ]
    else:
        keyboard = [
            [
                InlineKeyboardButton("🔐 Autenticar cuenta", callback_data=f"spotify_auth_{user['id']}"),
                InlineKeyboardButton("🎵 Mostrar artistas", callback_data=f"spotify_artists_{user['id']}")
            ],
            [
                InlineKeyboardButton("🔢 Cambiar cantidad", callback_data=f"spotify_limit_{user['id']}"),
                InlineKeyboardButton("👤 Cambiar usuario", callback_data=f"spotify_changeuser_{user['id']}")
            ]
        ]

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Verificar si es callback o mensaje normal
    try:
        if hasattr(update, 'callback_query') and update.callback_query:
            await update.callback_query.edit_message_text(
                message,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                message,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
    except Exception as e:
        logger.error(f"Error mostrando menú Spotify: {e}")



# ===========================
# CALLBACK HANDLERS
# ===========================

async def artist_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la selección de artistas desde los botones inline"""
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id

    if query.data == "cancel_artist_selection":
        db.clear_pending_selection(chat_id)
        await query.edit_message_text("❌ Selección de artista cancelada.")
        return

    # Extraer índice del artista seleccionado
    if not query.data.startswith("select_artist_"):
        return

    try:
        artist_index = int(query.data.split("_")[-1])
    except ValueError:
        await query.edit_message_text("❌ Error en la selección. Inténtalo de nuevo.")
        return

    # Obtener selección pendiente
    pending_data = db.get_pending_selection(chat_id)
    if not pending_data:
        await query.edit_message_text(
            "❌ No hay selección pendiente. Usa `/addartist` para buscar un artista."
        )
        return

    candidates, original_query = pending_data

    if artist_index >= len(candidates):
        await query.edit_message_text("❌ Selección inválida. Inténtalo de nuevo.")
        return

    selected_candidate = candidates[artist_index]

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await query.edit_message_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Crear el artista y añadirlo a seguimiento
    await query.edit_message_text(f"⏳ Añadiendo '{selected_candidate['name']}'...")

    artist_id = db.create_artist_from_candidate(selected_candidate)

    if not artist_id:
        await query.edit_message_text(
            f"❌ Error al añadir el artista '{selected_candidate['name']}'. Inténtalo de nuevo."
        )
        return

    was_new = db.add_followed_artist(user['id'], artist_id)

    # Limpiar selección pendiente
    db.clear_pending_selection(chat_id)

    if was_new:
        await query.edit_message_text(
            f"✅ ¡Ahora sigues a '{selected_candidate['name']}'! 🎵\n"
            f"Usa `/list` para ver todos tus artistas seguidos."
        )
    else:
        await query.edit_message_text(
            f"ℹ️ Ya seguías a '{selected_candidate['name']}'."
        )


async def country_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la selección de países desde los botones inline"""
    query = update.callback_query
    await query.answer()

    chat_id = query.message.chat_id

    if query.data == "cancel_country_selection":
        db.clear_pending_selection(chat_id)
        await query.edit_message_text("❌ Selección de país cancelada.")
        return

    # Extraer índice del país seleccionado
    if not query.data.startswith("select_country_"):
        return

    try:
        country_index = int(query.data.split("_")[-1])
    except ValueError:
        await query.edit_message_text("❌ Error en la selección. Inténtalo de nuevo.")
        return

    # Obtener selección pendiente
    pending_data = db.get_pending_selection(chat_id)
    if not pending_data:
        await query.edit_message_text(
            "❌ No hay selección pendiente. Usa `/addcountry` para buscar un país."
        )
        return

    countries, original_query = pending_data

    if country_index >= len(countries):
        await query.edit_message_text("❌ Selección inválida. Inténtalo de nuevo.")
        return

    selected_country = countries[country_index]

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await query.edit_message_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Añadir país
    await query.edit_message_text(
        f"⏳ Añadiendo país {selected_country['name']} ({selected_country['code']})...\n"
        f"Esto puede tardar un momento mientras obtenemos las ciudades..."
    )

    try:
        success = country_state_city.add_user_country(user['id'], selected_country['code'])

        # Limpiar selección pendiente
        db.clear_pending_selection(chat_id)

        if success:
            # Obtener estadísticas
            cities = country_state_city.get_country_cities(selected_country['code'])
            user_countries = country_state_city.get_user_countries(user['id'])

            await query.edit_message_text(
                f"✅ País añadido: {selected_country['name']} ({selected_country['code']})\n"
                f"🏙️ {len(cities)} ciudades cargadas\n"
                f"🌍 Total países configurados: {len(user_countries)}\n\n"
                f"Usa `/mycountries` para ver tu configuración actual."
            )
        else:
            await query.edit_message_text(
                f"❌ Error añadiendo el país {selected_country['name']}.\n"
                f"Es posible que ya lo tengas configurado."
            )
    except Exception as e:
        logger.error(f"Error añadiendo país: {e}")
        await query.edit_message_text(
            "❌ Error al añadir el país. Inténtalo de nuevo más tarde."
        )


async def expand_concerts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la expansión de conciertos desde los botones inline - VERSIÓN CORREGIDA"""
    query = update.callback_query
    await query.answer()

    try:
        # Parsear el callback data
        parts = query.data.split("_")

        if query.data.startswith("expand_all_"):
            # Expandir todos los conciertos
            user_id = int(parts[2])

            # Obtener datos del caché
            cache_data = get_user_search_cache(user_id)
            if not cache_data:
                await query.edit_message_text(
                    "❌ La búsqueda ha expirado. Usa `/search` para buscar de nuevo."
                )
                return

            search_type, concerts = cache_data

            # Mostrar todos los conciertos sin límite usando el formato original
            message = format_expanded_concerts_message_original(concerts, "🎵 Todos los conciertos encontrados")

            # Dividir en chunks si es muy largo
            if len(message) > 4000:
                chunks = split_long_message(message)

                # Editar el mensaje original con el primer chunk
                await query.edit_message_text(
                    chunks[0],
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )

                # Enviar chunks adicionales
                for chunk in chunks[1:]:
                    await query.message.reply_text(
                        chunk,
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
            else:
                await query.edit_message_text(
                    message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )

        elif query.data.startswith("expand_artist_"):
            # Expandir conciertos de un artista específico
            user_id = int(parts[-1])  # Último elemento es el user_id
            artist_name = "_".join(parts[2:-1])  # Todo entre "expand_artist" y user_id

            # Obtener datos del caché
            cache_data = get_user_search_cache(user_id)
            if not cache_data:
                await query.edit_message_text(
                    "❌ La búsqueda ha expirado. Usa `/search` para buscar de nuevo."
                )
                return

            search_type, all_concerts = cache_data

            # Filtrar conciertos del artista específico
            artist_concerts = [c for c in all_concerts if c.get('artist_name', '') == artist_name]

            if not artist_concerts:
                await query.edit_message_text(
                    f"❌ No se encontraron conciertos para {artist_name}"
                )
                return

            # Mostrar todos los conciertos del artista usando formato original
            message = format_single_artist_concerts_complete(artist_concerts, artist_name, show_notified=True)

            # Botón para volver
            keyboard = [[
                InlineKeyboardButton("🔙 Volver a la búsqueda", callback_data=f"back_to_search_{user_id}")
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await query.edit_message_text(
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )

        elif query.data.startswith("back_to_search_"):
            # Volver a la búsqueda original
            user_id = int(parts[3])

            # Obtener datos del caché
            cache_data = get_user_search_cache(user_id)
            if not cache_data:
                await query.edit_message_text(
                    "❌ La búsqueda ha expirado. Usa `/search` para buscar de nuevo."
                )
                return

            search_type, concerts = cache_data

            # Determinar el título basado en el tipo de búsqueda
            if search_type == "user_concerts":
                title = "🎵 Conciertos de tus artistas seguidos"
                show_notified = True
            else:
                # Extraer nombre del artista del tipo de búsqueda
                artist_name = search_type.replace("artist_search_", "")
                title = f"🎵 Conciertos de {artist_name}"
                show_notified = False

            # Volver al formato resumido ORIGINAL
            message, keyboard = format_concerts_message(
                concerts,
                title,
                show_notified=show_notified,
                show_expand_buttons=True,
                user_id=user_id
            )

            await query.edit_message_text(
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=keyboard
            )

    except Exception as e:
        logger.error(f"Error en expand_concerts_callback: {e}")
        await query.edit_message_text(
            "❌ Error al expandir conciertos. Inténtalo de nuevo."
        )



async def show_artist_concerts_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la selección de mostrar conciertos de un artista desde botones inline"""
    query = update.callback_query
    await query.answer()

    try:
        # Parsear el callback data: show_artist_concerts_ARTISTNAME_USERID
        parts = query.data.split("_")
        if not query.data.startswith("show_artist_concerts_"):
            return

        user_id = int(parts[-1])  # Último elemento es el user_id
        encoded_artist = "_".join(parts[3:-1])  # Todo entre "show_artist_concerts" y user_id

        # Decodificar el nombre del artista
        artist_name = encoded_artist.replace("__", "-").replace("_", " ")

        # Obtener todos los conciertos del usuario
        all_concerts = db.get_all_concerts_for_user(user_id)

        # Filtrar conciertos del artista específico
        artist_concerts = [c for c in all_concerts if c.get('artist_name', '').lower() == artist_name.lower()]

        if not artist_concerts:
            await query.edit_message_text(
                f"❌ No se encontraron conciertos para {artist_name}"
            )
            return

        # Formatear mensaje detallado
        message = format_artist_concerts_detailed(artist_concerts, artist_name, show_notified=False)

        # Botón para volver
        keyboard = [[
            InlineKeyboardButton("🔙 Volver al resumen", callback_data=f"back_to_summary_{user_id}")
        ]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Dividir en chunks si es muy largo
        if len(message) > 4000:
            chunks = split_long_message(message)

            # Editar mensaje original con el primer chunk
            await query.edit_message_text(
                chunks[0],
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )

            # Enviar chunks adicionales
            for chunk in chunks[1:]:
                await query.message.reply_text(
                    chunk,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
        else:
            await query.edit_message_text(
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )

    except Exception as e:
        logger.error(f"Error en show_artist_concerts_callback: {e}")
        await query.edit_message_text(
            "❌ Error al mostrar conciertos del artista. Inténtalo de nuevo."
        )


async def back_to_summary_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja el botón de volver al resumen"""
    query = update.callback_query
    await query.answer()

    try:
        # Parsear el callback data: back_to_summary_USERID
        user_id = int(query.data.split("_")[-1])

        # Obtener datos del caché o regenerar
        cache_data = get_user_search_cache(user_id)
        if not cache_data:
            await query.edit_message_text(
                "❌ La búsqueda ha expirado. Usa `/search` para buscar de nuevo."
            )
            return

        search_type, concerts = cache_data

        # Determinar el título basado en el tipo de búsqueda
        if search_type == "user_concerts":
            title = "🎵 Conciertos de tus artistas seguidos"
            show_notified = True
        else:
            # Extraer nombre del artista del tipo de búsqueda
            artist_name = search_type.replace("artist_search_", "")
            title = f"🎵 Conciertos de {artist_name}"
            show_notified = False

        # Volver al formato resumido
        message, keyboard = format_concerts_message(
            concerts,
            title,
            show_notified=show_notified,
            show_expand_buttons=True,
            user_id=user_id
        )

        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=keyboard
        )

    except Exception as e:
        logger.error(f"Error en back_to_summary_callback: {e}")
        await query.edit_message_text(
            "❌ Error al volver al resumen. Usa `/search` para buscar de nuevo."
        )


async def continent_selection_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la selección de continentes y muestra todos los países"""
    query = update.callback_query
    await query.answer()

    if not query.data.startswith("continent_"):
        return

    continent_code = query.data.replace("continent_", "")

    if not country_state_city:
        await query.edit_message_text("❌ Servicio de países no disponible.")
        return

    # Mensaje de estado
    await query.edit_message_text("🔍 Cargando países del continente...")

    try:
        # Obtener todos los países
        countries = country_state_city.get_available_countries()

        if not countries:
            await query.edit_message_text(
                "❌ No se pudieron obtener los países."
            )
            return

        # Definir mapeo de países por continente (más completo)
        continent_countries = {
            'europe': [
                'AD', 'AL', 'AT', 'BA', 'BE', 'BG', 'BY', 'CH', 'CY', 'CZ', 'DE', 'DK', 'EE', 'ES', 'FI', 'FR',
                'GB', 'GE', 'GR', 'HR', 'HU', 'IE', 'IS', 'IT', 'LI', 'LT', 'LU', 'LV', 'MC', 'MD', 'ME', 'MK',
                'MT', 'NL', 'NO', 'PL', 'PT', 'RO', 'RS', 'RU', 'SE', 'SI', 'SK', 'SM', 'UA', 'VA', 'XK'
            ],
            'north_america': [
                'AG', 'BB', 'BZ', 'CA', 'CR', 'CU', 'DM', 'DO', 'GD', 'GT', 'HN', 'HT', 'JM', 'KN', 'LC',
                'MX', 'NI', 'PA', 'SV', 'TT', 'US', 'VC'
            ],
            'south_america': [
                'AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'FK', 'GF', 'GY', 'PE', 'PY', 'SR', 'UY', 'VE'
            ],
            'asia': [
                'AE', 'AF', 'AM', 'AZ', 'BD', 'BH', 'BN', 'BT', 'CN', 'ID', 'IL', 'IN', 'IQ', 'IR', 'JO',
                'JP', 'KG', 'KH', 'KP', 'KR', 'KW', 'KZ', 'LA', 'LB', 'LK', 'MM', 'MN', 'MO', 'MV', 'MY',
                'NP', 'OM', 'PH', 'PK', 'PS', 'QA', 'SA', 'SG', 'SY', 'TH', 'TJ', 'TL', 'TM', 'TR', 'TW',
                'UZ', 'VN', 'YE'
            ],
            'oceania': [
                'AS', 'AU', 'CK', 'FJ', 'FM', 'GU', 'KI', 'MH', 'MP', 'NC', 'NR', 'NU', 'NZ', 'PF', 'PG',
                'PN', 'PW', 'SB', 'TK', 'TO', 'TV', 'VU', 'WF', 'WS'
            ],
            'africa': [
                'AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'DZ', 'EG', 'EH',
                'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE', 'KM', 'LR', 'LS', 'LY', 'MA', 'MG',
                'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE', 'NG', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'SS',
                'ST', 'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'ZA', 'ZM', 'ZW'
            ]
        }

        # Determinar qué países mostrar
        if continent_code == "all":
            selected_countries = countries
            continent_name = "Todos los continentes"
            continent_emoji = "🌍"
        else:
            # Filtrar países del continente seleccionado
            continent_codes = continent_countries.get(continent_code, [])

            # Crear diccionario de países por código para búsqueda rápida
            countries_by_code = {}
            for country in countries:
                code = country.get('iso2', country.get('code', ''))
                if code:
                    countries_by_code[code] = country

            # Filtrar países del continente
            selected_countries = []
            for code in continent_codes:
                if code in countries_by_code:
                    selected_countries.append(countries_by_code[code])

            # Obtener nombre y emoji del continente
            continent_info = {
                'europe': ('🇪🇺', 'Europa'),
                'north_america': ('🇺🇸', 'América del Norte'),
                'south_america': ('🇧🇷', 'América del Sur'),
                'asia': ('🇨🇳', 'Asia'),
                'oceania': ('🇦🇺', 'Oceanía'),
                'africa': ('🇿🇦', 'África'),
                'others': ('🌍', 'Otros')
            }

            continent_emoji, continent_name = continent_info.get(continent_code, ('🌍', 'Desconocido'))

        # Si hay países para otros continentes, añadirlos a "others"
        if continent_code == "others":
            all_continent_codes = set()
            for codes in continent_countries.values():
                all_continent_codes.update(codes)

            countries_by_code = {country.get('iso2', country.get('code', '')): country for country in countries}
            selected_countries = [country for code, country in countries_by_code.items()
                                if code and code not in all_continent_codes]

        if not selected_countries:
            await query.edit_message_text(
                f"❌ No se encontraron países para {continent_name}.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Volver a continentes", callback_data="back_to_continents")
                ]])
            )
            return

        # Ordenar países alfabéticamente
        selected_countries.sort(key=lambda x: x.get('name', ''))

        # Crear mensaje con todos los países del continente
        message_lines = [
            f"{continent_emoji} *Países de {continent_name}*\n",
            f"📊 Total: {len(selected_countries)} países\n"
        ]

        # Mostrar TODOS los países (sin límites)
        for i, country in enumerate(selected_countries, 1):
            code = country.get('iso2', country.get('code', ''))
            name = country.get('name', 'Nombre desconocido')

            # Información adicional si está disponible
            details = []
            if country.get('phonecode'):
                details.append(f"+{country['phonecode']}")
            if country.get('currency'):
                details.append(f"{country['currency']}")

            line = f"{i:2d}. *{code}* - {name}"
            if details:
                line += f" ({' | '.join(details)})"

            message_lines.append(line)

        message_lines.append(f"\n💡 *Uso:* `/addcountry <código>` o `/addcountry <nombre>`")

        response = "\n".join(message_lines)

        # Botón para volver
        keyboard = [[InlineKeyboardButton("🔙 Volver a continentes", callback_data="back_to_continents")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # Dividir en chunks si es muy largo
        if len(response) > 4000:
            chunks = split_long_message(response, max_length=4000)

            # Editar mensaje original con el primer chunk
            await query.edit_message_text(
                chunks[0],
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

            # Enviar chunks adicionales
            for chunk in chunks[1:]:
                await query.message.reply_text(
                    chunk,
                    parse_mode='Markdown'
                )
        else:
            await query.edit_message_text(
                response,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )

    except Exception as e:
        logger.error(f"Error mostrando países del continente: {e}")
        await query.edit_message_text(
            "❌ Error cargando países. Inténtalo de nuevo.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Volver a continentes", callback_data="back_to_continents")
            ]])
        )


async def back_to_continents_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Vuelve al menú de selección de continentes"""
    query = update.callback_query
    await query.answer()

    # Simular el comando listcountries original
    fake_update = type('obj', (object,), {
        'message': query.message
    })()

    fake_context = type('obj', (object,), {
        'args': []
    })()

    # Reutilizar la lógica del comando original
    if not country_state_city:
        await query.edit_message_text("❌ Servicio de países no disponible.")
        return

    try:
        # Obtener países para verificar disponibilidad
        countries = country_state_city.get_available_countries()

        if not countries:
            await query.edit_message_text(
                "❌ No se pudieron obtener los países disponibles."
            )
            return

        # Crear mensaje con botones de continentes
        message = (
            "🌍 *Países disponibles por continente*\n\n"
            f"📊 Total de países: {len(countries)}\n\n"
            "Selecciona un continente para ver todos sus países:"
        )

        # Definir continentes con emojis
        continents = [
            ("🇪🇺", "Europa", "europe"),
            ("🇺🇸", "América del Norte", "north_america"),
            ("🇧🇷", "América del Sur", "south_america"),
            ("🇨🇳", "Asia", "asia"),
            ("🇦🇺", "Oceanía", "oceania"),
            ("🇿🇦", "África", "africa"),
            ("🌍", "Otros", "others")
        ]

        # Crear teclado con botones de continentes
        keyboard = []
        for emoji, name, code in continents:
            keyboard.append([InlineKeyboardButton(f"{emoji} {name}", callback_data=f"continent_{code}")])

        # Botón para ver todos los países de una vez
        keyboard.append([InlineKeyboardButton("📋 Ver todos los países", callback_data="continent_all")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Error volviendo a continentes: {e}")
        await query.edit_message_text(
            "❌ Error al cargar continentes."
        )


async def list_page_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la navegación de páginas en el comando /list"""
    query = update.callback_query
    await query.answer()

    # Parsear callback data: list_page_PAGE_USERID
    try:
        parts = query.data.split("_")
        if len(parts) != 4 or parts[0] != "list" or parts[1] != "page":
            return

        page = int(parts[2])
        user_id = int(parts[3])

        # Obtener datos de la paginación
        pagination_data = db.get_list_pagination_data(user_id)
        if not pagination_data:
            await query.edit_message_text(
                "❌ Los datos han expirado. Usa `/list` de nuevo."
            )
            return

        followed_artists, display_name = pagination_data

        # Mostrar página solicitada
        fake_update = type('obj', (object,), {'callback_query': query, 'message': query.message})()
        response, keyboard = await show_artists_page(fake_update, user_id, followed_artists, display_name, page, edit_message=True)

        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

        await query.edit_message_text(
            response,
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=reply_markup
        )

    except (ValueError, IndexError) as e:
        logger.error(f"Error en callback de paginación: {e}")
        await query.edit_message_text(
            "❌ Error en la navegación. Usa `/list` de nuevo."
        )


async def config_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks del sistema de configuración - VERSIÓN CORREGIDA"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    logger.info(f"Config callback recibido: {callback_data}")

    # Parsear callback data
    parts = callback_data.split("_")
    if len(parts) < 3:
        await query.edit_message_text("❌ Error en el callback.")
        return

    prefix = parts[0]  # 'config', 'notif', 'country', 'service', 'artist'
    action = parts[1]

    # Obtener user_id del final
    try:
        user_id = int(parts[-1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Error de usuario.")
        return

    # Verificar que el usuario existe
    user = db.get_user_by_chat_id(query.message.chat_id)
    if not user or user['id'] != user_id:
        await query.edit_message_text("❌ Error de autenticación.")
        return

    services = get_services()

    try:
        # Manejar según el prefijo
        if prefix == "config":
            if action == "notifications":
                await show_notifications_menu(query, user)
            elif action == "countries":
                await show_countries_menu(query, user, services)
            elif action == "services":
                await show_services_menu(query, user)
            elif action == "artists":
                await show_artists_menu(query, user)
            elif action == "refresh" or action == "back":
                # Actualizar la configuración
                updated_user = db.get_user_by_chat_id(query.message.chat_id)
                fake_update = type('obj', (object,), {'callback_query': query})()
                await show_config_menu(fake_update, updated_user, edit_message=True)

        elif prefix == "notif":
            message, keyboard = await handle_notification_callback(query, action, user_id, context, user_services)
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"])] for btn in keyboard[0]]) if keyboard else None
            await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

        elif prefix == "country":
            message, keyboard = await handle_country_callback(query, action, user_id, parts, context, services)
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"])] for btn in keyboard[0]]) if keyboard else None
            await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

        elif prefix == "service":
            message, keyboard = await handle_service_callback(query, action, user_id, parts, user_services)
            reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"])] for btn in keyboard[0]]) if keyboard else None
            await query.edit_message_text(message, reply_markup=reply_markup)

        # CORRECCIÓN: Añadir manejo de callbacks de artistas
        elif prefix == "artist":
            if action == "add":
                # Solicitar nombre de artista
                message = (
                    "➕ *Añadir artista*\n\n"
                    "Envía el nombre del artista que quieres seguir.\n"
                    "Ejemplo: Radiohead\n\n"
                    "Responde a este mensaje con el nombre del artista."
                )
                context.user_data['waiting_for_artist_add'] = user_id
                keyboard = [[InlineKeyboardButton("❌ Cancelar", callback_data=f"config_artists_{user_id}")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

            elif action == "search":
                # Redirigir a búsqueda de conciertos
                await query.edit_message_text(
                    "🔍 *Buscar conciertos*\n\n"
                    "Usa `/search` para buscar nuevos conciertos de tus artistas seguidos.\n"
                    "Usa `/show` para ver conciertos ya guardados en base de datos."
                )
            else:
                await query.edit_message_text("❌ Acción de artista no reconocida.")

        else:
            await query.edit_message_text("❌ Acción no reconocida.")

    except Exception as e:
        logger.error(f"Error en config_callback_handler: {e}")
        await query.edit_message_text("❌ Error procesando la solicitud.")


# CORRECCIÓN CON DEBUG para Last.fm

# ===========================
# 1. CORRECCIÓN en handle_lastfm_period_selection en handlers_helpers.py
# ===========================

async def handle_lastfm_period_selection(query, user: Dict, period: str, services, database):
    """Maneja la selección de período de Last.fm - VERSIÓN CON DEBUG"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== handle_lastfm_period_selection: usuario {user['id']}, período {period} ===")

    lastfm_service = services.get('lastfm_service')

    if not lastfm_service:
        await query.edit_message_text("❌ Servicio de Last.fm no disponible.")
        return

    # Obtener usuario de Last.fm
    lastfm_user = database.get_user_lastfm(user['id'])
    if not lastfm_user:
        await query.edit_message_text("❌ No tienes usuario de Last.fm configurado.")
        return

    username = lastfm_user['lastfm_username']
    sync_limit = lastfm_user.get('sync_limit', 20)

    logger.info(f"Usuario: {username}, límite: {sync_limit}")

    # Mensaje de estado
    period_name = lastfm_service.get_period_display_name(period)
    await query.edit_message_text(
        f"🔍 Obteniendo top artistas de {username} ({period_name})...\n"
        f"Esto puede tardar un momento."
    )

    try:
        logger.info("Llamando a get_top_artists...")
        # Obtener artistas de Last.fm
        artists, status_message = lastfm_service.get_top_artists(username, period, sync_limit)

        logger.info(f"Resultado get_top_artists: {len(artists)} artistas")
        logger.info(f"Status: {status_message}")
        logger.info(f"Primeros 3 artistas: {[a.get('name', 'sin nombre') for a in artists[:3]]}")

        if not artists:
            logger.warning("No se encontraron artistas")
            await query.edit_message_text(
                f"📭 No se encontraron artistas para el período {period_name}.\n"
                f"Estado: {status_message}"
            )
            return

        logger.info("Guardando selección pendiente...")
        # Guardar selección pendiente
        save_result = database.save_pending_lastfm_sync(user['id'], period, artists)
        logger.info(f"Selección guardada: {save_result}")

        logger.info("Mostrando primera página...")
        # Mostrar primera página - IMPORTAR FUNCIÓN AQUÍ
        from handlers_helpers import show_lastfm_artists_page
        await show_lastfm_artists_page(query, user, period, artists, page=0, services=services)
        logger.info("Página mostrada correctamente")

    except Exception as e:
        logger.error(f"Error obteniendo artistas de Last.fm: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")

        await query.edit_message_text(
            f"❌ Error obteniendo artistas de {username}.\n"
            f"Error: {str(e)}\n"
            f"Inténtalo de nuevo más tarde."
        )

# ===========================
# 2. CORRECCIÓN en show_lastfm_artists_page en handlers_helpers.py
# ===========================

async def show_lastfm_artists_page(query, user: Dict, period: str, artists: List[Dict],
                                  page: int = 0, services: Dict = None):
    """Muestra una página de artistas de Last.fm con paginación - VERSIÓN CON DEBUG"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== show_lastfm_artists_page: {len(artists)} artistas, página {page}, período {period} ===")

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    lastfm_service = services.get('lastfm_service') if services else None

    artists_per_page = 15
    total_pages = (len(artists) + artists_per_page - 1) // artists_per_page

    if page >= total_pages:
        page = total_pages - 1
    elif page < 0:
        page = 0

    start_idx = page * artists_per_page
    end_idx = min(start_idx + artists_per_page, len(artists))
    page_artists = artists[start_idx:end_idx]

    logger.info(f"Mostrando artistas {start_idx}-{end_idx} de {len(artists)}")

    # Obtener nombre del período
    period_name = lastfm_service.get_period_display_name(period) if lastfm_service else period

    # Obtener username de la base de datos
    try:
        from database import ArtistTrackerDatabase
        temp_db = ArtistTrackerDatabase()
        lastfm_user = temp_db.get_user_lastfm(user['id'])
        username = lastfm_user['lastfm_username'] if lastfm_user else user.get('lastfm_username', 'Usuario')
    except Exception as e:
        logger.error(f"Error obteniendo username: {e}")
        username = user.get('lastfm_username', 'Usuario')

    logger.info(f"Username: {username}, período: {period_name}")

    # Construir texto
    message_lines = [
        f"🎵 *Top artistas de {username}*",
        f"📊 Período: {period_name}",
        f"🔢 Total encontrados: {len(artists)} artistas",
        f"📄 Página {page + 1} de {total_pages}\n"
    ]

    # Contar artistas con MBID en esta página
    mbid_count = sum(1 for artist in page_artists if artist.get("mbid"))

    for i, artist in enumerate(page_artists, start_idx + 1):
        playcount = artist.get("playcount", 0)
        name = artist.get("name", "Nombre desconocido")
        mbid = artist.get("mbid", "")

        # Escapar caracteres especiales para Markdown
        safe_name = name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        line = f"{i}. *{safe_name}*"

        # Añadir información de reproducción
        if playcount > 0:
            line += f" ({playcount:,} reproducciones)"

        # Indicar si tiene MBID
        if mbid:
            line += " 🎵"

        # Añadir géneros si están disponibles
        genres = artist.get("genres", [])
        if genres:
            genre_text = ", ".join(genres[:2])
            line += f" _{genre_text}_"

        message_lines.append(line)

    message_lines.append("")
    message_lines.append(f"🎵 {mbid_count}/{len(page_artists)} artistas con MBID para sincronización precisa")

    # Crear botones
    keyboard = []
    nav_buttons = []

    # Botón anterior
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            "⬅️ Anterior",
            callback_data=f"lastfm_page_{period}_{page-1}_{user['id']}"
        ))

    # Botón de página actual
    nav_buttons.append(InlineKeyboardButton(
        f"📄 {page + 1}/{total_pages}",
        callback_data="current_lastfm_page"
    ))

    # Botón siguiente
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            "Siguiente ➡️",
            callback_data=f"lastfm_page_{period}_{page+1}_{user['id']}"
        ))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Botón para confirmar sincronización
    keyboard.append([InlineKeyboardButton(
        "✅ Sincronizar todos",
        callback_data=f"lastfm_sync_{period}_{user['id']}"
    )])

    # Botón para cancelar
    keyboard.append([InlineKeyboardButton(
        "❌ Cancelar",
        callback_data=f"lastfm_cancel_{user['id']}"
    )])

    message = "\n".join(message_lines)

    logger.info(f"Mensaje preparado: {len(message)} caracteres")
    logger.info(f"Teclado: {len(keyboard)} filas de botones")

    # Actualizar mensaje
    try:
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info("Mensaje de Last.fm enviado correctamente")
    except Exception as e:
        logger.error(f"Error enviando mensaje de Last.fm: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await query.edit_message_text("❌ Error mostrando artistas de Last.fm.")

# ===========================
# 3. VERIFICAR en lastfm_callback_handler en telegram_bot.py
# ===========================

# Asegúrate de que esta función esté en telegram_bot.py:
async def lastfm_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks de Last.fm - VERSIÓN CON DEBUG"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    logger.info(f"Last.fm callback recibido: {callback_data}")

    # Parsear callback data
    parts = callback_data.split("_")
    if len(parts) < 3 or parts[0] != "lastfm":
        await query.edit_message_text("❌ Callback no válido.")
        return

    action = parts[1]

    # Obtener user_id del final
    try:
        user_id = int(parts[-1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Error de usuario.")
        return

    # Verificar que el usuario existe
    user = db.get_user_by_chat_id(query.message.chat_id)
    if not user or user['id'] != user_id:
        await query.edit_message_text("❌ Error de autenticación.")
        return

    services = get_services()

    try:
        if action == "cancel":
            await query.edit_message_text("❌ Configuración de Last.fm cancelada.")

        elif action == "period":
            period = parts[2] if len(parts) > 2 else ""
            logger.info(f"Manejando período: {period}")
            await handle_lastfm_period_selection(query, user, period, services, db)

        elif action == "page":
            # Manejar paginación
            period = parts[2] if len(parts) > 2 else ""
            page = int(parts[3]) if len(parts) > 3 else 0

            logger.info(f"Manejando página: período={period}, página={page}")

            # Obtener artistas pendientes
            artists = db.get_pending_lastfm_sync(user['id'], period) if db else []
            if artists:
                await show_lastfm_artists_page(query, user, period, artists, page, services)
            else:
                await query.edit_message_text("❌ No hay datos de artistas disponibles.")

        elif callback_data == "current_lastfm_page":
            # No hacer nada si presiona el botón de página actual
            return

        elif action == "sync":
            period = parts[2] if len(parts) > 2 else ""
            logger.info(f"Manejando sincronización: período={period}")

            message, keyboard = await handle_lastfm_do_sync(query, user, period, db, services)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "limit":
            message, keyboard = await handle_lastfm_change_limit(query, user, context)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "changeuser":
            message, keyboard = await handle_lastfm_change_user(query, user, context)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "menu":
            # Volver al menú principal de Last.fm
            lastfm_user = db.get_user_lastfm(user['id']) if db else None
            if lastfm_user:
                # Crear fake_update para show_lastfm_menu
                fake_update = type('obj', (object,), {
                    'message': query.message,
                    'callback_query': query
                })()
                await show_lastfm_menu(fake_update, user, lastfm_user)
            else:
                await query.edit_message_text("❌ No tienes usuario de Last.fm configurado.")

        else:
            await query.edit_message_text("❌ Acción no reconocida.")

    except Exception as e:
        logger.error(f"Error en lastfm_callback_handler: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await query.edit_message_text("❌ Error procesando la solicitud.")



async def show_country_selection(update, countries: List[Dict], original_query: str, message_to_edit):
    """Muestra una lista de países para que el usuario elija - FUNCIÓN FALTANTE"""
    chat_id = update.effective_chat.id

    # Guardar países para posterior selección
    db.save_pending_selection(chat_id, countries, original_query)

    # Crear mensaje con opciones
    message_lines = [f"🌍 *Encontré varios países para '{original_query}':*\n"]

    keyboard = []
    for i, country in enumerate(countries[:8]):  # Limitar a 8 opciones
        # Formatear información del país
        country_name = country.get('name', 'Desconocido')
        country_code = country.get('code', '')

        info_parts = []
        if country.get('currency'):
            info_parts.append(f"💰 {country['currency']}")
        if country.get('phone_code'):
            info_parts.append(f"📞 +{country['phone_code']}")

        info_text = " • ".join(info_parts) if info_parts else ""

        option_text = f"{i+1}. *{country_name}* ({country_code})"
        if info_text:
            option_text += f"\n   _{info_text}_"

        message_lines.append(option_text)

        # Botón para esta opción
        button_text = f"{i+1}. {country_name}"
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."

        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_country_{i}")])

    # Botón de cancelar
    keyboard.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel_country_selection")])

    message_lines.append("\n*Selecciona el país correcto:*")

    reply_markup = InlineKeyboardMarkup(keyboard)
    response = "\n".join(message_lines)

    try:
        await message_to_edit.edit_text(
            response,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error mostrando selección de países: {e}")
        await message_to_edit.edit_text("❌ Error mostrando países.")





async def spotify_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks de Spotify con OAuth - VERSIÓN CORREGIDA"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    logger.info(f"Spotify callback recibido: {callback_data}")

    # Parsear callback data
    parts = callback_data.split("_")
    if len(parts) < 3 or parts[0] != "spotify":
        await query.edit_message_text("❌ Callback no válido.")
        return

    action = parts[1]

    # Obtener user_id del final
    try:
        user_id = int(parts[-1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Error de usuario.")
        return

    # Verificar que el usuario existe
    user = db.get_user_by_chat_id(query.message.chat_id)
    if not user or user['id'] != user_id:
        await query.edit_message_text("❌ Error de autenticación.")
        return

    services = get_services()
    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    try:
        if action == "cancel":
            await query.edit_message_text("❌ Configuración de Spotify cancelada.")

        elif action == "auth":
            # Marcar que estamos esperando código OAuth
            context.user_data['waiting_for_spotify_code'] = user['id']
            message, keyboard, auth_url = await handle_spotify_authentication(query, user, services)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup, disable_web_page_preview=False)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "username":
            # Configuración solo con nombre de usuario (modo limitado)
            context.user_data['waiting_for_spotify_user'] = user['id']
            await show_spotify_username_setup(query, user)

        elif action == "real" and len(parts) > 2 and parts[2] == "artists":
            await handle_spotify_real_artists(query, user, services, db)

        elif action == "artists":
            await handle_spotify_show_artists(query, user, services, db)

        elif action == "add":
            message, keyboard = await handle_spotify_add_artists(query, user, db)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "limit":
            message, keyboard = await handle_spotify_change_limit(query, user, context)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "changeuser":
            message, keyboard = await handle_spotify_change_user(query, user, context)
            if keyboard:
                reply_markup = InlineKeyboardMarkup(keyboard)
                await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)
            else:
                await query.edit_message_text(message, parse_mode='Markdown')

        elif action == "page":
            period = parts[2] if len(parts) > 2 else ""
            page = int(parts[3]) if len(parts) > 3 else 0

            artists = db.get_pending_spotify_artists(user['id']) if db else []
            if artists:
                await show_spotify_artists_page(query, user, artists, page, services=services)
            else:
                await query.edit_message_text("❌ No hay datos de artistas disponibles.")

        elif action == "real" and len(parts) > 2 and parts[2] == "page":
            page = int(parts[3]) if len(parts) > 3 else 0

            artists = db.get_pending_spotify_artists(user['id']) if db else []
            if artists:
                await show_spotify_artists_page(query, user, artists, page, is_real=True, services=services)
            else:
                await query.edit_message_text("❌ No hay datos de artistas disponibles.")

        # CORRECCIÓN CRÍTICA: Manejo del callback "menu"
        elif action == "menu":
            # Volver al menú principal de Spotify
            spotify_user = db.get_user_spotify(user['id']) if db else None
            if spotify_user:
                # CORRECCIÓN: Crear un objeto fake_update para show_spotify_menu
                fake_update = type('obj', (object,), {
                    'message': query.message,
                    'callback_query': query
                })()
                await show_spotify_menu(fake_update, user, spotify_user)
            else:
                await query.edit_message_text("❌ No tienes usuario de Spotify configurado.")

        # CORRECCIÓN CRÍTICA: Añadir manejo de otros callbacks faltantes
        elif action == "revoke":
            # Revocar autenticación
            if spotify_service.revoke_user_authentication(user['id']):
                await query.edit_message_text(
                    "✅ Autenticación revocada correctamente.\n"
                    "Tendrás que autenticarte de nuevo para acceder a tus artistas seguidos."
                )
            else:
                await query.edit_message_text("❌ Error al revocar autenticación.")

        elif action == "follow":
            # Funcionalidad de seguir artistas en Spotify
            await query.edit_message_text(
                "🔗 *Seguir artistas en Spotify*\n\n"
                "Esta funcionalidad permitirá seguir automáticamente en Spotify "
                "los artistas que añadas al bot.\n\n"
                "⚠️ En desarrollo..."
            )

        elif action == "playlists":
            if len(parts) > 2 and parts[2] == "page":
                # Paginación de playlists
                page = int(parts[3]) if len(parts) > 3 else 0
                playlists = db.get_pending_playlists(user['id']) if db else []
                if playlists:
                    await show_spotify_playlists_page(query, user, playlists, page, services)
                else:
                    await query.edit_message_text("❌ No hay datos de playlists disponibles.")
            else:
                # Mostrar playlists
                await handle_spotify_playlists(query, user, services, db)

        elif action == "playlist":
            if parts[2] == "view":
                # Ver playlist específica
                playlist_id = parts[3] if len(parts) > 3 else ""
                await handle_spotify_playlist_view(query, user, playlist_id, services, db)
            elif parts[2] == "artists" and parts[3] == "page":
                # Paginación de artistas de playlist
                playlist_id = parts[4] if len(parts) > 4 else ""
                page = int(parts[5]) if len(parts) > 5 else 0

                playlist_data = db.get_pending_playlist_artists(user['id'], playlist_id)
                if playlist_data:
                    # Obtener info de la playlist desde las playlists guardadas
                    playlists = db.get_pending_playlists(user['id'])
                    playlist_info = next((p for p in playlists if p.get('id') == playlist_id), {}) if playlists else {}

                    await show_spotify_playlist_artists_page(
                        query, user, playlist_id, playlist_info,
                        playlist_data['artists'], page, services
                    )
                else:
                    await query.edit_message_text("❌ No hay datos de artistas de playlist disponibles.")
            elif parts[2] == "follow" and parts[3] == "all":
                # Seguir todos los artistas de una playlist
                playlist_id = parts[4] if len(parts) > 4 else ""
                await handle_spotify_playlist_follow_all(query, user, playlist_id, db)


        else:
            await query.edit_message_text("❌ Acción no reconocida.")



    except Exception as e:
        logger.error(f"Error en spotify_callback_handler: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await query.edit_message_text("❌ Error procesando la solicitud.")




async def playlist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /playlist - añade artistas de una playlist de Spotify por URL"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar la URL de la playlist.\n"
            "Ejemplo: `/playlist https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M`\n\n"
            "💡 Para obtener la URL:\n"
            "1. Abre Spotify\n"
            "2. Ve a la playlist\n"
            "3. Clic en '...' → 'Compartir' → 'Copiar enlace de la playlist'\n"
            "4. Pega el enlace aquí",
            parse_mode='Markdown'
        )
        return

    playlist_url = context.args[0]
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    services = get_services()
    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await update.message.reply_text(
            "❌ Servicio de Spotify no disponible.\n"
            "Contacta al administrador para configurar las credenciales."
        )
        return

    # Verificar autenticación
    if not spotify_service.is_user_authenticated(user['id']):
        await update.message.reply_text(
            "❌ No estás autenticado con Spotify.\n"
            "Usa `/spotify` y autentícate primero."
        )
        return

    # Validar URL
    import re
    if not re.search(r'open\.spotify\.com/playlist/[a-zA-Z0-9]+', playlist_url):
        await update.message.reply_text(
            "❌ URL de playlist inválida.\n"
            "Debe ser una URL de Spotify como:\n"
            "`https://open.spotify.com/playlist/XXXXXXXXX`",
            parse_mode='Markdown'
        )
        return

    # Mensaje de estado
    status_message = await update.message.reply_text(
        f"🔍 Analizando playlist de Spotify...\n"
        f"Obteniendo información..."
    )

    try:
        # Obtener información de la playlist
        playlist_info, status = spotify_service.get_playlist_by_url(user['id'], playlist_url)

        if not playlist_info:
            await status_message.edit_text(
                f"❌ No se pudo obtener información de la playlist.\n"
                f"Estado: {status}\n\n"
                f"💡 Verifica que:\n"
                f"• La URL sea correcta\n"
                f"• La playlist sea pública o tengas acceso\n"
                f"• Estés autenticado correctamente"
            )
            return

        playlist_name = playlist_info.get('name', 'Playlist')
        tracks_total = playlist_info.get('tracks_total', 0)
        playlist_id = playlist_info.get('id', '')

        await status_message.edit_text(
            f"✅ Playlist encontrada: *{playlist_name}*\n"
            f"📊 Total de canciones: {tracks_total}\n\n"
            f"🔍 Obteniendo artistas...",
            parse_mode='Markdown'
        )

        # Obtener artistas de la playlist
        artists, artist_status = spotify_service.get_playlist_tracks(user['id'], playlist_id)

        if not artists:
            await status_message.edit_text(
                f"📭 No se encontraron artistas en la playlist '{playlist_name}'.\n"
                f"Estado: {artist_status}"
            )
            return

        await status_message.edit_text(
            f"✅ Playlist: *{playlist_name}*\n"
            f"🎤 Artistas únicos encontrados: {len(artists)}\n\n"
            f"⏳ Añadiendo artistas a tu lista de seguimiento...\n"
            f"Esto puede tardar un momento.",
            parse_mode='Markdown'
        )

        # Añadir artistas a la base de datos
        added_count = 0
        skipped_count = 0
        error_count = 0

        for i, artist_data in enumerate(artists, 1):
            artist_name = artist_data.get('name', '')

            # Actualizar progreso cada 5 artistas
            if i % 5 == 0 or i == len(artists):
                progress_msg = (
                    f"⏳ Añadiendo artistas de '{playlist_name}'...\n"
                    f"Progreso: {i}/{len(artists)}\n"
                    f"✅ Añadidos: {added_count} | ⏭️ Ya seguidos: {skipped_count} | ❌ Errores: {error_count}"
                )
                try:
                    await status_message.edit_text(progress_msg)
                except:
                    pass

            if not artist_name:
                error_count += 1
                continue

            try:
                # Buscar candidatos en MusicBrainz
                candidates = db.search_artist_candidates(artist_name)

                if not candidates:
                    skipped_count += 1
                    continue

                # Usar el mejor candidato
                best_candidate = candidates[0]
                artist_id = db.create_artist_from_candidate(best_candidate)

                if not artist_id:
                    error_count += 1
                    continue

                # Añadir a seguimiento
                was_new = db.add_followed_artist(user['id'], artist_id)

                if was_new:
                    added_count += 1
                else:
                    skipped_count += 1

                # Pausa breve
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error procesando artista {artist_name}: {e}")
                error_count += 1
                continue

        # Mensaje de resultado final
        message = (
            f"✅ *Playlist sincronizada*\n\n"
            f"🎵 Playlist: {playlist_name}\n"
            f"🎤 Artistas procesados: {len(artists)}\n"
            f"➕ Nuevos artistas añadidos: {added_count}\n"
            f"⏭️ Ya seguías: {skipped_count}\n"
        )

        if error_count > 0:
            message += f"❌ Errores: {error_count}\n"

        # Calcular porcentaje de éxito
        success_rate = ((added_count + skipped_count) / len(artists)) * 100 if artists else 0
        message += f"📈 Tasa de éxito: {success_rate:.1f}%\n"

        message += (
            f"\n💡 *Comandos útiles:*\n"
            f"• `/list` - Ver todos tus artistas seguidos\n"
            f"• `/search` - Buscar conciertos de tus artistas\n"
            f"• `/spotify` - Gestionar más playlists"
        )

        await status_message.edit_text(message, parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error en comando playlist: {e}")
        await status_message.edit_text(
            f"❌ Error procesando la playlist.\n"
            f"Error: {str(e)}\n\n"
            f"💡 Inténtalo de nuevo o contacta al administrador."
        )



async def show_spotify_username_setup(query, user: Dict):
    """Muestra setup solo para nombre de usuario (modo limitado)"""
    message = (
        "👤 *Configuración básica de Spotify*\n\n"
        "Este modo te permite ver artistas simulados y configuración básica, "
        "pero no podrás acceder a tus artistas realmente seguidos.\n\n"
        "Envía tu nombre de usuario de Spotify:"
    )

    keyboard = [
        [InlineKeyboardButton("🔐 Mejor usar autenticación completa", callback_data=f"spotify_auth_{user['id']}")],
        [InlineKeyboardButton("❌ Cancelar", callback_data=f"spotify_cancel_{user['id']}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

# ===========================
# HANDLER DE ENTRADA DE TEXTO
# ===========================

async def handle_text_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja la entrada de texto cuando se espera configuración"""
    logger.info(f"DEBUG: handle_text_input llamado con user_data: {context.user_data}")
    services = get_services()

    # PRIORIDAD 1: Cambio de hora de notificación
    if 'waiting_for_time' in context.user_data:
        user_id = context.user_data['waiting_for_time']
        time_str = update.message.text.strip()

        try:
            datetime.strptime(time_str, '%H:%M')

            if user_services.set_notification_time(user_id, time_str):
                await update.message.reply_text(
                    f"✅ Hora de notificación cambiada a {time_str}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a configuración", callback_data=f"config_back_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al cambiar la hora.")
        except ValueError:
            await update.message.reply_text("❌ Formato inválido. Usa HH:MM (ejemplo: 09:00)")

        del context.user_data['waiting_for_time']
        return

    # PRIORIDAD 2: Añadir país
    elif 'waiting_for_country_add' in context.user_data:
        user_id = context.user_data['waiting_for_country_add']
        country_input = update.message.text.strip()

        if services.get('country_state_city'):
            if len(country_input) == 2 and country_input.isalpha():
                country_code = country_input.upper()
                success = services['country_state_city'].add_user_country(user_id, country_code)
            else:
                matching_countries = services['country_state_city'].search_countries(country_input)
                if len(matching_countries) == 1:
                    success = services['country_state_city'].add_user_country(user_id, matching_countries[0]['code'])
                else:
                    await update.message.reply_text("❌ País no encontrado o ambiguo. Usa el código de 2 letras.")
                    del context.user_data['waiting_for_country_add']
                    return

            if success:
                await update.message.reply_text(
                    f"✅ País añadido correctamente",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a configuración", callback_data=f"config_back_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al añadir el país o ya lo tienes configurado.")
        else:
            await update.message.reply_text("❌ Sistema de países múltiples no disponible.")

        del context.user_data['waiting_for_country_add']
        return

    # PRIORIDAD 3: Usuario de Last.fm
    elif 'waiting_for_lastfm_user' in context.user_data:
        user_id = context.user_data['waiting_for_lastfm_user']
        lastfm_username = update.message.text.strip()

        if not lastfm_username:
            await update.message.reply_text("❌ Nombre de usuario no válido.")
            del context.user_data['waiting_for_lastfm_user']
            return

        if not services.get('lastfm_service'):
            await update.message.reply_text("❌ Servicio de Last.fm no disponible.")
            del context.user_data['waiting_for_lastfm_user']
            return

        status_message = await update.message.reply_text(f"🔍 Verificando usuario '{lastfm_username}'...")

        try:
            if not services['lastfm_service'].check_user_exists(lastfm_username):
                await status_message.edit_text(
                    f"❌ El usuario '{lastfm_username}' no existe en Last.fm.\n"
                    f"Verifica el nombre e inténtalo de nuevo."
                )
                del context.user_data['waiting_for_lastfm_user']
                return

            user_info = services['lastfm_service'].get_user_info(lastfm_username)

            if db.set_user_lastfm(user_id, lastfm_username, user_info):
                message = f"✅ Usuario de Last.fm configurado: {lastfm_username}"
                if user_info and user_info.get('playcount', 0) > 0:
                    message += f"\n📊 Reproducciones: {user_info['playcount']:,}"

                await status_message.edit_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎵 Abrir Last.fm", callback_data=f"lastfm_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text("❌ Error al configurar el usuario de Last.fm.")

        except Exception as e:
            logger.error(f"Error configurando usuario Last.fm: {e}")
            await status_message.edit_text("❌ Error verificando el usuario. Inténtalo de nuevo.")

        del context.user_data['waiting_for_lastfm_user']
        return

    # PRIORIDAD 4: Usuario de Spotify con código OAuth
    elif 'waiting_for_spotify_code' in context.user_data:
        user_id = context.user_data['waiting_for_spotify_code']
        user_input = update.message.text.strip()

        if not user_input:
            await update.message.reply_text("❌ Entrada no válida.")
            del context.user_data['waiting_for_spotify_code']
            return

        if not services.get('spotify_service'):
            await update.message.reply_text("❌ Servicio de Spotify no disponible.")
            del context.user_data['waiting_for_spotify_code']
            return

        status_message = await update.message.reply_text("🔄 Procesando autorización...")

        try:
            authorization_code = extract_auth_code_from_input(user_input)

            if not authorization_code:
                await status_message.edit_text(
                    "❌ No se pudo extraer el código de autorización.\n\n"
                    "Envía:\n"
                    "• La URL completa de redirección\n"
                    "• Solo el código (parte después de 'code=')\n"
                    "• Si la página muestra 'Authorization successful', copia todo el texto"
                )
                del context.user_data['waiting_for_spotify_code']
                return

            success, message_text, user_info = services['spotify_service'].process_authorization_code(user_id, authorization_code)

            if success:
                spotify_username = user_info.get('spotify_id', 'unknown')
                db.set_user_spotify(user_id, spotify_username, user_info)

                success_message = (
                    f"✅ *¡Autenticación exitosa!*\n\n"
                    f"👤 Usuario: {user_info.get('display_name', spotify_username)}\n"
                    f"🆔 ID: {spotify_username}\n"
                    f"👥 Seguidores: {user_info.get('followers', 0):,}\n"
                    f"🎵 Playlists: {user_info.get('public_playlists', 0)}\n"
                    f"🌍 País: {user_info.get('country', 'No especificado')}\n"
                    f"💎 Tipo: {user_info.get('product', 'free').title()}\n\n"
                    f"Ahora puedes acceder a todas las funciones de Spotify."
                )

                await status_message.edit_text(
                    success_message,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎵 Abrir Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text(
                    f"❌ Error en autenticación:\n{message_text}\n\n"
                    f"💡 **Consejos:**\n"
                    f"• Verifica que copiaste el código completo\n"
                    f"• El código expira en 10 minutos\n"
                    f"• Intenta generar una nueva URL con `/spotify`"
                )

        except Exception as e:
            logger.error(f"Error procesando código OAuth: {e}")
            await status_message.edit_text(
                "❌ Error procesando el código.\n\n"
                "🔄 Intenta de nuevo:\n"
                "1. Ve a `/spotify`\n"
                "2. Genera nueva URL de autorización\n"
                "3. Copia el código completo"
            )

        del context.user_data['waiting_for_spotify_code']
        return

    # PRIORIDAD 5: Cambio de límites (Last.fm/Spotify)
    elif 'waiting_for_lastfm_limit' in context.user_data:
        user_id = context.user_data['waiting_for_lastfm_limit']
        limit_text = update.message.text.strip()

        try:
            limit = int(limit_text)

            if limit < 5 or limit > 10000:
                await update.message.reply_text("❌ El límite debe estar entre 5 y 10000 artistas.")
                del context.user_data['waiting_for_lastfm_limit']
                return

            if db.set_lastfm_sync_limit(user_id, limit):
                await update.message.reply_text(
                    f"✅ Límite de sincronización establecido a {limit} artistas.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Last.fm", callback_data=f"lastfm_menu_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al establecer el límite.")

        except ValueError:
            await update.message.reply_text("❌ Debes enviar un número válido.")

        del context.user_data['waiting_for_lastfm_limit']
        return

    elif 'waiting_for_spotify_limit' in context.user_data:
        user_id = context.user_data['waiting_for_spotify_limit']
        limit_text = update.message.text.strip()

        try:
            limit = int(limit_text)

            if limit < 5 or limit > 10000:
                await update.message.reply_text("❌ El límite debe estar entre 5 y 10000 artistas.")
                del context.user_data['waiting_for_spotify_limit']
                return

            if db.set_spotify_artists_limit(user_id, limit):
                await update.message.reply_text(
                    f"✅ Límite de artistas establecido a {limit}.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al establecer el límite.")

        except ValueError:
            await update.message.reply_text("❌ Debes enviar un número válido.")

        del context.user_data['waiting_for_spotify_limit']
        return


    # PRIORIDAD 4: Cambio de usuario de Last.fm
    elif 'waiting_for_lastfm_change_user' in context.user_data:
        # Procesar cambio de usuario de Last.fm
        user_id = context.user_data['waiting_for_lastfm_change_user']
        lastfm_username = update.message.text.strip()

        if not lastfm_username:
            await update.message.reply_text("❌ Nombre de usuario no válido.")
            del context.user_data['waiting_for_lastfm_change_user']
            return

        if not lastfm_service:
            await update.message.reply_text("❌ Servicio de Last.fm no disponible.")
            del context.user_data['waiting_for_lastfm_change_user']
            return

        # Verificar usuario
        status_message = await update.message.reply_text(f"🔍 Verificando usuario '{lastfm_username}'...")

        try:
            if not lastfm_service.check_user_exists(lastfm_username):
                await status_message.edit_text(
                    f"❌ El usuario '{lastfm_username}' no existe en Last.fm.\n"
                    f"Verifica el nombre e inténtalo de nuevo."
                )
                del context.user_data['waiting_for_lastfm_change_user']
                return

            # Obtener información y actualizar
            user_info = lastfm_service.get_user_info(lastfm_username)

            if db.set_user_lastfm(user_id, lastfm_username, user_info):
                message = f"✅ Usuario de Last.fm actualizado: {lastfm_username}"
                if user_info and user_info.get('playcount', 0) > 0:
                    message += f"\n📊 Reproducciones: {user_info['playcount']:,}"

                await status_message.edit_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Last.fm", callback_data=f"lastfm_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text("❌ Error al actualizar el usuario de Last.fm.")

        except Exception as e:
            logger.error(f"Error actualizando usuario Last.fm: {e}")
            await status_message.edit_text("❌ Error verificando el usuario. Inténtalo de nuevo.")

        del context.user_data['waiting_for_lastfm_change_user']
        return

    # PRIORIDAD 5: Límite de Last.fm
    elif 'waiting_for_lastfm_limit' in context.user_data:
        # Procesar nuevo límite de Last.fm
        user_id = context.user_data['waiting_for_lastfm_limit']
        limit_text = update.message.text.strip()

        try:
            limit = int(limit_text)

            if limit < 5 or limit > 10000:
                await update.message.reply_text("❌ El límite debe estar entre 5 y 10000 artistas.")
                del context.user_data['waiting_for_lastfm_limit']
                return

            if db.set_lastfm_sync_limit(user_id, limit):
                await update.message.reply_text(
                    f"✅ Límite de sincronización establecido a {limit} artistas.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Last.fm", callback_data=f"lastfm_menu_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al establecer el límite.")

        except ValueError:
            await update.message.reply_text("❌ Debes enviar un número válido.")

        del context.user_data['waiting_for_lastfm_limit']
        return

    # PRIORIDAD 6: Añadir artista
    elif 'waiting_for_artist_add' in context.user_data:
        user_id = context.user_data['waiting_for_artist_add']
        artist_name = update.message.text.strip()

        if not artist_name:
            await update.message.reply_text("❌ Nombre de artista no válido.")
            del context.user_data['waiting_for_artist_add']
            return

        # Simular el comando addartist
        fake_context = type('obj', (object,), {
            'args': artist_name.split(),
            'user_data': context.user_data
        })()

        fake_update = type('obj', (object,), {
            'effective_chat': type('obj', (object,), {'id': update.effective_chat.id})(),
            'message': update.message
        })()

        # Limpiar el estado antes de llamar a addartist
        del context.user_data['waiting_for_artist_add']

        # Llamar al comando addartist existente
        await addartist_command(fake_update, fake_context)
        return


# PRIORIDAD: Usuario de Spotify
    elif 'waiting_for_spotify_user' in context.user_data:
        # Procesar nuevo usuario de Spotify
        user_id = context.user_data['waiting_for_spotify_user']
        spotify_username = update.message.text.strip()

        if not spotify_username:
            await update.message.reply_text("❌ Nombre de usuario no válido.")
            del context.user_data['waiting_for_spotify_user']
            return

        # Verificar que el servicio esté disponible
        if not spotify_service:
            await update.message.reply_text("❌ Servicio de Spotify no disponible.")
            del context.user_data['waiting_for_spotify_user']
            return

        # Verificar que el usuario existe en Spotify
        status_message = await update.message.reply_text(f"🔍 Verificando usuario '{spotify_username}'...")

        try:
            if not spotify_service.check_user_exists(spotify_username):
                await status_message.edit_text(
                    f"❌ El usuario '{spotify_username}' no existe en Spotify.\n"
                    f"Verifica el nombre e inténtalo de nuevo."
                )
                del context.user_data['waiting_for_spotify_user']
                return

            # Obtener información del usuario
            user_info = spotify_service.get_user_info(spotify_username)

            # Obtener número de playlists
            playlists_count = spotify_service.get_user_playlists_count(spotify_username)
            if user_info:
                user_info['public_playlists'] = playlists_count

            # Guardar en base de datos
            if db.set_user_spotify(user_id, spotify_username, user_info):
                message = f"✅ Usuario de Spotify configurado: {spotify_username}"
                if user_info:
                    display_name = user_info.get('display_name', spotify_username)
                    followers = user_info.get('followers', 0)
                    if display_name != spotify_username:
                        message += f" ({display_name})"
                    message += f"\n👥 Seguidores: {followers:,}"
                    message += f"\n🎵 Playlists: {playlists_count}"

                await status_message.edit_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎵 Abrir Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text("❌ Error al configurar el usuario de Spotify.")

        except Exception as e:
            logger.error(f"Error configurando usuario Spotify: {e}")
            await status_message.edit_text("❌ Error verificando el usuario. Inténtalo de nuevo.")

        del context.user_data['waiting_for_spotify_user']
        return

    # PRIORIDAD: Cambio de usuario de Spotify
    elif 'waiting_for_spotify_change_user' in context.user_data:
        # Procesar cambio de usuario de Spotify
        user_id = context.user_data['waiting_for_spotify_change_user']
        spotify_username = update.message.text.strip()

        if not spotify_username:
            await update.message.reply_text("❌ Nombre de usuario no válido.")
            del context.user_data['waiting_for_spotify_change_user']
            return

        if not spotify_service:
            await update.message.reply_text("❌ Servicio de Spotify no disponible.")
            del context.user_data['waiting_for_spotify_change_user']
            return

        # Verificar usuario
        status_message = await update.message.reply_text(f"🔍 Verificando usuario '{spotify_username}'...")

        try:
            if not spotify_service.check_user_exists(spotify_username):
                await status_message.edit_text(
                    f"❌ El usuario '{spotify_username}' no existe en Spotify.\n"
                    f"Verifica el nombre e inténtalo de nuevo."
                )
                del context.user_data['waiting_for_spotify_change_user']
                return

            # Obtener información y actualizar
            user_info = spotify_service.get_user_info(spotify_username)
            playlists_count = spotify_service.get_user_playlists_count(spotify_username)
            if user_info:
                user_info['public_playlists'] = playlists_count

            if db.set_user_spotify(user_id, spotify_username, user_info):
                message = f"✅ Usuario de Spotify actualizado: {spotify_username}"
                if user_info:
                    display_name = user_info.get('display_name', spotify_username)
                    followers = user_info.get('followers', 0)
                    if display_name != spotify_username:
                        message += f" ({display_name})"
                    message += f"\n👥 Seguidores: {followers:,}"
                    message += f"\n🎵 Playlists: {playlists_count}"

                await status_message.edit_text(
                    message,
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text("❌ Error al actualizar el usuario de Spotify.")

        except Exception as e:
            logger.error(f"Error actualizando usuario Spotify: {e}")
            await status_message.edit_text("❌ Error verificando el usuario. Inténtalo de nuevo.")

        del context.user_data['waiting_for_spotify_change_user']
        return

    # PRIORIDAD: Límite de Spotify
    elif 'waiting_for_spotify_limit' in context.user_data:
        user_id = context.user_data['waiting_for_spotify_limit']
        limit_text = update.message.text.strip()

        try:
            limit = int(limit_text)

            if limit < 5 or limit > 10000:
                await update.message.reply_text("❌ El límite debe estar entre 5 y 10000 artistas.")
                del context.user_data['waiting_for_spotify_limit']
                return

            if db.set_spotify_artists_limit(user_id, limit):
                await update.message.reply_text(
                    f"✅ Límite de artistas establecido a {limit}.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Volver a Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await update.message.reply_text("❌ Error al establecer el límite.")

        except ValueError:
            await update.message.reply_text("❌ Debes enviar un número válido.")

        del context.user_data['waiting_for_spotify_limit']
        return


    # PRIORIDAD MÁXIMA: Código de autorización OAuth de Spotify
    elif 'waiting_for_spotify_code' in context.user_data:
        # Procesar código de autorización OAuth
        user_id = context.user_data['waiting_for_spotify_code']
        user_input = update.message.text.strip()

        logger.info(f"DEBUG: Procesando input OAuth: {user_input[:20]}...")

        if not user_input:
            await update.message.reply_text("❌ Entrada no válida.")
            del context.user_data['waiting_for_spotify_code']
            return

        # Verificar que el servicio esté disponible
        if not spotify_service:
            await update.message.reply_text("❌ Servicio de Spotify no disponible.")
            del context.user_data['waiting_for_spotify_code']
            return

        # Procesar input - puede ser código o URL completa
        status_message = await update.message.reply_text("🔄 Procesando autorización...")

        try:
            # Extraer código de diferentes formatos posibles
            authorization_code = extract_auth_code_from_input(user_input)

            if not authorization_code:
                await status_message.edit_text(
                    "❌ No se pudo extraer el código de autorización.\n\n"
                    "Envía:\n"
                    "• La URL completa de redirección\n"
                    "• Solo el código (parte después de 'code=')\n"
                    "• Si la página muestra 'Authorization successful', copia todo el texto"
                )
                del context.user_data['waiting_for_spotify_code']
                return

            success, message_text, user_info = spotify_service.process_authorization_code(user_id, authorization_code)

            if success:
                # Actualizar información en base de datos
                spotify_username = user_info.get('spotify_id', 'unknown')
                db.set_user_spotify(user_id, spotify_username, user_info)

                success_message = (
                    f"✅ *¡Autenticación exitosa!*\n\n"
                    f"👤 Usuario: {user_info.get('display_name', spotify_username)}\n"
                    f"🆔 ID: {spotify_username}\n"
                    f"👥 Seguidores: {user_info.get('followers', 0):,}\n"
                    f"🎵 Playlists: {user_info.get('public_playlists', 0)}\n"
                    f"🌍 País: {user_info.get('country', 'No especificado')}\n"
                    f"💎 Tipo: {user_info.get('product', 'free').title()}\n\n"
                    f"Ahora puedes acceder a todas las funciones de Spotify."
                )

                await status_message.edit_text(
                    success_message,
                    parse_mode='Markdown',
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🎵 Abrir Spotify", callback_data=f"spotify_menu_{user_id}")
                    ]])
                )
            else:
                await status_message.edit_text(
                    f"❌ Error en autenticación:\n{message_text}\n\n"
                    f"💡 **Consejos:**\n"
                    f"• Verifica que copiaste el código completo\n"
                    f"• El código expira en 10 minutos\n"
                    f"• Intenta generar una nueva URL con `/spotify`"
                )

        except Exception as e:
            logger.error(f"Error procesando código OAuth: {e}")
            await status_message.edit_text(
                "❌ Error procesando el código.\n\n"
                "🔄 Intenta de nuevo:\n"
                "1. Ve a `/spotify`\n"
                "2. Genera nueva URL de autorización\n"
                "3. Copia el código completo"
            )

        del context.user_data['waiting_for_spotify_code']
        return

    # Si no hay nada esperado, no hacer nada
    else:
        print(f"DEBUG: No hay handlers esperando input, user_data: {context.user_data}")  # DEBUG temporal



# ===========================
# FUNCIONES AUXILIARES ESPECÍFICAS
# ===========================

async def show_notifications_menu(query, user: Dict):
    """Muestra el submenú de notificaciones"""
    status = "✅ Activadas" if user['notification_enabled'] else "❌ Desactivadas"

    message = (
        f"🔔 *Gestión de Notificaciones*\n\n"
        f"Estado actual: {status}\n"
        f"Hora actual: {user['notification_time']}\n\n"
        f"Selecciona una opción:"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Activar", callback_data=f"notif_on_{user['id']}"),
            InlineKeyboardButton("❌ Desactivar", callback_data=f"notif_off_{user['id']}")
        ],
        [
            InlineKeyboardButton("⏰ Cambiar hora", callback_data=f"notif_time_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔙 Volver", callback_data=f"config_back_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_countries_menu(query, user: Dict, services: Dict):
    """Muestra el submenú de países"""
    if services.get('country_state_city'):
        user_countries = services['country_state_city'].get_user_countries(user['id'])
        if user_countries:
            countries_text = "\n".join([f"• {c['name']} ({c['code']})" for c in user_countries])
        else:
            countries_text = "Ningún país configurado"
    else:
        user_services_config = user_services.get_user_services(user['id'])
        countries_text = f"• {user_services_config.get('country_filter', 'ES')} (sistema legacy)"

    message = (
        f"🌍 *Gestión de Países*\n\n"
        f"Países actuales:\n{countries_text}\n\n"
        f"Selecciona una opción:"
    )

    keyboard = [
        [
            InlineKeyboardButton("➕ Añadir país", callback_data=f"country_add_{user['id']}"),
            InlineKeyboardButton("➖ Eliminar país", callback_data=f"country_remove_{user['id']}")
        ],
        [
            InlineKeyboardButton("📋 Ver disponibles", callback_data=f"country_list_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔙 Volver", callback_data=f"config_back_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_services_menu(query, user: Dict):
    """Muestra el submenú de servicios"""
    user_services_config = user_services.get_user_services(user['id'])

    services_status = []
    for service in ['ticketmaster', 'spotify', 'setlistfm']:
        status = "✅" if user_services_config.get(service, True) else "❌"
        services_status.append(f"{status} {service.capitalize()}")

    message = (
        f"🔧 *Gestión de Servicios*\n\n"
        f"Estado actual:\n" + "\n".join(services_status) + "\n\n"
        f"Selecciona una opción:"
    )

    keyboard = [
        [
            InlineKeyboardButton("✅ Activar servicio", callback_data=f"service_activate_{user['id']}"),
            InlineKeyboardButton("❌ Desactivar servicio", callback_data=f"service_deactivate_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔙 Volver", callback_data=f"config_back_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_artists_menu(query, user: Dict):
    """Muestra el submenú de artistas (equivalente a /list)"""
    followed_artists = db.get_user_followed_artists(user['id'])

    if not followed_artists:
        message = (
            f"🎵 *Artistas seguidos*\n\n"
            f"No tienes artistas seguidos aún.\n"
            f"Usa el botón de abajo para añadir artistas."
        )
    else:
        message_lines = [f"🎵 *Artistas seguidos* ({len(followed_artists)})\n"]

        # Mostrar solo los primeros 10 para no sobrecargar
        for i, artist in enumerate(followed_artists[:10], 1):
            line = f"{i}. *{artist['name']}*"

            details = []
            if artist['country']:
                details.append(f"🌍 {artist['country']}")
            if artist['formed_year']:
                details.append(f"📅 {artist['formed_year']}")

            if details:
                line += f" ({', '.join(details)})"

            message_lines.append(line)

        if len(followed_artists) > 10:
            message_lines.append(f"_...y {len(followed_artists) - 10} más_")

        message_lines.append(f"\nUsa `/list` para ver la lista completa con enlaces.")
        message = "\n".join(message_lines)

    keyboard = [
        [
            InlineKeyboardButton("➕ Añadir artista", callback_data=f"artist_add_{user['id']}"),
            InlineKeyboardButton("🔍 Buscar conciertos", callback_data=f"artist_search_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔙 Volver", callback_data=f"config_back_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

# ===========================
# FUNCIÓN PRINCIPAL
# ===========================



def filter_future_concerts_by_countries(all_concerts, user_countries, database_path=None):
    """
    Filtra conciertos futuros por países del usuario
    VERSIÓN CORREGIDA: Acepta database_path como parámetro
    """
    from user_services import get_services
    from datetime import datetime

    services = get_services()
    today = datetime.now().date()
    future_concerts = []

    # Primero filtrar por fecha (solo futuros)
    for concert in all_concerts:
        concert_date = concert.get('date', '')
        if concert_date and len(concert_date) >= 10:
            try:
                concert_date_obj = datetime.strptime(concert_date[:10], '%Y-%m-%d').date()
                if concert_date_obj >= today:
                    future_concerts.append(concert)
            except ValueError:
                # Si no se puede parsear la fecha, incluir el concierto
                future_concerts.append(concert)
        else:
            # Si no hay fecha, incluir el concierto
            future_concerts.append(concert)

    # Luego filtrar por países del usuario
    filtered_concerts = []

    if services.get('country_state_city') and database_path:
        try:
            from apis.country_state_city import ArtistTrackerDatabaseExtended
            extended_db = ArtistTrackerDatabaseExtended(database_path, services['country_state_city'])
            filtered_concerts = extended_db.filter_concerts_by_countries(future_concerts, user_countries)
            logger.debug(f"Filtrado avanzado: {len(future_concerts)} -> {len(filtered_concerts)} conciertos")

        except Exception as e:
            logger.error(f"Error filtrando conciertos por países: {e}")
            # Fallback a filtrado básico
            filtered_concerts = _basic_country_filter(future_concerts, user_countries)
    else:
        # Filtrado básico si no hay servicio de países
        filtered_concerts = _basic_country_filter(future_concerts, user_countries)

    return filtered_concerts


def _basic_country_filter(concerts, user_countries):
    """Filtrado básico por código de país"""
    filtered_concerts = []

    for concert in concerts:
        concert_country = concert.get('country_code', concert.get('country', '')).upper()

        # Si no hay información de país O el país está en la lista del usuario, incluir
        if not concert_country or concert_country in user_countries:
            filtered_concerts.append(concert)

    logger.debug(f"Filtrado básico: {len(concerts)} -> {len(filtered_concerts)} conciertos")
    return filtered_concerts

def get_no_concerts_suggestions(is_search, countries_text):
    """Obtiene sugerencias cuando no se encuentran conciertos"""
    if is_search:
        return (
            "💡 Sugerencias:\n"
            f"• Usa `/show` para ver conciertos ya guardados\n"
            f"• Usa `/addcountry <país>` para añadir más países\n"
            f"• Algunos conciertos pueden anunciarse más cerca de las fechas"
        )
    else:
        return (
            "💡 Sugerencias:\n"
            f"• Usa `/addcountry <país>` para añadir más países\n"
            f"• Usa `/search` para buscar nuevos conciertos\n"
            f"• Usa `/searchartist <nombre>` para buscar conciertos de un artista específico"
        )


async def searchartist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /searchartist - búsqueda específica de un artista"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el nombre del artista.\n"
            "Ejemplo: `/searchartist Metallica`"
        )
        return

    artist_name = " ".join(context.args)
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado para usar su configuración
    user = db.get_user_by_chat_id(chat_id)
    user_services_config = None

    if user:
        user_services_config = user_services.get_user_services(user['id'])

        # Verificar que tenga al menos un servicio activo
        active_services = [s for s, active in user_services_config.items() if active and s not in ['country_filter', 'countries']]
        if not active_services:
            await update.message.reply_text(
                "❌ No tienes ningún servicio de búsqueda activo.\n"
                "Usa `/serviceon <servicio>` para activar al menos uno.\n"
                "Servicios disponibles: ticketmaster, spotify, setlistfm"
            )
            return

        # Verificar que tenga países configurados
        user_countries = user_services_config.get('countries', set())
        if not user_countries:
            await update.message.reply_text(
                "❌ No tienes países configurados.\n"
                "Usa `/addcountry <país>` para añadir países.\n"
                "Ejemplo: `/addcountry ES`"
            )
            return

        services_text = ", ".join(active_services)
        countries_text = ", ".join(sorted(user_countries))
        status_message = await update.message.reply_text(
            f"🔍 Buscando conciertos para '{artist_name}'...\n"
            f"🔧 Servicios activos: {services_text}\n"
            f"🌍 Países: {countries_text}"
        )
    else:
        # Usuario no registrado - usar configuración por defecto
        status_message = await update.message.reply_text(
            f"🔍 Buscando conciertos para '{artist_name}'...\n"
            f"(Usando configuración por defecto. Regístrate con `/adduser` para personalizar)"
        )

    try:
        # Buscar conciertos para el artista
        services = get_services()
        concerts = await search_concerts_for_artist(
            artist_name,
            user_services_config,
            user_id=user['id'] if user else None,
            services=services,
            database=db
        )

        if not concerts:
            country_info = f" en tus países configurados" if user else ""
            await status_message.edit_text(
                f"📭 No se encontraron conciertos para '{artist_name}'{country_info}.\n"
                "Verifica la ortografía e inténtalo de nuevo."
            )
            return

        # Guardar conciertos en la base de datos
        for concert in concerts:
            db.save_concert(concert)

        # Formatear mensaje usando el nuevo formato
        message = format_single_artist_concerts_complete(
            concerts,
            artist_name,
            show_notified=False
        )

        # Dividir en chunks si es muy largo
        if len(message) > 4000:
            chunks = split_long_message(message, max_length=4000)

            # Editar mensaje original con el primer chunk
            await status_message.edit_text(
                chunks[0],
                parse_mode='Markdown',
                disable_web_page_preview=True
            )

            # Enviar chunks adicionales
            for chunk in chunks[1:]:
                await update.message.reply_text(
                    chunk,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
        else:
            await status_message.edit_text(
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )

    except Exception as e:
        logger.error(f"Error en comando searchartist: {e}")
        await status_message.edit_text(
            f"❌ Error al buscar conciertos para '{artist_name}'. Inténtalo de nuevo más tarde."
        )

async def showartist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /showartist - muestra conciertos futuros de un artista filtrados por países del usuario"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el nombre del artista.\n"
            "Ejemplo: `/showartist Radiohead`"
        )
        return

    artist_name = " ".join(context.args)
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Obtener configuración de países del usuario
    user_services_config = user_services.get_user_services(user['id'])
    if not user_services_config:
        user_services_config = {'countries': {'ES'}, 'country_filter': 'ES'}

    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        country_filter = user_services_config.get('country_filter', 'ES')
        user_countries = {country_filter}

    # Obtener TODOS los conciertos del artista de la base de datos
    conn = db.get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT DISTINCT c.*
            FROM concerts c
            WHERE LOWER(c.artist_name) = LOWER(?)
            ORDER BY c.date ASC
        """, (artist_name,))

        rows = cursor.fetchall()
        all_artist_concerts = [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"Error obteniendo conciertos de {artist_name}: {e}")
        all_artist_concerts = []
    finally:
        conn.close()

    if not all_artist_concerts:
        await update.message.reply_text(
            f"📭 No se encontraron conciertos para '{artist_name}' en la base de datos.\n"
            f"💡 Sugerencias:\n"
            f"• Verifica la ortografía del nombre\n"
            f"• Usa `/addartist {artist_name}` para seguir al artista\n"
            f"• Usa `/searchartist {artist_name}` para buscar nuevos conciertos"
        )
        return

    # Filtrar solo conciertos futuros y por países
    filtered_concerts = filter_future_concerts_by_countries(all_artist_concerts, user_countries)

    # Verificar si el usuario sigue a este artista
    followed_artists = db.get_user_followed_artists(user['id'])
    is_following = any(artist['name'].lower() == artist_name.lower() for artist in followed_artists)

    # Mensaje de información inicial
    countries_text = ", ".join(sorted(user_countries))
    info_message = f"🎵 Conciertos de *{artist_name}*\n"
    info_message += f"🌍 Mostrando países: {countries_text}\n"
    info_message += f"📊 {len(filtered_concerts)} de {len(all_artist_concerts)} conciertos\n"

    if not is_following:
        info_message += f"💡 Usa `/addartist {artist_name}` para seguir y recibir notificaciones\n"

    info_message += "─" * 30

    # Mostrar primero los conciertos filtrados por países
    if not filtered_concerts:
        # No hay conciertos en los países del usuario
        no_concerts_message = (
            f"📭 *{artist_name}* no tiene conciertos futuros en tus países ({countries_text})\n\n"
            f"📊 Pero tiene {len(all_artist_concerts)} conciertos en la base de datos\n\n"
            f"💡 Usa `/addcountry <país>` para añadir más países\n"
            f"💡 Usa `/searchartist {artist_name}` para buscar nuevos conciertos"
        )

        await update.message.reply_text(
            no_concerts_message,
            parse_mode='Markdown'
        )
    else:
        # Hay conciertos en los países del usuario
        await update.message.reply_text(info_message, parse_mode='Markdown')

        # Usar la función mejorada que filtra conciertos futuros automáticamente
        message = format_single_artist_concerts_complete(
            filtered_concerts,
            artist_name,
            show_notified=is_following
        )

        # Dividir en chunks si es muy largo
        if len(message) > 4000:
            chunks = split_long_message(message)

            # Enviar el primer chunk
            await update.message.reply_text(
                chunks[0],
                parse_mode='Markdown',
                disable_web_page_preview=True
            )

            # Enviar chunks adicionales con pausa
            for chunk in chunks[1:]:
                await asyncio.sleep(0.5)
                await update.message.reply_text(
                    chunk,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
        else:
            await update.message.reply_text(
                message,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )

# ===========================
# COMANDOS DE SERVICIOS
# ===========================

async def serviceon_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /serviceon para activar un servicio"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el servicio.\n"
            "Ejemplo: `/serviceon ticketmaster`\n"
            "Servicios disponibles: ticketmaster, spotify, setlistfm"
        )
        return

    service = context.args[0].lower()
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Validar servicio
    valid_services = ['ticketmaster', 'spotify', 'setlistfm']
    if service not in valid_services:
        await update.message.reply_text(
            f"❌ Servicio '{service}' no válido.\n"
            f"Servicios disponibles: {', '.join(valid_services)}"
        )
        return

    # Activar servicio
    if user_services.set_service_status(user['id'], service, True):
        await update.message.reply_text(
            f"✅ Servicio '{service}' activado correctamente.\n"
            f"Usa `/config` para ver tu configuración actual."
        )
    else:
        await update.message.reply_text(
            f"❌ Error al activar el servicio '{service}'."
        )

async def serviceoff_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /serviceoff para desactivar un servicio"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el servicio.\n"
            "Ejemplo: `/serviceoff spotify`\n"
            "Servicios disponibles: ticketmaster, spotify, setlistfm"
        )
        return

    service = context.args[0].lower()
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Validar servicio
    valid_services = ['ticketmaster', 'spotify', 'setlistfm']
    if service not in valid_services:
        await update.message.reply_text(
            f"❌ Servicio '{service}' no válido.\n"
            f"Servicios disponibles: {', '.join(valid_services)}"
        )
        return

    # Verificar que no sea el último servicio activo
    user_services_config = user_services.get_user_services(user['id'])
    active_services = [s for s, active in user_services_config.items() if active and s not in ['country_filter', 'countries']]

    if len(active_services) == 1 and user_services_config.get(service, False):
        await update.message.reply_text(
            f"❌ No puedes desactivar '{service}' porque es el único servicio activo.\n"
            f"Activa otro servicio primero con `/serviceon <servicio>`."
        )
        return

    # Desactivar servicio
    if user_services.set_service_status(user['id'], service, False):
        await update.message.reply_text(
            f"✅ Servicio '{service}' desactivado correctamente.\n"
            f"Usa `/config` para ver tu configuración actual."
        )
    else:
        await update.message.reply_text(
            f"❌ Error al desactivar el servicio '{service}'."
        )

# ===========================
# COMANDOS DE PAÍSES
# ===========================

async def country_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /country - VERSIÓN LEGACY con redirección al nuevo sistema"""
    services = get_services()

    if not context.args:
        message = (
            "❌ Uso incorrecto. Debes especificar el código de país.\n"
            "Ejemplo: `/country ES`\n\n"
        )

        if services.get('country_state_city'):
            message += (
                "💡 *Nuevo sistema disponible:*\n"
                "Ahora puedes tener múltiples países configurados:\n"
                "`/addcountry <país>` - Añadir país\n"
                "`/mycountries` - Ver países configurados\n"
                "`/listcountries` - Ver países disponibles"
            )
        else:
            message += "Códigos comunes: ES, US, FR, DE, IT, UK"

        await update.message.reply_text(message, parse_mode='Markdown')
        return

    country_code = context.args[0].upper()
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Validar formato del código de país
    if len(country_code) < 2 or len(country_code) > 3 or not country_code.isalpha():
        await update.message.reply_text(
            "❌ Código de país inválido. Debe tener 2-3 letras.\n"
            "Ejemplos: ES, US, FR, DE, IT, UK"
        )
        return

    if services.get('country_state_city'):
        # Usar nuevo sistema
        await update.message.reply_text(
            f"🔄 Configurando país usando el nuevo sistema...\n"
            f"Esto reemplazará tu configuración actual de países."
        )

        # Limpiar países existentes
        user_countries = services['country_state_city'].get_user_countries(user['id'])
        for country in user_countries:
            services['country_state_city'].remove_user_country(user['id'], country['code'])

        # Añadir nuevo país
        success = services['country_state_city'].add_user_country(user['id'], country_code)

        if success:
            country_info = services['country_state_city'].get_country_info(country_code)
            country_name = country_info['name'] if country_info else country_code

            await update.message.reply_text(
                f"✅ País configurado: {country_name} ({country_code})\n\n"
                f"💡 Ahora puedes añadir más países con `/addcountry`\n"
                f"Usa `/mycountries` para ver tu configuración."
            )
        else:
            await update.message.reply_text(
                f"❌ Error configurando el país {country_code}.\n"
                f"Verifica que el código sea válido."
            )
    else:
        # Usar sistema legacy
        if user_services.set_country_filter(user['id'], country_code):
            await update.message.reply_text(
                f"✅ Filtro de país establecido a '{country_code}'.\n"
                f"Usa `/config` para ver tu configuración actual."
            )
        else:
            await update.message.reply_text(
                f"❌ Error al establecer el filtro de país."
            )


async def addcountry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /addcountry - añade un país a la configuración del usuario"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el código o nombre del país.\n"
            "Ejemplo: `/addcountry ES` o `/addcountry Spain`\n"
            "Usa `/listcountries` para ver países disponibles"
        )
        return

    if not country_state_city:
        await update.message.reply_text(
            "❌ Servicio de países no disponible.\n"
            "Contacta al administrador para configurar la API key."
        )
        return

    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    query = " ".join(context.args)

    # Mensaje de estado
    status_message = await update.message.reply_text(
        f"🔍 Buscando país: '{query}'..."
    )

    try:
        # Si es un código de 2 letras, usarlo directamente
        if len(query) == 2 and query.isalpha():
            country_code = query.upper()

            # Verificar que existe
            country_info = country_state_city.get_country_info(country_code)
            if not country_info:
                # Intentar obtener países actualizados
                countries = country_state_city.get_available_countries(force_refresh=True)
                country_info = country_state_city.get_country_info(country_code)

            if country_info:
                selected_country = country_info
            else:
                await status_message.edit_text(
                    f"❌ País con código '{country_code}' no encontrado.\n"
                    f"Usa `/listcountries` para ver países disponibles."
                )
                return
        else:
            # Buscar por nombre
            matching_countries = country_state_city.search_countries(query)

            if not matching_countries:
                await status_message.edit_text(
                    f"❌ No se encontraron países que coincidan con '{query}'.\n"
                    f"Usa `/listcountries` para ver países disponibles."
                )
                return
            elif len(matching_countries) == 1:
                selected_country = matching_countries[0]
            else:
                # Múltiples coincidencias - mostrar opciones
                await show_country_selection(update, matching_countries, query, status_message)
                return

        # Añadir país
        await status_message.edit_text(
            f"⏳ Añadiendo país {selected_country['name']} ({selected_country['code']})...\n"
            f"Esto puede tardar un momento mientras obtenemos las ciudades..."
        )

        success = country_state_city.add_user_country(user['id'], selected_country['code'])

        if success:
            # Obtener estadísticas
            cities = country_state_city.get_country_cities(selected_country['code'])
            user_countries = country_state_city.get_user_countries(user['id'])

            await status_message.edit_text(
                f"✅ País añadido: {selected_country['name']} ({selected_country['code']})\n"
                f"🏙️ {len(cities)} ciudades cargadas\n"
                f"🌍 Total países configurados: {len(user_countries)}\n\n"
                f"Usa `/mycountries` para ver tu configuración actual."
            )
        else:
            await status_message.edit_text(
                f"❌ Error añadiendo el país {selected_country['name']}.\n"
                f"Es posible que ya lo tengas configurado."
            )

    except Exception as e:
        logger.error(f"Error en comando addcountry: {e}")
        await status_message.edit_text(
            "❌ Error al añadir el país. Inténtalo de nuevo más tarde."
        )


async def removecountry_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /removecountry - elimina un país de la configuración del usuario"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el código del país.\n"
            "Ejemplo: `/removecountry ES`\n"
            "Usa `/mycountries` para ver tus países configurados"
        )
        return

    if not country_state_city:
        await update.message.reply_text(
            "❌ Servicio de países no disponible."
        )
        return

    chat_id = update.effective_chat.id
    country_code = context.args[0].upper()

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Verificar que el usuario tenga más de un país (no puede quedarse sin países)
    user_countries = country_state_city.get_user_countries(user['id'])
    if len(user_countries) <= 1:
        await update.message.reply_text(
            "❌ No puedes eliminar tu último país configurado.\n"
            "Añade otro país primero con `/addcountry`"
        )
        return

    # Eliminar país
    success = country_state_city.remove_user_country(user['id'], country_code)

    if success:
        country_info = country_state_city.get_country_info(country_code)
        country_name = country_info['name'] if country_info else country_code

        remaining_countries = country_state_city.get_user_countries(user['id'])

        await update.message.reply_text(
            f"✅ País eliminado: {country_name} ({country_code})\n"
            f"🌍 Países restantes: {len(remaining_countries)}\n\n"
            f"Usa `/mycountries` para ver tu configuración actual."
        )
    else:
        await update.message.reply_text(
            f"❌ No tenías el país '{country_code}' configurado."
        )


async def mycountries_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /mycountries - muestra países configurados del usuario"""
    if not country_state_city:
        await update.message.reply_text(
            "❌ Servicio de países no disponible."
        )
        return

    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Obtener países del usuario
    user_countries = country_state_city.get_user_countries(user['id'])

    if not user_countries:
        await update.message.reply_text(
            "📭 No tienes países configurados.\n"
            "Usa `/addcountry <país>` para añadir países.\n"
            "Ejemplo: `/addcountry ES`"
        )
        return

    # Formatear mensaje
    message_lines = [f"🌍 *Países configurados para {user['username']}:*\n"]

    for i, country in enumerate(user_countries, 1):
        line = f"{i}. *{country['name']}* ({country['code']})"

        details = []
        if country.get('currency'):
            details.append(f"💰 {country['currency']}")
        if country.get('phone_code'):
            details.append(f"📞 +{country['phone_code']}")

        if details:
            line += f" - {' | '.join(details)}"

        message_lines.append(line)

    message_lines.append(f"\n📊 Total: {len(user_countries)} países")
    message_lines.append("\n💡 *Comandos útiles:*")
    message_lines.append("`/addcountry <país>` - Añadir país")
    message_lines.append("`/removecountry <código>` - Eliminar país")
    message_lines.append("`/listcountries` - Ver países disponibles")

    response = "\n".join(message_lines)

    try:
        await update.message.reply_text(
            response,
            parse_mode='Markdown'
        )
    except Exception as e:
        # Si hay error con Markdown, enviar sin formato
        logger.warning(f"Error con Markdown en mycountries, enviando texto plano: {e}")
        plain_response = response.replace('*', '').replace('`', '')
        await update.message.reply_text(plain_response)



async def listcountries_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /listcountries - muestra continentes para seleccionar países"""
    if not country_state_city:
        await update.message.reply_text(
            "❌ Servicio de países no disponible."
        )
        return

    # Mensaje de estado
    status_message = await update.message.reply_text(
        "🌍 Cargando continentes disponibles..."
    )

    try:
        # Obtener países para verificar disponibilidad
        countries = country_state_city.get_available_countries()

        if not countries:
            await status_message.edit_text(
                "❌ No se pudieron obtener los países disponibles.\n"
                "Inténtalo de nuevo más tarde."
            )
            return

        # Crear mensaje con botones de continentes
        message = (
            "🌍 *Países disponibles por continente*\n\n"
            f"📊 Total de países: {len(countries)}\n\n"
            "Selecciona un continente para ver todos sus países:"
        )

        # Definir continentes con emojis
        continents = [
            ("🇪🇺", "Europa", "europe"),
            ("🇺🇸", "América del Norte", "north_america"),
            ("🇧🇷", "América del Sur", "south_america"),
            ("🇨🇳", "Asia", "asia"),
            ("🇦🇺", "Oceanía", "oceania"),
            ("🇿🇦", "África", "africa"),
            ("🌍", "Otros", "others")
        ]

        # Crear teclado con botones de continentes
        keyboard = []
        for emoji, name, code in continents:
            keyboard.append([InlineKeyboardButton(f"{emoji} {name}", callback_data=f"continent_{code}")])

        # Botón para ver todos los países de una vez
        keyboard.append([InlineKeyboardButton("📋 Ver todos los países", callback_data="continent_all")])

        reply_markup = InlineKeyboardMarkup(keyboard)

        await status_message.edit_text(
            message,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

    except Exception as e:
        logger.error(f"Error en comando listcountries: {e}")
        await status_message.edit_text(
            "❌ Error al cargar continentes. Inténtalo de nuevo más tarde."
        )


async def refreshcountries_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /refreshcountries - actualiza la base de datos de países (solo admins)"""
    if not country_state_city:
        await update.message.reply_text(
            "❌ Servicio de países no disponible."
        )
        return

    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Mensaje de estado
    status_message = await update.message.reply_text(
        "🔄 Actualizando base de datos de países desde API...\n"
        "Esto puede tardar un momento..."
    )

    try:
        # Forzar actualización desde API
        countries = country_state_city.get_available_countries(force_refresh=True)

        if countries:
            await status_message.edit_text(
                f"✅ Base de datos actualizada correctamente.\n"
                f"📊 {len(countries)} países disponibles.\n\n"
                f"Usa `/listcountries` para ver la lista actualizada."
            )
        else:
            await status_message.edit_text(
                "❌ No se pudieron actualizar los países.\n"
                "Verifica la conexión y la API key."
            )

    except Exception as e:
        logger.error(f"Error en comando refreshcountries: {e}")
        await status_message.edit_text(
            "❌ Error al actualizar países. Inténtalo de nuevo más tarde."
        )



# ===========================
# COMANDOS DE CONFIGURACIÓN
# ===========================

async def config_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /config - VERSIÓN INTERACTIVA con botones"""
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Mostrar configuración con botones
    await show_config_menu(update, user)

async def show_config_menu(update, user: Dict, edit_message: bool = False):
    """Muestra el menú principal de configuración"""
    services = get_services()

    # Obtener configuración de servicios
    user_services_config = user_services.get_user_services(user['id'])

    # Formatear mensaje de configuración
    config_lines = [f"⚙️ *Configuración de {user['username']}*\n"]

    # Notificaciones
    notification_status = "✅ Activadas" if user['notification_enabled'] else "❌ Desactivadas"
    config_lines.append(f"🔔 *Notificaciones:* {notification_status}")
    config_lines.append(f"⏰ *Hora:* {user['notification_time']}")
    config_lines.append("")

    # Países configurados
    if services.get('country_state_city'):
        user_countries = services['country_state_city'].get_user_countries(user['id'])
        if user_countries:
            config_lines.append("🌍 *Países configurados:*")
            countries_text = ", ".join([f"{c['name']} ({c['code']})" for c in user_countries[:3]])
            if len(user_countries) > 3:
                countries_text += f" y {len(user_countries) - 3} más"
            config_lines.append(f"   {countries_text}")
        else:
            config_lines.append("🌍 *Países:* Ninguno configurado")
    else:
        # Fallback al sistema legacy
        country_filter = user_services_config.get('country_filter', 'ES')
        config_lines.append(f"🌍 *País:* {country_filter}")

    config_lines.append("")

    # Estado de servicios
    config_lines.append("🔧 *Servicios de búsqueda:*")
    active_services = []
    inactive_services = []

    for service in ['ticketmaster', 'spotify', 'setlistfm']:
        if user_services_config.get(service, True):
            active_services.append(service.capitalize())
        else:
            inactive_services.append(service.capitalize())

    if active_services:
        config_lines.append(f"   ✅ {', '.join(active_services)}")
    if inactive_services:
        config_lines.append(f"   ❌ {', '.join(inactive_services)}")

    # Artistas seguidos
    followed_artists = db.get_user_followed_artists(user['id'])
    config_lines.append("")
    config_lines.append(f"🎵 *Artistas seguidos:* {len(followed_artists)}")

    # Crear botones del menú principal
    keyboard = [
        [
            InlineKeyboardButton("🔔 Notificaciones", callback_data=f"config_notifications_{user['id']}"),
            InlineKeyboardButton("🌍 Países", callback_data=f"config_countries_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔧 Servicios", callback_data=f"config_services_{user['id']}"),
            InlineKeyboardButton("🎵 Artistas", callback_data=f"config_artists_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔄 Actualizar", callback_data=f"config_refresh_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    response = "\n".join(config_lines)

    try:
        if edit_message and hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(
                response,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                response,
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
    except Exception as e:
        # Si hay error con Markdown, enviar sin formato
        logger.warning(f"Error con Markdown en config, enviando texto plano: {e}")
        plain_response = response.replace('*', '').replace('`', '')
        if edit_message and hasattr(update, 'callback_query'):
            await update.callback_query.edit_message_text(
                plain_response,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                plain_response,
                reply_markup=reply_markup
            )

# ===========================
# COMANDOS DE LAST.FM Y SPOTIFY
# ===========================

async def lastfm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /lastfm - gestión de sincronización con Last.fm"""
    services = get_services()

    if not services.get('lastfm_service'):
        await update.message.reply_text(
            "❌ Servicio de Last.fm no disponible.\n"
            "Contacta al administrador para configurar la API key."
        )
        return

    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Verificar si ya tiene usuario de Last.fm configurado
    lastfm_user = db.get_user_lastfm(user['id'])

    if not lastfm_user:
        # No tiene usuario configurado, pedirlo
        context.user_data['waiting_for_lastfm_user'] = user['id']
        await show_lastfm_setup(update, user, context)
    else:
        # Ya tiene usuario, mostrar menú principal
        await show_lastfm_menu(update, user, lastfm_user)

async def show_lastfm_setup(update, user: Dict, context = None):
    """Muestra el setup inicial de Last.fm"""
    message = (
        "🎵 *Configuración de Last.fm*\n\n"
        "Para sincronizar tus artistas más escuchados desde Last.fm, "
        "necesito tu nombre de usuario.\n\n"
        "Envía tu nombre de usuario de Last.fm:"
    )

    keyboard = [[InlineKeyboardButton("❌ Cancelar", callback_data=f"lastfm_cancel_{user['id']}")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_lastfm_menu(update, user: Dict, lastfm_user: Dict):
    """Muestra el menú principal de Last.fm"""
    username = lastfm_user['lastfm_username']
    playcount = lastfm_user.get('lastfm_playcount', 0)
    sync_limit = lastfm_user.get('sync_limit', 20)

    message = (
        f"🎵 *Last.fm - {username}*\n\n"
        f"📊 Reproducciones: {playcount:,}\n"
        f"🔢 Límite de sincronización: {sync_limit} artistas\n\n"
        f"Selecciona el período para sincronizar:"
    )

    keyboard = [
        [
            InlineKeyboardButton("🌟 De siempre", callback_data=f"lastfm_period_overall_{user['id']}"),
            InlineKeyboardButton("📅 Último año", callback_data=f"lastfm_period_12month_{user['id']}")
        ],
        [
            InlineKeyboardButton("📊 Último mes", callback_data=f"lastfm_period_1month_{user['id']}"),
            InlineKeyboardButton("⚡ Última semana", callback_data=f"lastfm_period_7day_{user['id']}")
        ],
        [
            InlineKeyboardButton("🔢 Cambiar cantidad", callback_data=f"lastfm_limit_{user['id']}"),
            InlineKeyboardButton("👤 Cambiar usuario", callback_data=f"lastfm_changeuser_{user['id']}")
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        message,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )


# ===========================
# COMANDOS BÁSICOS
# ===========================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /start y /help"""
    services = get_services()

    help_text = (
        "🐣Bienvenido al bot de novedades musicales\n\n"
        "Usa /adduser para registrarte y añadir artistas con /addartist \n"
        "/spotify y /lastfm permiten añadir artistas facilmente\n"
        "/muspy sirve para buscar nuevos lanzamientos de tus artistas\n\n"

        "📝 *Comandos básicos*\n"
        "/addartist <artista> - Añadir artista al bot\n"
        "/search - Buscar nuevos conciertos de TODOS tus artistas\n"
        "/show - Ver conciertos guardados de TODOS tus artistas\n"
        "/mostrar - Ver nuevos lanzamientos de tus artistas de Muspy\n\n"
    )

    help_text += (
        "❓ *Comandos directos:*\n"
        "/commands - Mostrar todos los comandos disponibles\n"
    )

    await update.message.reply_text(help_text, parse_mode='Markdown')

async def commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /commands"""
    services = get_services()

    help_text = (
        "🐣Bienvenido al bot de novedades musicales\n\n"

        "💻 *Menús de configuración*\n"
        "/config - Configura países, notificaciones y artistas seguidos\n"
        "/muspy - Usa tu cuenta de muspy para buscar nuevos lanzamientos de tus artistas\n"
        "/spotify - Permite añadir tus artistas seguidos, añadir artistas de playlists propias o desde enlaces\n"
        "/lastfm - Posibilita añadir tus artistas más escuchados por períodos de tiempo\n\n"

        "📖 *Manejo básico:*\n"
        "/adduser <usuario> - Registrarte en el sistema\n"
        "/notify [HH:MM] - Configurar notificaciones diarias\n"
        "/serviceon <servicio> - Activar servicio (ticketmaster/spotify/setlistfm)\n"
        "/serviceoff <servicio> - Desactivar servicio\n\n"

        "🤖 *Base de datos del bot:*\n"
        "/addartist <artista> - Seguir un artista\n"
        "/remove <artista> - Dejar de seguir un artista\n"
        "/list [usuario] - Ver artistas seguidos\n\n"

        "💽 *Lanzamientos musicales*\n"
        "/mostrar - Ver nuevos lanzamientos de tus artistas\n"
        "/artistas - Lista los artistas seguidos _en Muspy_\n\n"

        "🥁 *Buscar conciertos:*\n"
        "/search - Buscar nuevos conciertos de tus artistas (APIs)\n"
        "/show - Ver conciertos guardados de tus artistas (BD)\n"
        "/searchartist <artista> - Buscar conciertos específicos\n"
        "/showartist <artista> - Ver todos los conciertos de un artista\n\n"

    )

    if services.get('country_state_city'):
        help_text += (
            "🌍 *Gestión de países:*\n"
            "/addcountry <país> - Añadir país a tu configuración\n"
            "/removecountry <código> - Eliminar país\n"
            "/mycountries - Ver tus países configurados\n"
            "/listcountries - Ver países disponibles\n\n"
        )
    else:
        help_text += (
            "🌍 *Configuración de país:*\n"
            "/country <código> - Establecer filtro de país (ej: ES, US, FR)\n\n"
        )

    help_text += (
        "❓ *Ayuda:*\n"
        "/config - Ver tu configuración actual\n"
        "/help - Mostrar este mensaje de ayuda\n\n"

        "💡 *Diferencia entre comandos:*\n"
        "• `/search` = Busca nuevos conciertos en APIs (más lento)\n"
        "• `/show` = Consulta conciertos ya guardados (más rápido)"
    )

    await update.message.reply_text(help_text, parse_mode='Markdown')

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /help"""
    await start(update, context)

async def commands_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /commands"""
    await commands(update, context)

async def adduser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /adduser"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar un nombre de usuario.\n"
            "Ejemplo: `/adduser tu_nombre`"
        )
        return

    username = context.args[0]
    chat_id = update.effective_chat.id

    # Validar nombre de usuario
    if len(username) < 2 or len(username) > 50:
        await update.message.reply_text(
            "❌ El nombre de usuario debe tener entre 2 y 50 caracteres."
        )
        return

    if db.add_user(username, chat_id):
        await update.message.reply_text(
            f"✅ Usuario '{username}' registrado correctamente.\n"
            f"Ya puedes usar `/addartist` para seguir artistas."
        )
    else:
        await update.message.reply_text(
            "❌ Error al registrar el usuario. Inténtalo de nuevo."
        )

async def addartist_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /addartist mejorado con selección múltiple"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el nombre del artista.\n"
            "Ejemplo: `/addartist Radiohead`"
        )
        return

    artist_name = " ".join(context.args)
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Mensaje de estado
    status_message = await update.message.reply_text(
        f"🔍 Buscando artistas que coincidan con '{artist_name}'..."
    )

    # Buscar candidatos
    candidates = db.search_artist_candidates(artist_name)

    if not candidates:
        await status_message.edit_text(
            f"❌ No se encontraron artistas que coincidan con '{artist_name}'.\n"
            f"Verifica la ortografía e inténtalo de nuevo."
        )
        return

    logger.info(f"Encontrados {len(candidates)} candidatos para '{artist_name}'")

    # Si solo hay un candidato, añadirlo directamente
    if len(candidates) == 1:
        artist_id = db.create_artist_from_candidate(candidates[0])

        if not artist_id:
            await status_message.edit_text(
                f"❌ Error al añadir el artista '{artist_name}'. Inténtalo de nuevo."
            )
            return

        was_new = db.add_followed_artist(user['id'], artist_id)

        if was_new:
            await status_message.edit_text(
                f"✅ ¡Ahora sigues a '{candidates[0]['name']}'! 🎵\n"
                f"Usa `/list` para ver todos tus artistas seguidos."
            )
        else:
            await status_message.edit_text(
                f"ℹ️ Ya seguías a '{candidates[0]['name']}'."
            )
        return

    # Múltiples candidatos: verificar si hay un candidato claramente mejor
    best_candidate = candidates[0]
    second_best = candidates[1] if len(candidates) > 1 else None

    # Si el mejor candidato tiene score muy alto y hay una diferencia significativa
    if (best_candidate['score'] >= 95 and
        (second_best is None or best_candidate['score'] - second_best['score'] >= 20)):

        artist_id = db.create_artist_from_candidate(best_candidate)

        if not artist_id:
            await status_message.edit_text(
                f"❌ Error al añadir el artista '{artist_name}'. Inténtalo de nuevo."
            )
            return

        was_new = db.add_followed_artist(user['id'], artist_id)

        if was_new:
            await status_message.edit_text(
                f"✅ ¡Ahora sigues a '{best_candidate['name']}'! 🎵\n"
                f"(Seleccionado automáticamente por alta coincidencia: {best_candidate['score']}%)\n"
                f"Usa `/list` para ver todos tus artistas seguidos."
            )
        else:
            await status_message.edit_text(
                f"ℹ️ Ya seguías a '{best_candidate['name']}'."
            )
        return

    # Múltiples candidatos: mostrar opciones
    await show_artist_candidates(update, candidates, artist_name, status_message)

async def show_artist_candidates(update: Update, candidates: List[Dict], original_query: str, message_to_edit):
    """Muestra una lista de candidatos para que el usuario elija"""
    chat_id = update.effective_chat.id

    # Guardar candidatos para posterior selección
    db.save_pending_selection(chat_id, candidates, original_query)

    # Crear mensaje con opciones
    message_lines = [f"🎵 *Encontré varios artistas para '{original_query}':*\n"]

    keyboard = []
    for i, candidate in enumerate(candidates[:8]):  # Limitar a 8 opciones
        # Formatear información del candidato
        info_parts = []
        if candidate.get('type'):
            info_parts.append(candidate['type'].title())
        if candidate.get('country'):
            info_parts.append(f"🌍 {candidate['country']}")
        if candidate.get('formed_year'):
            info_parts.append(f"📅 {candidate['formed_year']}")
        if candidate.get('disambiguation'):
            info_parts.append(f"({candidate['disambiguation']})")

        info_text = " • ".join(info_parts) if info_parts else ""

        option_text = f"{i+1}. *{candidate['name']}*"
        if info_text:
            option_text += f"\n   _{info_text}_"

        message_lines.append(option_text)

        # Botón para esta opción
        button_text = f"{i+1}. {candidate['name']}"
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."

        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"select_artist_{i}")])

    # Botón de cancelar
    keyboard.append([InlineKeyboardButton("❌ Cancelar", callback_data="cancel_artist_selection")])

    message_lines.append("\n*Selecciona el artista correcto:*")

    reply_markup = InlineKeyboardMarkup(keyboard)

    # Escapar caracteres especiales para Markdown
    response = "\n".join(message_lines)
    for char in ['_', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']:
        if char not in ['*', '_']:  # No escapar asteriscos y guiones bajos que usamos para formato
            response = response.replace(char, f'\\{char}')

    await message_to_edit.edit_text(
        response,
        parse_mode='MarkdownV2',
        reply_markup=reply_markup
    )

async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /list mejorado con paginación automática"""
    chat_id = update.effective_chat.id

    # Determinar qué usuario consultar
    if context.args:
        # Consultar otro usuario
        target_username = context.args[0]
        target_user = db.get_user_by_username(target_username)

        if not target_user:
            await update.message.reply_text(
                f"❌ Usuario '{target_username}' no encontrado."
            )
            return

        user_id = target_user['id']
        display_name = target_username
    else:
        # Consultar usuario actual
        current_user = db.get_user_by_chat_id(chat_id)
        if not current_user:
            await update.message.reply_text(
                "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
            )
            return

        user_id = current_user['id']
        display_name = "tú"

    # Obtener artistas seguidos
    followed_artists = db.get_user_followed_artists(user_id)

    if not followed_artists:
        pronoun = "no tienes" if display_name == "tú" else "no tiene"
        await update.message.reply_text(
            f"📭 {display_name.capitalize()} {pronoun} artistas seguidos aún.\n"
            f"Usa `/addartist <nombre>` para empezar a seguir artistas."
        )
        return

    # Si hay 15 o menos artistas, mostrar sin paginación (comportamiento original)
    if len(followed_artists) <= 15:
        response = await show_artists_without_pagination(update, followed_artists, display_name)
        try:
            await update.message.reply_text(
                response,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        except Exception as e:
            # Si hay error con Markdown, enviar sin formato
            logger.warning(f"Error con Markdown en list, enviando texto plano: {e}")
            plain_response = response.replace('*', '').replace('`', '')
            await update.message.reply_text(plain_response)
    else:
        # Guardar datos para paginación y mostrar primera página
        db.save_list_pagination_data(user_id, followed_artists, display_name)

        response, keyboard = await show_artists_page(update, user_id, followed_artists, display_name, page=0, edit_message=False)
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None

        try:
            await update.message.reply_text(
                response,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
        except Exception as e:
            # Si hay error con Markdown, enviar sin formato
            logger.warning(f"Error con Markdown en página de artistas: {e}")
            plain_response = response.replace('*', '').replace('`', '')
            await update.message.reply_text(plain_response, reply_markup=reply_markup)

async def remove_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /remove"""
    if not context.args:
        await update.message.reply_text(
            "❌ Uso incorrecto. Debes especificar el nombre del artista.\n"
            "Ejemplo: `/remove Radiohead`"
        )
        return

    artist_name = " ".join(context.args)
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Eliminar de la lista de seguimiento
    was_removed = db.remove_followed_artist(user['id'], artist_name)

    if was_removed:
        await update.message.reply_text(
            f"✅ Has dejado de seguir a '{artist_name}'."
        )
    else:
        await update.message.reply_text(
            f"❌ No seguías a '{artist_name}' o no se encontró el artista."
        )

async def notify_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /notify para configurar notificaciones"""
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    if not context.args:
        # Mostrar configuración actual
        status = "activadas" if user['notification_enabled'] else "desactivadas"
        await update.message.reply_text(
            f"🔔 *Configuración de notificaciones:*\n\n"
            f"Estado: {status}\n"
            f"Hora: {user['notification_time']}\n\n"
            f"*Uso:*\n"
            f"`/notify HH:MM` - Establecer hora (ej: /notify 09:00)\n"
            f"`/notify toggle` - Activar/desactivar\n"
            f"`/notify status` - Ver configuración actual\n\n"
            f"*Nota:* Las notificaciones se envían mediante un script separado.\n"
            f"Asegúrate de ejecutar `python notification_scheduler.py` en segundo plano.",
            parse_mode='Markdown'
        )
        return

    command = context.args[0].lower()

    if command == "toggle":
        # Cambiar estado de notificaciones
        new_state = user_services.toggle_notifications(user['id'])
        status = "activadas" if new_state else "desactivadas"
        await update.message.reply_text(
            f"🔔 Notificaciones {status}."
        )
    elif command == "status":
        # Mostrar estado actual
        status = "activadas" if user['notification_enabled'] else "desactivadas"
        await update.message.reply_text(
            f"🔔 Notificaciones: {status}\n"
            f"⏰ Hora: {user['notification_time']}"
        )
    else:
        # Intentar establecer hora
        time_str = context.args[0]

        # Validar formato de hora
        try:
            datetime.strptime(time_str, '%H:%M')
        except ValueError:
            await update.message.reply_text(
                "❌ Formato de hora inválido. Usa HH:MM (ej: 09:00)"
            )
            return

        # Establecer nueva hora
        if user_services.set_notification_time(user['id'], time_str):
            await update.message.reply_text(
                f"✅ Hora de notificación establecida a las {time_str}\n"
                f"🔔 Las notificaciones están {'activadas' if user['notification_enabled'] else 'desactivadas'}"
            )
        else:
            await update.message.reply_text(
                "❌ Error al establecer la hora de notificación."
            )

# ===========================
# COMANDOS DE BÚSQUEDA
# ===========================

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /search - versión con mejor manejo de red y límites"""
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Obtener configuración del usuario
    user_services_config = user_services.get_user_services(user['id'])

    if not user_services_config:
        user_services_config = {
            'countries': {'ES'},
            'country_filter': 'ES'
        }

    # Verificar países configurados
    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        country_filter = user_services_config.get('country_filter', 'ES')
        user_countries = {country_filter}

    # Verificar servicios activos
    active_services = [s for s, active in user_services_config.items() if active and s not in ['country_filter', 'countries']]
    if not active_services:
        await update.message.reply_text(
            "❌ No tienes ningún servicio de búsqueda activo.\n"
            "Usa `/serviceon <servicio>` para activar al menos uno.\n"
            "Servicios disponibles: ticketmaster, spotify, setlistfm"
        )
        return

    # Obtener artistas seguidos
    followed_artists = db.get_user_followed_artists(user['id'])

    if not followed_artists:
        await update.message.reply_text(
            "📭 No tienes artistas seguidos aún.\n"
            "Usa `/addartist <nombre>` para seguir artistas.\n"
            "Usa `/show` para ver conciertos ya guardados en base de datos."
        )
        return

    # NUEVO: Limitar número de artistas para evitar saturar la red
    MAX_ARTISTS_PER_SEARCH = 100  # Límite razonable
    if len(followed_artists) > MAX_ARTISTS_PER_SEARCH:
        await update.message.reply_text(
            f"⚠️ Tienes {len(followed_artists)} artistas seguidos, pero el límite por búsqueda es {MAX_ARTISTS_PER_SEARCH} para evitar problemas de red.\n\n"
            f"Se procesarán los primeros {MAX_ARTISTS_PER_SEARCH} artistas.\n\n"
            f"💡 **Sugerencia:** Usa `/show` para ver conciertos ya guardados (más rápido) o reduce el número de artistas seguidos."
        )
        followed_artists = followed_artists[:MAX_ARTISTS_PER_SEARCH]

    # Mensaje de estado inicial
    countries_text = ", ".join(sorted(user_countries))
    services_text = ", ".join(active_services)

    status_message = await safe_send_message(
        update.message.reply_text,
        f"🔍 Buscando nuevos conciertos de tus artistas seguidos...\n"
        f"🎵 Artistas a procesar: {len(followed_artists)}\n"
        f"🔧 Servicios activos: {services_text}\n"
        f"🌍 Países: {countries_text}\n\n"
        f"⏳ Iniciando búsqueda concurrente..."
    )

    if not status_message:
        logger.error("No se pudo enviar mensaje de estado inicial")
        return

    try:
        # MEJORA: Búsqueda concurrente con control de red más estricto
        semaphore = asyncio.Semaphore(3)  # REDUCIDO de 5 a 3 para menos carga de red
        services = get_services()

        async def search_artist_concurrent_safe(artist, progress_callback):
            """Busca conciertos para un artista de forma concurrente con mejor manejo de errores"""
            async with semaphore:
                try:
                    artist_name = artist['name']

                    # NUEVO: Pausa adicional entre búsquedas para reducir carga de red
                    await asyncio.sleep(0.5)

                    concerts = await search_concerts_for_artist(
                        artist_name,
                        user_services_config,
                        user_id=user['id'],
                        services=services,
                        database=db
                    )
                    await progress_callback(artist_name, len(concerts))
                    return concerts
                except Exception as e:
                    logger.error(f"Error buscando conciertos para {artist['name']}: {e}")
                    await progress_callback(artist['name'], 0)
                    return []

        # Contador de progreso thread-safe
        progress_lock = asyncio.Lock()
        processed_count = 0
        total_artists = len(followed_artists)

        async def update_progress_safe(artist_name, concert_count):
            """Actualiza el progreso de forma thread-safe con mejor manejo de errores de red"""
            nonlocal processed_count
            async with progress_lock:
                processed_count += 1
                # Actualizar cada 5 artistas o al final para reducir tráfico de red
                if processed_count % 5 == 0 or processed_count == total_artists:
                    try:
                        await safe_edit_message(
                            status_message.edit_text,
                            f"🔍 Búsqueda concurrente en progreso...\n"
                            f"📊 Progreso: {processed_count}/{total_artists} artistas\n"
                            f"🎵 Último: {artist_name} ({concert_count} conciertos)\n"
                            f"🔧 Servicios: {services_text}\n"
                            f"🌍 Países: {countries_text}\n\n"
                            f"⏱️ Tiempo estimado restante: {((total_artists - processed_count) * 2)} segundos"
                        )
                    except Exception as e:
                        logger.debug(f"Error actualizando progreso (no crítico): {e}")

        # Ejecutar todas las búsquedas concurrentemente
        tasks = [
            search_artist_concurrent_safe(artist, update_progress_safe)
            for artist in followed_artists
        ]

        # MEJORA: Usar asyncio.gather con mejor manejo de excepciones
        try:
            all_results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error crítico en búsqueda concurrente: {e}")
            await safe_edit_message(
                status_message.edit_text,
                f"❌ Error crítico en la búsqueda concurrente.\n"
                f"Algunos artistas pueden no haberse procesado.\n"
                f"Error: {str(e)[:100]}..."
            )
            return

        # Procesar resultados
        all_found_concerts = []
        error_count = 0

        for result in all_results:
            if isinstance(result, list):
                all_found_concerts.extend(result)
            elif isinstance(result, Exception):
                error_count += 1
                logger.error(f"Excepción en resultado: {result}")
            else:
                logger.error(f"Resultado inesperado: {result}")

        # Filtrar por países y fechas futuras
        await safe_edit_message(
            status_message.edit_text,
            f"✅ Búsqueda concurrente completada!\n"
            f"🎵 {len(all_found_concerts)} conciertos encontrados\n"
            f"❌ {error_count} errores de búsqueda\n"
            f"🌍 Filtrando por países: {countries_text}\n"
            f"📅 Filtrando solo conciertos futuros..."
        )

        filtered_concerts = filter_future_concerts_by_countries(
            all_found_concerts,
            user_countries,
            database_path=db.db_path
        )

        # Procesar y enviar resultados
        await process_and_send_concert_results_safe(
            update, status_message, filtered_concerts, processed_count,
            countries_text, services_text, is_search=True, error_count=error_count
        )

    except Exception as e:
        logger.error(f"Error en comando search concurrente: {e}")
        await safe_edit_message(
            status_message.edit_text,
            f"❌ Error al buscar conciertos. Inténtalo de nuevo más tarde.\n"
            f"Error: {str(e)[:100]}...\n\n"
            f"💡 **Sugerencias:**\n"
            f"• Verifica tu conexión a internet\n"
            f"• Reduce el número de artistas seguidos\n"
            f"• Usa `/show` para ver conciertos ya guardados"
        )

# FRAGMENTO DE telegram_bot.py - Solo las funciones modificadas para mejor manejo de red

import asyncio
from telegram.error import NetworkError, RetryAfter, TimedOut

async def search_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /search - versión por lotes sin límite máximo"""
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Obtener configuración del usuario
    user_services_config = user_services.get_user_services(user['id'])

    if not user_services_config:
        user_services_config = {
            'countries': {'ES'},
            'country_filter': 'ES'
        }

    # Verificar países configurados
    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        country_filter = user_services_config.get('country_filter', 'ES')
        user_countries = {country_filter}

    # Verificar servicios activos
    active_services = [s for s, active in user_services_config.items() if active and s not in ['country_filter', 'countries']]
    if not active_services:
        await update.message.reply_text(
            "❌ No tienes ningún servicio de búsqueda activo.\n"
            "Usa `/serviceon <servicio>` para activar al menos uno.\n"
            "Servicios disponibles: ticketmaster, spotify, setlistfm"
        )
        return

    # Obtener artistas seguidos
    followed_artists = db.get_user_followed_artists(user['id'])

    if not followed_artists:
        await update.message.reply_text(
            "📭 No tienes artistas seguidos aún.\n"
            "Usa `/addartist <nombre>` para seguir artistas.\n"
            "Usa `/show` para ver conciertos ya guardados en base de datos."
        )
        return

    # Configuración de lotes
    BATCH_SIZE = 50  # Procesar de 50 en 50
    total_artists = len(followed_artists)
    total_batches = (total_artists + BATCH_SIZE - 1) // BATCH_SIZE

    # Mensaje de estado inicial
    countries_text = ", ".join(sorted(user_countries))
    services_text = ", ".join(active_services)

    status_message = await safe_send_message(
        update.message.reply_text,
        f"🔍 **Búsqueda por lotes iniciada**\n\n"
        f"🎵 **Total de artistas:** {total_artists}\n"
        f"📦 **Lotes a procesar:** {total_batches} (de {BATCH_SIZE} artistas cada uno)\n"
        f"🔧 **Servicios activos:** {services_text}\n"
        f"🌍 **Países:** {countries_text}\n\n"
        f"⏳ **Iniciando lote 1/{total_batches}...**\n\n"
        f"💡 Puedes usar otros comandos mientras se procesa la búsqueda.",
        parse_mode='Markdown'
    )

    if not status_message:
        logger.error("No se pudo enviar mensaje de estado inicial")
        return

    try:
        # Procesar por lotes
        all_found_concerts = []
        processed_count = 0
        total_search_errors = 0
        total_network_errors = 0
        services = get_services()

        for batch_num in range(total_batches):
            start_idx = batch_num * BATCH_SIZE
            end_idx = min(start_idx + BATCH_SIZE, total_artists)
            batch_artists = followed_artists[start_idx:end_idx]

            logger.info(f"Procesando lote {batch_num + 1}/{total_batches}: artistas {start_idx + 1}-{end_idx}")

            # Actualizar estado del lote
            await safe_edit_message(
                status_message.edit_text,
                f"🔍 **Búsqueda por lotes en progreso**\n\n"
                f"📦 **Lote actual:** {batch_num + 1}/{total_batches}\n"
                f"🎵 **Artistas en este lote:** {len(batch_artists)}\n"
                f"📊 **Progreso total:** {processed_count}/{total_artists} completados\n"
                f"🔧 **Servicios:** {services_text}\n"
                f"🌍 **Países:** {countries_text}\n\n"
                f"⏳ **Procesando lote {batch_num + 1}...**\n"
                f"🕒 **Tiempo estimado restante:** {(total_batches - batch_num) * 2} minutos",
                parse_mode='Markdown'
            )

            # Procesar lote con concurrencia controlada
            batch_concerts, batch_errors, batch_network_errors = await process_artist_batch(
                batch_artists, user_services_config, user['id'], services, status_message,
                batch_num + 1, total_batches, processed_count, total_artists
            )

            # Acumular resultados
            all_found_concerts.extend(batch_concerts)
            total_search_errors += batch_errors
            total_network_errors += batch_network_errors
            processed_count += len(batch_artists)

            # Pausa entre lotes para no saturar APIs
            if batch_num < total_batches - 1:  # No pausar después del último lote
                await asyncio.sleep(3.0)

                # Mensaje de pausa entre lotes
                await safe_edit_message(
                    status_message.edit_text,
                    f"🔍 **Búsqueda por lotes en progreso**\n\n"
                    f"✅ **Lote {batch_num + 1}/{total_batches} completado**\n"
                    f"📊 **Progreso total:** {processed_count}/{total_artists} artistas\n"
                    f"🎵 **Conciertos encontrados hasta ahora:** {len(all_found_concerts)}\n"
                    f"❌ **Errores acumulados:** {total_search_errors}\n\n"
                    f"⏸️ **Pausa de 3 segundos entre lotes...**\n"
                    f"🕒 **Tiempo estimado restante:** {(total_batches - batch_num - 1) * 2} minutos",
                    parse_mode='Markdown'
                )

                await asyncio.sleep(2.0)  # Pausa adicional

        # Filtrar por países y fechas futuras
        await safe_edit_message(
            status_message.edit_text,
            f"✅ **Búsqueda por lotes completada!**\n\n"
            f"📦 **Lotes procesados:** {total_batches}\n"
            f"🎵 **Artistas procesados:** {processed_count}\n"
            f"🎪 **Conciertos encontrados:** {len(all_found_concerts)}\n"
            f"❌ **Errores de búsqueda:** {total_search_errors}\n"
            f"🌐 **Errores de red:** {total_network_errors}\n\n"
            f"🌍 **Filtrando por países:** {countries_text}\n"
            f"📅 **Filtrando solo conciertos futuros...**",
            parse_mode='Markdown'
        )

        filtered_concerts = filter_future_concerts_by_countries(
            all_found_concerts,
            user_countries,
            database_path=db.db_path
        )

        # Procesar y enviar resultados
        await process_and_send_concert_results_safe(
            update, status_message, filtered_concerts, processed_count,
            countries_text, services_text, is_search=True,
            error_count=total_search_errors, network_errors=total_network_errors
        )

    except Exception as e:
        logger.error(f"Error en comando search por lotes: {e}")
        await safe_edit_message(
            status_message.edit_text,
            f"❌ **Error en búsqueda por lotes**\n\n"
            f"🎵 **Artistas procesados:** {processed_count}/{total_artists}\n"
            f"🎪 **Conciertos encontrados:** {len(all_found_concerts) if 'all_found_concerts' in locals() else 0}\n"
            f"❌ **Error:** {str(e)[:200]}...\n\n"
            f"💡 **Sugerencias:**\n"
            f"• Los artistas ya procesados se guardaron\n"
            f"• Usa `/show` para ver conciertos guardados\n"
            f"• Inténtalo de nuevo más tarde\n"
            f"• Verifica tu conexión a internet",
            parse_mode='Markdown'
        )


async def process_artist_batch(batch_artists, user_services_config, user_id, services,
                              status_message, batch_num, total_batches, processed_count, total_artists):
    """
    Procesa un lote de artistas con concurrencia controlada

    Returns:
        tuple: (conciertos_encontrados, errores_busqueda, errores_red)
    """
    # Configuración de concurrencia para lotes
    semaphore = asyncio.Semaphore(3)  # Máximo 3 búsquedas simultáneas por lote
    batch_concerts = []
    batch_errors = 0
    batch_network_errors = 0

    # Contador de progreso dentro del lote
    batch_progress = 0
    batch_size = len(batch_artists)

    async def search_artist_in_batch(artist, artist_index):
        """Busca conciertos para un artista dentro del lote"""
        nonlocal batch_progress, batch_errors, batch_network_errors

        async with semaphore:
            try:
                artist_name = artist['name']

                # Pausa para reducir carga de APIs
                await asyncio.sleep(0.3)

                concerts = await search_concerts_for_artist(
                    artist_name,
                    user_services_config,
                    user_id=user_id,
                    services=services,
                    database=db
                )

                # Actualizar progreso dentro del lote cada 5 artistas
                async with asyncio.Lock():
                    batch_progress += 1

                    if batch_progress % 5 == 0 or batch_progress == batch_size:
                        total_processed = processed_count + batch_progress

                        try:
                            await safe_edit_message(
                                status_message.edit_text,
                                f"🔍 **Lote {batch_num}/{total_batches} en progreso**\n\n"
                                f"📦 **Progreso del lote:** {batch_progress}/{batch_size}\n"
                                f"📊 **Progreso total:** {total_processed}/{total_artists}\n"
                                f"🎵 **Último procesado:** {artist_name}\n"
                                f"🎪 **Conciertos encontrados:** {len(concerts)}\n\n"
                                f"⏳ **Completando lote {batch_num}...**",
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            # Error de red al actualizar estado (no crítico)
                            batch_network_errors += 1
                            logger.debug(f"Error actualizando progreso (no crítico): {e}")

                return concerts

            except Exception as e:
                logger.error(f"Error buscando conciertos para {artist['name']}: {e}")
                batch_errors += 1
                return []

    # Ejecutar búsquedas del lote concurrentemente
    tasks = [
        search_artist_in_batch(artist, i)
        for i, artist in enumerate(batch_artists)
    ]

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Procesar resultados del lote
        for result in results:
            if isinstance(result, list):
                batch_concerts.extend(result)
            elif isinstance(result, Exception):
                batch_errors += 1
                logger.error(f"Excepción en lote: {result}")

    except Exception as e:
        logger.error(f"Error crítico en lote {batch_num}: {e}")
        batch_errors += len(batch_artists)  # Marcar todo el lote como error

    logger.info(f"Lote {batch_num} completado: {len(batch_concerts)} conciertos, {batch_errors} errores")

    return batch_concerts, batch_errors, batch_network_errors



async def safe_send_message(send_func, *args, **kwargs):
    """Envía un mensaje de forma segura con reintentos"""
    max_retries = 3
    base_delay = 1

    for attempt in range(max_retries):
        try:
            return await send_func(*args, **kwargs)
        except NetworkError as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)  # Backoff exponencial
                logger.warning(f"Error de red enviando mensaje (intento {attempt + 1}/{max_retries}): {e}")
                logger.info(f"Reintentando en {delay} segundos...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Error definitivo enviando mensaje después de {max_retries} intentos: {e}")
                return None
        except RetryAfter as e:
            if attempt < max_retries - 1:
                delay = e.retry_after + 1
                logger.warning(f"Rate limit, esperando {delay} segundos...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Rate limit persistente después de {max_retries} intentos")
                return None
        except TimedOut as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Timeout enviando mensaje (intento {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Timeout definitivo después de {max_retries} intentos: {e}")
                return None
        except Exception as e:
            logger.error(f"Error inesperado enviando mensaje: {e}")
            return None

    return None


async def safe_edit_message(edit_func, *args, **kwargs):
    """Edita un mensaje de forma segura con reintentos"""
    max_retries = 3
    base_delay = 1

    for attempt in range(max_retries):
        try:
            return await edit_func(*args, **kwargs)
        except NetworkError as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Error de red editando mensaje (intento {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Error definitivo editando mensaje: {e}")
                return None
        except RetryAfter as e:
            if attempt < max_retries - 1:
                delay = e.retry_after + 1
                logger.warning(f"Rate limit editando mensaje, esperando {delay} segundos...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Rate limit persistente editando mensaje")
                return None
        except TimedOut as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Timeout editando mensaje (intento {attempt + 1}/{max_retries}): {e}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Timeout definitivo editando mensaje: {e}")
                return None
        except Exception as e:
            # Para otros errores (como mensaje idéntico), no reintentar
            logger.debug(f"Error editando mensaje (no crítico): {e}")
            return None

    return None


async def process_and_send_concert_results_safe(update, status_message, concerts, processed_count,
                                               countries_text, source_text, is_search=True,
                                               error_count=0, network_errors=0):
    """Procesa y envía los resultados de conciertos con mejor manejo de red - VERSIÓN ACTUALIZADA"""
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
                future_concerts.append(concert)  # Incluir si no se puede parsear
        else:
            future_concerts.append(concert)  # Incluir si no hay fecha

    # Agrupar conciertos por artista
    concerts_by_artist = {}
    for concert in future_concerts:
        artist_name = concert.get('artist_name', 'Artista desconocido')
        if artist_name not in concerts_by_artist:
            concerts_by_artist[artist_name] = []
        concerts_by_artist[artist_name].append(concert)

    # Actualizar mensaje de estado
    await safe_edit_message(
        status_message.edit_text,
        f"✅ **Procesamiento completado!**\n\n"
        f"🎵 **Artistas con conciertos futuros:** {len(concerts_by_artist)}\n"
        f"📅 **Conciertos próximos:** {len(future_concerts)}\n"
        f"🎪 **Total encontrados:** {len(concerts)}\n"
        f"❌ **Errores de búsqueda:** {error_count}\n"
        f"🌐 **Errores de red:** {network_errors}\n"
        f"🌍 **Países:** {countries_text}\n\n"
        f"📤 **Enviando resultados...**",
        parse_mode='Markdown'
    )

    # Enviar un mensaje por cada artista con conciertos futuros
    artists_with_concerts = 0
    messages_sent = 0
    send_errors = 0

    for artist_name, artist_concerts in concerts_by_artist.items():
        if artist_concerts:  # Solo enviar si tiene conciertos futuros
            # Formatear mensaje del artista
            message = format_single_artist_concerts_complete(
                artist_concerts,
                artist_name,
                show_notified=not is_search  # Solo mostrar notificaciones en /show
            )

            # Dividir en chunks si es muy largo
            if len(message) > 4000:
                chunks = split_long_message(message, max_length=4000)
                for i, chunk in enumerate(chunks):
                    result = await safe_send_message(
                        update.message.reply_text,
                        chunk,
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
                    if result:
                        messages_sent += 1
                    else:
                        send_errors += 1

                    # Pausa entre chunks del mismo artista
                    if i < len(chunks) - 1:
                        await asyncio.sleep(0.5)
            else:
                result = await safe_send_message(
                    update.message.reply_text,
                    message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                if result:
                    messages_sent += 1
                else:
                    send_errors += 1

            artists_with_concerts += 1

            # Pausa entre mensajes de diferentes artistas
            await asyncio.sleep(1.0)

            # Si hay muchos errores de envío, pausar más tiempo
            if send_errors > 3:
                logger.warning(f"Detectados {send_errors} errores de envío, aumentando pausa...")
                await asyncio.sleep(3.0)

    # Mensaje final de resumen
    action_text = "búsqueda por lotes"
    if artists_with_concerts == 0:
        suggestion_text = get_no_concerts_suggestions(is_search, countries_text)

        summary_result = await safe_send_message(
            update.message.reply_text,
            f"📭 **No se encontraron conciertos futuros** en tus países configurados ({countries_text}).\n\n"
            f"📊 **Estadísticas de {action_text}:**\n"
            f"• **Artistas procesados:** {processed_count}\n"
            f"• **Conciertos encontrados:** {len(concerts)}\n"
            f"• **Conciertos futuros:** {len(future_concerts)}\n"
            f"• **Errores de búsqueda:** {error_count}\n"
            f"• **Errores de red:** {network_errors}\n"
            f"• **Fuente:** {source_text}\n\n"
            f"{suggestion_text}",
            parse_mode='Markdown'
        )
    else:
        summary_message = (
            f"🎉 **Resultados de {action_text}**\n\n"
            f"📊 **Artistas con conciertos futuros:** {artists_with_concerts}\n"
            f"📅 **Total de conciertos próximos:** {len(future_concerts)}\n"
            f"📤 **Mensajes enviados exitosamente:** {messages_sent}\n"
            f"❌ **Errores de búsqueda:** {error_count}\n"
            f"🌐 **Errores de red:** {network_errors}\n"
            f"📨 **Errores de envío:** {send_errors}\n"
            f"🔧 **Fuente:** {source_text}\n"
            f"🌍 **Países consultados:** {countries_text}\n\n"
            f"💡 **Comandos útiles:**\n"
            f"• `/show` - Ver conciertos guardados\n"
            f"• `/showartist <nombre>` - Ver todos los conciertos de un artista\n"
            f"• `/addcountry <país>` - Añadir más países"
        )

        if network_errors > 0 or send_errors > 0:
            summary_message += f"\n\n⚠️ **Nota:** Se detectaron errores de comunicación. Algunos datos pueden no haberse enviado correctamente."

        summary_result = await safe_send_message(
            update.message.reply_text,
            summary_message,
            parse_mode='Markdown'
        )

    # Actualizar mensaje de estado final
    final_status = f"✅ **{action_text.capitalize()} completada**\n\n"
    final_status += f"🎵 **Artistas con conciertos:** {artists_with_concerts}\n"
    final_status += f"📅 **Conciertos futuros:** {len(future_concerts)}\n"
    final_status += f"📤 **Mensajes enviados:** {messages_sent}"

    if error_count > 0:
        final_status += f"\n❌ **Errores de búsqueda:** {error_count}"
    if network_errors > 0:
        final_status += f"\n🌐 **Errores de red:** {network_errors}"
    if send_errors > 0:
        final_status += f"\n📨 **Errores de envío:** {send_errors}"

    await safe_edit_message(status_message.edit_text, final_status, parse_mode='Markdown')


async def search_concerts_for_artist_async(artist_name, user_services_config, user_id=None, services=None, database=None):
    """
    Versión asíncrona de search_concerts_for_artist
    Esta función debe ser implementada para reemplazar la versión síncrona
    """
    # Crear tareas asíncronas para cada servicio
    tasks = []

    if user_services_config.get('ticketmaster', True) and services.get('ticketmaster_service'):
        tasks.append(search_ticketmaster_async(artist_name, user_services_config, services))

    if user_services_config.get('spotify', True) and services.get('spotify_service'):
        tasks.append(search_spotify_async(artist_name, user_services_config, services))

    if user_services_config.get('setlistfm', True) and services.get('setlistfm_service'):
        tasks.append(search_setlistfm_async(artist_name, user_services_config, services))

    # Ejecutar todas las búsquedas concurrentemente
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combinar resultados exitosos
        all_concerts = []
        for result in results:
            if isinstance(result, list):
                all_concerts.extend(result)
            else:
                logger.error(f"Error en búsqueda de servicio: {result}")

        # Guardar conciertos en base de datos de forma asíncrona
        if database and all_concerts:
            await save_concerts_async(database, all_concerts)

        return all_concerts

    return []

async def error_handler_improved(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Maneja errores de forma que no afecte a otros usuarios - VERSIÓN MEJORADA"""
    logger.error("Exception while handling an update:", exc_info=context.error)

    # Si hay un update válido, informar al usuario específico
    if update and hasattr(update, 'effective_chat'):
        try:
            error_message = "❌ Error temporal. "

            # Personalizar mensaje según el tipo de error
            if isinstance(context.error, NetworkError):
                error_message += "Problema de conexión detectado. Inténtalo de nuevo en unos segundos."
            elif isinstance(context.error, RetryAfter):
                retry_after = getattr(context.error, 'retry_after', 30)
                error_message += f"Límite de velocidad alcanzado. Espera {retry_after} segundos."
            elif isinstance(context.error, TimedOut):
                error_message += "Tiempo de espera agotado. Inténtalo de nuevo."
            else:
                error_message += "Inténtalo de nuevo en unos segundos."

            await safe_send_message(
                context.bot.send_message,
                chat_id=update.effective_chat.id,
                text=error_message
            )
        except Exception:
            pass  # Ignorar errores al enviar mensaje de error






async def save_concerts_async(database, concerts):
    """Guarda conciertos en base de datos de forma asíncrona"""
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: [database.save_concert(concert) for concert in concerts]
        )
    except Exception as e:
        logger.error(f"Error guardando conciertos: {e}")


async def show_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Comando /show - muestra conciertos futuros de artistas seguidos desde la base de datos"""
    chat_id = update.effective_chat.id

    # Verificar que el usuario esté registrado
    user = db.get_user_by_chat_id(chat_id)
    if not user:
        await update.message.reply_text(
            "❌ Primero debes registrarte con `/adduser <tu_nombre>`"
        )
        return

    # Obtener configuración del usuario
    user_services_config = user_services.get_user_services(user['id'])

    # Manejar caso donde user_services puede ser None
    if not user_services_config:
        user_services_config = {
            'countries': {'ES'},
            'country_filter': 'ES'
        }

    # Verificar que tenga países configurados
    user_countries = user_services_config.get('countries', set())
    if not user_countries:
        # Usar país por defecto si no tiene configurado
        country_filter = user_services_config.get('country_filter', 'ES')
        user_countries = {country_filter}

    # Mensaje de estado inicial
    countries_text = ", ".join(sorted(user_countries))
    status_message = await update.message.reply_text(
        f"📊 Consultando conciertos de tus artistas seguidos...\n"
        f"🌍 Países configurados: {countries_text}\n"
        f"📂 Consultando base de datos..."
    )

    try:
        # Obtener artistas seguidos
        followed_artists = db.get_user_followed_artists(user['id'])

        if not followed_artists:
            await status_message.edit_text(
                "📭 No tienes artistas seguidos aún.\n"
                "Usa `/addartist <nombre>` para seguir artistas.\n"
                "Usa `/search` para buscar nuevos conciertos de tus artistas."
            )
            return

        # Obtener TODOS los conciertos de los artistas seguidos desde la base de datos
        conn = db.get_connection()
        cursor = conn.cursor()

        artist_names = [artist['name'] for artist in followed_artists]
        placeholders = ','.join(['?' for _ in artist_names])

        cursor.execute(f"""
            SELECT DISTINCT c.*
            FROM concerts c
            WHERE LOWER(c.artist_name) IN ({','.join(['LOWER(?)' for _ in artist_names])})
            ORDER BY c.date ASC
        """, artist_names)

        rows = cursor.fetchall()
        all_concerts = [dict(row) for row in rows]
        conn.close()

        await status_message.edit_text(
            f"📊 Encontrados {len(all_concerts)} conciertos en base de datos\n"
            f"🌍 Filtrando por países: {countries_text}\n"
            f"📅 Filtrando solo conciertos futuros..."
        )

        # Filtrar solo conciertos futuros y por países
        future_concerts = filter_future_concerts_by_countries(all_concerts, user_countries)

        # Procesar y enviar resultados
        await process_and_send_concert_results(
            update, status_message, future_concerts, len(followed_artists), countries_text, "Base de datos", is_search=False
        )

    except Exception as e:
        logger.error(f"Error en comando show: {e}")
        await status_message.edit_text(
            f"❌ Error al consultar conciertos. Inténtalo de nuevo más tarde.\n"
            f"Error: {str(e)[:100]}..."
        )


async def process_and_send_concert_results(update, status_message, concerts, processed_count, countries_text, source_text, is_search=True):
    """Procesa y envía los resultados de conciertos de manera consistente"""
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
                future_concerts.append(concert)  # Incluir si no se puede parsear
        else:
            future_concerts.append(concert)  # Incluir si no hay fecha

    # Agrupar conciertos por artista
    concerts_by_artist = {}
    for concert in future_concerts:
        artist_name = concert.get('artist_name', 'Artista desconocido')
        if artist_name not in concerts_by_artist:
            concerts_by_artist[artist_name] = []
        concerts_by_artist[artist_name].append(concert)

    # Actualizar mensaje de estado
    await status_message.edit_text(
        f"✅ Procesamiento completado!\n"
        f"🎵 {len(concerts_by_artist)} artistas con conciertos futuros\n"
        f"📅 {len(future_concerts)} conciertos próximos\n"
        f"🌍 {countries_text}\n\n"
        f"📤 Enviando resultados..."
    )

    # Enviar un mensaje por cada artista con conciertos futuros
    artists_with_concerts = 0
    messages_sent = 0

    for artist_name, artist_concerts in concerts_by_artist.items():
        if artist_concerts:  # Solo enviar si tiene conciertos futuros
            # Formatear mensaje del artista
            message = format_single_artist_concerts_complete(
                artist_concerts,
                artist_name,
                show_notified=not is_search  # Solo mostrar notificaciones en /show
            )

            # Dividir en chunks si es muy largo
            if len(message) > 4000:
                chunks = split_long_message(message, max_length=4000)
                for i, chunk in enumerate(chunks):
                    await update.message.reply_text(
                        chunk,
                        parse_mode='Markdown',
                        disable_web_page_preview=True
                    )
                    messages_sent += 1
                    # Pausa entre chunks del mismo artista
                    if i < len(chunks) - 1:
                        await asyncio.sleep(0.5)
            else:
                await update.message.reply_text(
                    message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                messages_sent += 1

            artists_with_concerts += 1

            # Pausa entre mensajes de diferentes artistas
            await asyncio.sleep(1.0)

    # Mensaje final de resumen
    action_text = "búsqueda activa" if is_search else "consulta"
    if artists_with_concerts == 0:
        suggestion_text = get_no_concerts_suggestions(is_search, countries_text)

        await update.message.reply_text(
            f"📭 No se encontraron conciertos futuros en tus países configurados ({countries_text}).\n\n"
            f"📊 Estadísticas de {action_text}:\n"
            f"• Artistas procesados: {processed_count}\n"
            f"• Conciertos encontrados: {len(concerts)}\n"
            f"• Conciertos futuros: {len(future_concerts)}\n"
            f"• Fuente: {source_text}\n\n"
            f"{suggestion_text}"
        )
    else:
        summary_message = (
            f"🎉 *Resultados de {action_text}*\n\n"
            f"📊 Artistas con conciertos futuros: {artists_with_concerts}\n"
            f"📅 Total de conciertos próximos: {len(future_concerts)}\n"
            f"📤 Mensajes enviados: {messages_sent}\n"
            f"🔧 Fuente: {source_text}\n"
            f"🌍 Países consultados: {countries_text}\n\n"
            f"💡 Comandos útiles:\n"
            f"• `/search` - Buscar nuevos conciertos\n" if not is_search else "• `/show` - Ver conciertos guardados\n"
            f"• `/showartist <nombre>` - Ver todos los conciertos de un artista\n"
            f"• `/addcountry <país>` - Añadir más países"
        )
        await update.message.reply_text(
            summary_message,
            parse_mode='Markdown'
        )

    # Actualizar mensaje de estado final
    await status_message.edit_text(
        f"✅ {action_text.capitalize()} completada\n"
        f"🎵 {artists_with_concerts} artistas con conciertos\n"
        f"📅 {len(future_concerts)} conciertos futuros\n"
        f"📤 {messages_sent} mensajes enviados"
    )


# ===========================
# MUSPY
# ===========================

async def muspy_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks de Muspy - VERSIÓN INTEGRADA"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    logger.info(f"Muspy callback recibido: {callback_data}")

    # Parsear callback data
    parts = callback_data.split("_")
    if len(parts) < 3 or parts[0] != "muspy":
        await query.edit_message_text("❌ Callback no válido.")
        return

    action = parts[1]

    # Obtener user_id del final
    try:
        user_id = int(parts[-1])
    except (ValueError, IndexError):
        await query.edit_message_text("❌ Error de usuario.")
        return

    # Verificar que el usuario existe
    user = db.get_user_by_chat_id(query.message.chat_id)
    if not user or user['id'] != user_id:
        await query.edit_message_text("❌ Error de autenticación.")
        return

    try:
        if action == "menu":
            # Volver al menú principal de Muspy
            fake_update = type('obj', (object,), {
                'message': query.message,
                'callback_query': query,
                'effective_chat': query.message.chat
            })()
            await muspy_handlers.muspy_command(fake_update, context)

        elif action == "artists" and len(parts) > 2 and parts[2] == "page":
            # Manejar paginación de artistas
            page = int(parts[3]) if len(parts) > 3 else 0

            if user_id in muspy_handlers.user_artists_cache:
                artists = muspy_handlers.user_artists_cache[user_id]
                await muspy_handlers._show_artists_page(query, user_id, artists, page)
            else:
                await query.edit_message_text("❌ Lista de artistas expirada. Usa `/muspy` de nuevo.")

        elif action == "current" and parts[1] == "current" and parts[2] == "page":
            # No hacer nada si presiona el botón de página actual
            return

        else:
            # Delegar al handler principal de Muspy
            await muspy_handlers.muspy_callback_handler(update, context)

    except Exception as e:
        logger.error(f"Error en muspy_callback_handler: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await query.edit_message_text("❌ Error procesando la solicitud.")



# ===========================
# CALENDARIO
# ===========================

async def calendar_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Maneja todos los callbacks de calendario"""
    query = update.callback_query
    await query.answer()

    callback_data = query.data
    logger.info(f"Calendar callback recibido: {callback_data}")

    # Parsear callback data
    parts = callback_data.split("_")
    if len(parts) != 3 or parts[0] != "cal":
        await query.edit_message_text("❌ Callback no válido.")
        return

    action = parts[1]
    user_id = int(parts[2])

    # Verificar que el usuario existe
    user = db.get_user_by_chat_id(query.message.chat_id)
    if not user or user['id'] != user_id:
        await query.edit_message_text("❌ Error de autenticación.")
        return

    try:
        # Delegar al handler principal de calendario
        await calendar_handlers.cal_callback_handler(update, context)

    except Exception as e:
        logger.error(f"Error en calendar_callback_handler: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await query.edit_message_text("❌ Error procesando la solicitud.")



# ===========================
# FUNCIÓN PRINCIPAL
# ===========================

def main():
    """Función principal MODIFICADA para soportar concurrencia real"""
    global db, user_services, application, muspy_service, muspy_handlers

    # Configuración
    TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_CONCIERTOS')
    DB_PATH = os.getenv('DB_PATH', 'artist_tracker.db')
    CACHE_DIR = os.getenv('CACHE_DIR', './cache')

    if not TELEGRAM_TOKEN:
        logger.error("❌ No se ha configurado TELEGRAM_BOT_CONCIERTOS en las variables de entorno")
        return

    # MEJORA: Inicializar base de datos normal y luego añadir wrapper concurrente
    db = ArtistTrackerDatabase(DB_PATH)

    # Añadir funciones de wrapper concurrente al archivo database.py
    from database import DatabaseConcurrentWrapper
    db = DatabaseConcurrentWrapper(db)
    logger.info("✅ Base de datos inicializada con wrapper thread-safe")

    db.init_muspy_tables()

    # Inicializar servicios de usuario
    user_services = UserServices(db)

    # Inicializar servicios de conciertos
    initialize_concert_services()

    # Inicializar servicio de países
    initialize_country_service(DB_PATH)

    # Obtener referencia al servicio inicializado
    services = get_services()
    global country_state_city
    country_state_city = services.get('country_state_city')

    # Inicializar servicio de Last.fm
    initialize_lastfm_service()

    # Configurar MusicBrainz si está disponible
    user_agent = {
        "app": "MusicLiveShowsTrackerBot",
        "version": "0.1",
        "contact": "frodobolson+server@disroot.org"
    }

    try:
        from apis.mb_artist_info import setup_musicbrainz
        setup_musicbrainz(user_agent=user_agent, cache_directory=CACHE_DIR)
        logger.info("✅ MusicBrainz configurado correctamente")
    except Exception as e:
        logger.warning(f"MusicBrainz no disponible: {e}")

    # Inicializar servicio de Muspy
    muspy_service = MuspyService()
    muspy_handlers = MuspyHandlers(db, muspy_service)

    # Inicializar handlers de calendario
    global calendar_handlers
    calendar_handlers = CalendarHandlers(db, muspy_service)

    # Validar servicios
    validate_services()

    # MEJORA: Crear la aplicación con configuración optimizada para concurrencia
    application = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .concurrent_updates(256)  # Permitir hasta 256 updates concurrentes
        .build()
    )

    # ConversationHandler para el login de Muspy
    muspy_login_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(
            muspy_handlers._start_muspy_login,
            pattern="^muspy_login_"
        )],
        states={
            MUSPY_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, muspy_handlers.login_email_handler)],
            MUSPY_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, muspy_handlers.login_password_handler)],
            MUSPY_USERID: [MessageHandler(filters.TEXT & ~filters.COMMAND, muspy_handlers.login_userid_handler)],
        },
        fallbacks=[CommandHandler('cancel', muspy_handlers.cancel_login)],
        per_chat=True,
        per_user=True
    )

    # Handlers de comandos básicos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("commands", commands_command))
    application.add_handler(CommandHandler("adduser", adduser_command))
    application.add_handler(CommandHandler("addartist", addartist_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("remove", remove_command))
    application.add_handler(CommandHandler("notify", notify_command))
    application.add_handler(CommandHandler("playlist", playlist_command))

    # Handler de comando calendario
    application.add_handler(CommandHandler("cal", calendar_handlers.cal_command))

    # MEJORA: Handlers de búsqueda con la nueva versión concurrente
    application.add_handler(CommandHandler("search", search_command))  # Usa la nueva versión concurrente
    application.add_handler(CommandHandler("show", show_command))
    application.add_handler(CommandHandler("searchartist", searchartist_command))
    application.add_handler(CommandHandler("showartist", showartist_command))

    # Resto de handlers (sin cambios)...
    application.add_handler(CommandHandler("serviceon", serviceon_command))
    application.add_handler(CommandHandler("serviceoff", serviceoff_command))
    application.add_handler(CommandHandler("country", country_command))
    application.add_handler(CommandHandler("addcountry", addcountry_command))
    application.add_handler(CommandHandler("removecountry", removecountry_command))
    application.add_handler(CommandHandler("mycountries", mycountries_command))
    application.add_handler(CommandHandler("listcountries", listcountries_command))
    application.add_handler(CommandHandler("refreshcountries", refreshcountries_command))
    application.add_handler(CommandHandler("config", config_command))
    application.add_handler(CommandHandler("lastfm", lastfm_command))
    application.add_handler(CommandHandler("spotify", spotify_command))

    # ConversationHandler para login de Muspy
    application.add_handler(muspy_login_conv_handler)
    application.add_handler(CommandHandler("muspy", muspy_handlers.muspy_command))
    application.add_handler(CallbackQueryHandler(muspy_callback_handler, pattern="^muspy_"))

    # Callbacks específicos de países
    application.add_handler(CallbackQueryHandler(country_selection_callback, pattern="^(select_country_|cancel_country_selection)"))
    application.add_handler(CallbackQueryHandler(continent_selection_callback, pattern="^continent_"))
    application.add_handler(CallbackQueryHandler(back_to_continents_callback, pattern="^back_to_continents"))

    # Handlers de callbacks específicos (ORDEN IMPORTANTE)
    application.add_handler(CallbackQueryHandler(artist_selection_callback, pattern="^(select_artist_|cancel_artist_selection)"))
    application.add_handler(CallbackQueryHandler(list_page_callback, pattern="^list_page_"))
    application.add_handler(CallbackQueryHandler(lastfm_callback_handler, pattern="^lastfm_"))
    application.add_handler(CallbackQueryHandler(spotify_callback_handler, pattern="^spotify_"))

    # Callbacks de calendario (DESPUÉS de muspy_callback_handler)
    application.add_handler(CallbackQueryHandler(calendar_callback_handler, pattern="^cal_"))

    # Callback para página actual (no hace nada, solo evita errores)
    application.add_handler(CallbackQueryHandler(
        lambda update, context: update.callback_query.answer(),
        pattern="^(current_list_page|current_lastfm_page|current_spotify_page)$"
    ))

    # Handler genérico de configuración (DEBE IR AL FINAL de los callbacks)
    application.add_handler(CallbackQueryHandler(config_callback_handler, pattern="^(config_|notif_|country_|service_|artist_)"))

    # Handler de texto (DEBE SER EL ÚLTIMO)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_input))

    # MEJORA: Configurar manejo de errores global para concurrencia
    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Maneja errores de forma que no afecte a otros usuarios"""
        logger.error("Exception while handling an update:", exc_info=context.error)

        # Si hay un update válido, informar al usuario específico
        if update and hasattr(update, 'effective_chat'):
            try:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text="❌ Error temporal. Inténtalo de nuevo en unos segundos."
                )
            except Exception:
                pass  # Ignorar errores al enviar mensaje de error

    application.add_error_handler(error_handler_improved)

    # Iniciar el bot
    services = get_services()
    logger.info("🤖 Bot de seguimiento de artistas iniciado con soporte CONCURRENTE.")
    logger.info("✅ Múltiples usuarios pueden usar comandos simultáneamente")
    if services.get('country_state_city'):
        logger.info("✅ Sistema de países múltiples activado")
    else:
        logger.info("⚠️ Sistema de países múltiples no disponible (falta API key)")

    logger.info("🔔 Para notificaciones, ejecuta: python notification_scheduler.py")
    logger.info("⚡ Máximo 256 updates concurrentes configurados")
    logger.info("🗄️ Pool de 10 conexiones de base de datos para concurrencia")
    logger.info("Presiona Ctrl+C para detenerlo.")

    try:
        # MEJORA: Usar polling optimizado para concurrencia
        application.run_polling(
            allowed_updates=['message', 'callback_query'],  # Solo los updates que necesitamos
            drop_pending_updates=True,  # Descartar updates pendientes al reiniciar
            timeout=30,  # Timeout más alto para mejor estabilidad
            poll_interval=0.1  # Polling más frecuente para mejor responsividad
        )
    except KeyboardInterrupt:
        logger.info("🛑 Bot detenido por el usuario")
        # Cerrar pool de conexiones limpiamente
        if hasattr(db, 'close_pool'):
            db.close_pool()
            logger.info("✅ Pool de conexiones cerrado")
    except Exception as e:
        logger.error(f"❌ Error crítico en el bot: {e}")
        # Cerrar pool de conexiones en caso de error
        if hasattr(db, 'close_pool'):
            db.close_pool()

if __name__ == "__main__":
    main()
