#!/usr/bin/env python3
"""
Módulo de funciones auxiliares para handlers
Contiene funciones de soporte para los callbacks y handlers de Telegram
"""

import asyncio
import logging
import json
import re
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import unquote
from telegram import InlineKeyboardButton, InlineKeyboardMarkup


logger = logging.getLogger(__name__)



# ===========================
# FUNCIONES DE NOTIFICACIONES
# ===========================

async def handle_notification_callback(query, action: str, user_id: int, context, user_services):
    """Maneja callbacks específicos de notificaciones"""
    if action == "on":
        # Activar notificaciones
        conn = user_services.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE users SET notification_enabled = 1 WHERE id = ?", (user_id,))
            conn.commit()
            success = cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error activando notificaciones: {e}")
            success = False
        finally:
            conn.close()

        message = "✅ Notificaciones activadas correctamente." if success else "❌ Error al activar notificaciones."
        keyboard = [[{"text": "🔙 Volver al menú", "callback_data": f"config_back_{user_id}"}]]

    elif action == "off":
        # Desactivar notificaciones
        conn = user_services.db.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("UPDATE users SET notification_enabled = 0 WHERE id = ?", (user_id,))
            conn.commit()
            success = cursor.rowcount > 0
        except Exception as e:
            logger.error(f"Error desactivando notificaciones: {e}")
            success = False
        finally:
            conn.close()

        message = "❌ Notificaciones desactivadas." if success else "❌ Error al desactivar notificaciones."
        keyboard = [[{"text": "🔙 Volver al menú", "callback_data": f"config_back_{user_id}"}]]

    elif action == "time":
        # Solicitar nueva hora
        message = (
            "⏰ *Cambiar hora de notificación*\n\n"
            "Envía la nueva hora en formato HH:MM\n"
            "Ejemplo: 09:00, 14:30, 20:15\n\n"
            "Responde a este mensaje con la hora deseada."
        )
        keyboard = [[{"text": "❌ Cancelar", "callback_data": f"config_back_{user_id}"}]]

        # Guardar estado para esperar respuesta
        context.user_data['waiting_for_time'] = user_id

    return message, keyboard

# ===========================
# FUNCIONES DE PAÍSES
# ===========================

async def handle_country_callback(query, action: str, user_id: int, parts: list, context, services):
    """Maneja callbacks específicos de países"""
    country_state_city = services.get('country_state_city')

    if action == "add":
        message = (
            "➕ *Añadir país*\n\n"
            "Envía el código o nombre del país que quieres añadir.\n"
            "Ejemplos: ES, Spain, FR, France\n\n"
            "Responde a este mensaje con el país deseado."
        )
        context.user_data['waiting_for_country_add'] = user_id
        keyboard = [[{"text": "❌ Cancelar", "callback_data": f"config_countries_{user_id}"}]]

    elif action == "remove":
        if country_state_city:
            user_countries = country_state_city.get_user_countries(user_id)
            if not user_countries:
                message = "❌ No tienes países configurados para eliminar."
                keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_countries_{user_id}"}]]
            elif len(user_countries) == 1:
                message = "❌ No puedes eliminar tu último país configurado."
                keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_countries_{user_id}"}]]
            else:
                message = "➖ *Eliminar país*\n\nSelecciona el país a eliminar:"
                keyboard = []
                for country in user_countries:
                    keyboard.append([{
                        "text": f"❌ {country['name']} ({country['code']})",
                        "callback_data": f"country_delete_{country['code']}_{user_id}"
                    }])
                keyboard.append([{"text": "🔙 Cancelar", "callback_data": f"config_countries_{user_id}"}])
        else:
            message = "❌ Sistema de países múltiples no disponible."
            keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_countries_{user_id}"}]]

    elif action == "list":
        message = (
            "📋 *Países disponibles*\n\n"
            "Usa `/listcountries` para ver la lista completa de países disponibles."
        )
        keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_countries_{user_id}"}]]

    elif action == "delete":
        # Manejar eliminación de país específico
        if len(parts) >= 4:
            country_code = parts[2]
            if country_state_city:
                success = country_state_city.remove_user_country(user_id, country_code)
                if success:
                    country_info = country_state_city.get_country_info(country_code)
                    country_name = country_info['name'] if country_info else country_code
                    message = f"✅ País {country_name} ({country_code}) eliminado correctamente."
                else:
                    message = f"❌ Error al eliminar el país {country_code}."
            else:
                message = "❌ Sistema de países múltiples no disponible."
        else:
            message = "❌ Error en la eliminación del país."

        keyboard = [[{"text": "🔙 Volver al menú", "callback_data": f"config_countries_{user_id}"}]]

    return message, keyboard

# ===========================
# FUNCIONES DE SERVICIOS
# ===========================

async def handle_service_callback(query, action: str, user_id: int, parts: list, user_services):
    """Maneja callbacks específicos de servicios"""
    services_config = user_services.get_user_services(user_id)
    services = ['ticketmaster', 'spotify', 'setlistfm']

    if action == "activate":
        # Mostrar servicios inactivos para activar
        inactive_services = [s for s in services if not services_config.get(s, True)]

        if not inactive_services:
            message = "✅ Todos los servicios ya están activos."
            keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_services_{user_id}"}]]
        else:
            message = "✅ *Activar servicio*\n\nSelecciona el servicio a activar:"
            keyboard = []
            for i, service in enumerate(inactive_services, 1):
                keyboard.append([{
                    "text": f"{i}. {service.capitalize()}",
                    "callback_data": f"service_enable_{service}_{user_id}"
                }])
            keyboard.append([{"text": "🔙 Cancelar", "callback_data": f"config_services_{user_id}"}])

    elif action == "deactivate":
        # Mostrar servicios activos para desactivar
        active_services = [s for s in services if services_config.get(s, True)]

        if len(active_services) <= 1:
            message = "❌ Debes mantener al menos un servicio activo."
            keyboard = [[{"text": "🔙 Volver", "callback_data": f"config_services_{user_id}"}]]
        else:
            message = "❌ *Desactivar servicio*\n\nSelecciona el servicio a desactivar:"
            keyboard = []
            for i, service in enumerate(active_services, 1):
                keyboard.append([{
                    "text": f"{i}. {service.capitalize()}",
                    "callback_data": f"service_disable_{service}_{user_id}"
                }])
            keyboard.append([{"text": "🔙 Cancelar", "callback_data": f"config_services_{user_id}"}])

    elif action == "enable" or action == "disable":
        # Procesar activar/desactivar servicio específico
        if len(parts) >= 4:
            service = parts[2]
            success = user_services.set_service_status(user_id, service, action == "enable")
            action_text = "activado" if action == "enable" else "desactivado"

            if success:
                message = f"✅ Servicio {service.capitalize()} {action_text} correctamente."
            else:
                message = f"❌ Error al modificar el servicio {service.capitalize()}."
        else:
            message = "❌ Error en la operación del servicio."

        keyboard = [[{"text": "🔙 Volver al menú", "callback_data": f"config_services_{user_id}"}]]

    return message, keyboard

# ===========================
# FUNCIONES DE LAST.FM
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



async def handle_lastfm_do_sync(query, user: Dict, period: str, database, services):
    """Realiza la sincronización de artistas de Last.fm usando MBID cuando esté disponible"""
    lastfm_service = services.get('lastfm_service')

    # Obtener artistas pendientes
    artists = database.get_pending_lastfm_sync(user['id'], period)
    if not artists:
        await query.edit_message_text("❌ No hay sincronización pendiente.")
        return

    period_name = lastfm_service.get_period_display_name(period) if lastfm_service else period

    # Mensaje de estado
    await query.edit_message_text(
        f"⏳ Sincronizando {len(artists)} artistas de Last.fm...\n"
        f"Esto puede tardar un momento."
    )

    try:
        added_count = 0
        skipped_count = 0
        error_count = 0
        mbid_used_count = 0
        mbid_available_count = 0

        total_artists = len(artists)
        processed = 0

        for artist_data in artists:
            artist_name = artist_data.get('name', '')
            artist_mbid = artist_data.get('mbid', '')

            processed += 1

            # Actualizar mensaje de progreso cada 5 artistas
            if processed % 5 == 0 or processed == total_artists:
                progress_msg = (
                    f"⏳ Sincronizando {total_artists} artistas de Last.fm...\n"
                    f"Progreso: {processed}/{total_artists}\n"
                    f"✅ Añadidos: {added_count} | ⏭️ Ya seguidos: {skipped_count} | ❌ Errores: {error_count}"
                )
                try:
                    await query.edit_message_text(progress_msg)
                except:
                    pass  # Ignorar errores de edición (rate limit)

            if not artist_name:
                error_count += 1
                continue

            try:
                artist_id = None

                # Estrategia 1: Si tenemos MBID, intentar usarlo directamente
                if artist_mbid:
                    mbid_available_count += 1
                    artist_id = database.get_artist_by_mbid(artist_mbid)

                    if artist_id:
                        logger.debug(f"✅ Artista encontrado por MBID: {artist_name} ({artist_mbid})")
                        mbid_used_count += 1
                    else:
                        # Crear artista usando MBID directamente
                        candidate = {
                            'mbid': artist_mbid,
                            'name': artist_name,
                            'type': '',
                            'country': '',
                            'disambiguation': '',
                            'score': 100  # Score alto porque viene de Last.fm
                        }

                        # Añadir información extra de Last.fm si está disponible
                        if 'genres' in artist_data:
                            candidate['genres'] = artist_data['genres']
                        if 'listeners' in artist_data:
                            candidate['listeners'] = artist_data['listeners']

                        artist_id = database.create_artist_from_candidate(candidate)
                        if artist_id:
                            logger.debug(f"✅ Artista creado con MBID: {artist_name} ({artist_mbid})")
                            mbid_used_count += 1

                # Estrategia 2: Si no hay MBID o falló, usar búsqueda tradicional
                if not artist_id:
                    candidates = database.search_artist_candidates(artist_name)

                    if not candidates:
                        skipped_count += 1
                        logger.debug(f"⚠️ No se encontraron candidatos para: {artist_name}")
                        continue

                    # Usar el mejor candidato
                    best_candidate = candidates[0]
                    artist_id = database.create_artist_from_candidate(best_candidate)

                    if artist_id:
                        logger.debug(f"✅ Artista creado por búsqueda: {artist_name}")

                if not artist_id:
                    error_count += 1
                    logger.debug(f"❌ Error creando artista: {artist_name}")
                    continue

                # Añadir a seguimiento
                was_new = database.add_followed_artist(user['id'], artist_id)

                if was_new:
                    added_count += 1
                else:
                    skipped_count += 1  # Ya lo seguía

                # Pausa breve para no sobrecargar
                await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error procesando artista {artist_name}: {e}")
                error_count += 1
                continue

        # Limpiar sincronización pendiente
        database.clear_pending_lastfm_sync(user['id'], period)

        # Mensaje de resultado detallado
        message = (
            f"✅ *Sincronización de Last.fm completada*\n\n"
            f"📊 Período: {period_name}\n"
            f"➕ Artistas añadidos: {added_count}\n"
            f"⏭️ Ya seguidos: {skipped_count}\n"
        )

        if error_count > 0:
            message += f"❌ Errores: {error_count}\n"

        message += f"\n🎯 *Estadísticas de MBID:*\n"
        message += f"📋 Artistas con MBID: {mbid_available_count}/{total_artists}\n"
        message += f"🎵 Sincronizados via MBID: {mbid_used_count}\n"

        # Calcular porcentaje de éxito
        success_rate = ((added_count + skipped_count) / total_artists) * 100 if total_artists > 0 else 0
        message += f"📈 Tasa de éxito: {success_rate:.1f}%\n"

        message += f"\nUsa `/list` para ver todos tus artistas seguidos."

        keyboard = [[{"text": "🔙 Volver a Last.fm", "callback_data": f"lastfm_menu_{user['id']}"}]]

        return message, keyboard

    except Exception as e:
        logger.error(f"Error en sincronización de Last.fm: {e}")
        return "❌ Error durante la sincronización. Inténtalo de nuevo.", []

async def handle_lastfm_change_limit(query, user: Dict, context):
    """Maneja el cambio de límite de sincronización"""
    message = (
        "🔢 *Cambiar cantidad de artistas*\n\n"
        "Envía el número de artistas que quieres sincronizar por período.\n"
        "Rango permitido: 5-10000 artistas\n\n"
        "Ejemplo: 50"
    )

    keyboard = [[{"text": "❌ Cancelar", "callback_data": f"lastfm_cancel_{user['id']}"}]]

    # Marcar que estamos esperando el límite
    context.user_data['waiting_for_lastfm_limit'] = user['id']

    return message, keyboard

async def handle_lastfm_change_user(query, user: Dict, context):
    """Maneja el cambio de usuario de Last.fm"""
    message = (
        "👤 *Cambiar usuario de Last.fm*\n\n"
        "Envía tu nuevo nombre de usuario de Last.fm:"
    )

    keyboard = [[{"text": "❌ Cancelar", "callback_data": f"lastfm_cancel_{user['id']}"}]]

    # Marcar que estamos esperando el nuevo usuario
    context.user_data['waiting_for_lastfm_change_user'] = user['id']

    return message, keyboard

# ===========================
# FUNCIONES DE SPOTIFY
# ===========================

async def handle_spotify_authentication(query, user: Dict, services):
    """Inicia el proceso de autenticación OAuth"""
    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    try:
        # Generar URL de autenticación
        auth_url = spotify_service.generate_auth_url(user['id'])

        if not auth_url:
            await query.edit_message_text(
                "❌ Error generando URL de autenticación.\n"
                "Verifica que las credenciales de Spotify estén configuradas."
            )
            return

        # Crear mensaje con instrucciones
        message = (
            "🔐 *Autenticación de Spotify*\n\n"
            "Para conectar tu cuenta de Spotify:\n\n"
            "1️⃣ Abre este enlace en tu navegador:\n"
            f"[🔗 Autenticar con Spotify]({auth_url})\n\n"
            "2️⃣ Inicia sesión con tu cuenta de Spotify\n\n"
            "3️⃣ Acepta los permisos solicitados\n\n"
            "4️⃣ Serás redirigido a una página. Copia el *código* que aparece en la URL "
            "(el texto después de 'code=' y antes de '&') y envíamelo aquí.\n\n"
            "⏰ *Tienes 30 minutos para completar este proceso.*"
        )

        keyboard = [
            [{"text": "🔗 Abrir enlace", "url": auth_url}],
            [{"text": "❌ Cancelar", "callback_data": f"spotify_cancel_{user['id']}"}]
        ]

        return message, keyboard, auth_url

    except Exception as e:
        logger.error(f"Error en autenticación Spotify: {e}")
        return "❌ Error iniciando autenticación. Inténtalo de nuevo.", [], None

async def handle_spotify_real_artists(query, user: Dict, services, database):
    """Maneja mostrar artistas realmente seguidos (con OAuth) - VERSIÓN CON DEBUG"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== INICIANDO handle_spotify_real_artists para usuario {user['id']} ===")

    spotify_service = services.get('spotify_service')
    logger.info(f"Spotify service disponible: {spotify_service is not None}")

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    logger.info(f"Verificando autenticación para usuario {user['id']}")
    is_authenticated = spotify_service.is_user_authenticated(user['id'])
    logger.info(f"Usuario autenticado: {is_authenticated}")

    if not is_authenticated:
        await query.edit_message_text(
            "❌ No estás autenticado. Usa la opción 'Autenticar cuenta' primero."
        )
        return

    # Obtener configuración
    logger.info("Obteniendo configuración de usuario Spotify...")
    spotify_user = database.get_user_spotify(user['id'])
    logger.info(f"Spotify user config: {spotify_user}")

    if not spotify_user:
        await query.edit_message_text("❌ No tienes usuario de Spotify configurado.")
        return

    artists_limit = spotify_user.get('artists_limit', 20)
    logger.info(f"Límite de artistas: {artists_limit}")

    # Mensaje de estado
    await query.edit_message_text(
        f"🔍 Obteniendo tus artistas seguidos en Spotify...\n"
        f"Límite: {artists_limit} artistas\n"
        f"Esto puede tardar un momento..."
    )

    try:
        logger.info("Llamando a get_user_followed_artists_real...")
        # Obtener artistas reales
        artists, status_message = spotify_service.get_user_followed_artists_real(user['id'], artists_limit)

        logger.info(f"Resultado: {len(artists)} artistas obtenidos")
        logger.info(f"Status message: {status_message}")
        logger.info(f"Primeros 3 artistas: {[a.get('name', 'sin nombre') for a in artists[:3]]}")

        if not artists:
            logger.warning("No se encontraron artistas")
            await query.edit_message_text(
                f"📭 No se encontraron artistas seguidos.\n"
                f"Estado: {status_message}\n\n"
                f"💡 Consejos:\n"
                f"• Asegúrate de seguir artistas en Spotify\n"
                f"• Verifica que diste permisos de 'user-follow-read'\n"
                f"• Intenta revocar y autenticar de nuevo"
            )
            return

        logger.info("Guardando artistas pendientes...")
        # Guardar artistas pendientes
        save_result = database.save_pending_spotify_artists(user['id'], artists)
        logger.info(f"Artistas guardados: {save_result}")

        logger.info("Mostrando página de artistas...")
        # Mostrar primera página - IMPORTAR LA FUNCIÓN AQUÍ
        from handlers_helpers import show_spotify_artists_page
        await show_spotify_artists_page(query, user, artists, page=0, is_real=True, services=services)
        logger.info("Página mostrada correctamente")

    except Exception as e:
        logger.error(f"Error obteniendo artistas reales: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")

        await query.edit_message_text(
            f"❌ Error obteniendo tus artistas seguidos.\n"
            f"Error: {str(e)}\n\n"
            f"💡 Posibles soluciones:\n"
            f"• Revocar acceso y autenticar de nuevo\n"
            f"• Verificar que sigues artistas en Spotify\n"
            f"• Contactar al administrador si persiste"
        )



async def handle_spotify_show_artists(query, user: Dict, services, database):
    """Maneja mostrar artistas seguidos de Spotify - VERSIÓN CORREGIDA"""
    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    # Obtener usuario de Spotify
    spotify_user = database.get_user_spotify(user['id'])
    if not spotify_user:
        await query.edit_message_text("❌ No tienes usuario de Spotify configurado.")
        return

    username = spotify_user['spotify_username']
    artists_limit = spotify_user.get('artists_limit', 20)

    # Mensaje de estado
    await query.edit_message_text(
        f"🔍 Obteniendo artistas populares para {username}...\n"
        f"(Simulación con {artists_limit} artistas)\n"
        f"Esto puede tardar un momento."
    )

    try:
        # CORRECCIÓN: Usar la función correcta
        artists, status_message = spotify_service.search_and_get_followed_artists_simulation(username, artists_limit)

        if not artists:
            await query.edit_message_text(
                f"📭 No se encontraron artistas para {username}.\n"
                f"Estado: {status_message}"
            )
            return

        # Guardar artistas pendientes
        database.save_pending_spotify_artists(user['id'], artists)

        # Mostrar primera página
        await show_spotify_artists_page(query, user, artists, page=0, services=services)

    except Exception as e:
        logger.error(f"Error obteniendo artistas de Spotify: {e}")
        await query.edit_message_text(
            f"❌ Error obteniendo artistas de {username}.\n"
            f"Inténtalo de nuevo más tarde."
        )



async def handle_spotify_add_artists(query, user: Dict, database):
    """Añade los artistas de Spotify a la base de datos para seguimiento de conciertos"""
    # Obtener artistas pendientes
    artists = database.get_pending_spotify_artists(user['id'])
    if not artists:
        await query.edit_message_text("❌ No hay artistas para añadir.")
        return

    # Mensaje de estado
    await query.edit_message_text(
        f"⏳ Añadiendo {len(artists)} artistas de Spotify...\n"
        f"Esto puede tardar un momento."
    )

    try:
        added_count = 0
        skipped_count = 0
        error_count = 0

        total_artists = len(artists)
        processed = 0

        for artist_data in artists:
            artist_name = artist_data.get('name', '')
            spotify_id = artist_data.get('id', '')

            processed += 1

            # Actualizar mensaje de progreso cada 5 artistas
            if processed % 5 == 0 or processed == total_artists:
                progress_msg = (
                    f"⏳ Añadiendo {total_artists} artistas de Spotify...\n"
                    f"Progreso: {processed}/{total_artists}\n"
                    f"✅ Añadidos: {added_count} | ⏭️ Ya seguidos: {skipped_count} | ❌ Errores: {error_count}"
                )
                try:
                    await query.edit_message_text(progress_msg)
                except:
                    pass  # Ignorar errores de edición

            if not artist_name:
                error_count += 1
                continue

            try:
                # Buscar candidatos en MusicBrainz
                candidates = database.search_artist_candidates(artist_name)

                if not candidates:
                    skipped_count += 1
                    continue

                # Usar el mejor candidato
                best_candidate = candidates[0]
                artist_id = database.create_artist_from_candidate(best_candidate)

                if not artist_id:
                    error_count += 1
                    continue

                # Añadir a seguimiento
                was_new = database.add_followed_artist(user['id'], artist_id)

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

        # Limpiar artistas pendientes
        database.clear_pending_spotify_artists(user['id'])

        # Mensaje de resultado
        message = (
            f"✅ *Sincronización de Spotify completada*\n\n"
            f"➕ Artistas añadidos: {added_count}\n"
            f"⏭️ Ya seguidos: {skipped_count}\n"
        )

        if error_count > 0:
            message += f"❌ Errores: {error_count}\n"

        # Calcular porcentaje de éxito
        success_rate = ((added_count + skipped_count) / total_artists) * 100 if total_artists > 0 else 0
        message += f"📈 Tasa de éxito: {success_rate:.1f}%\n"

        message += f"\nUsa `/list` para ver todos tus artistas seguidos."

        keyboard = [[{"text": "🔙 Volver a Spotify", "callback_data": f"spotify_menu_{user['id']}"}]]

        return message, keyboard

    except Exception as e:
        logger.error(f"Error en añadir artistas de Spotify: {e}")
        return "❌ Error durante la sincronización. Inténtalo de nuevo.", []

async def handle_spotify_change_limit(query, user: Dict, context):
    """Maneja el cambio de límite de artistas"""
    message = (
        "🔢 *Cambiar cantidad de artistas*\n\n"
        "Envía el número de artistas que quieres mostrar.\n"
        "Rango permitido: 5-10000 artistas\n\n"
        "Ejemplo: 30"
    )

    keyboard = [[{"text": "❌ Cancelar", "callback_data": f"spotify_cancel_{user['id']}"}]]

    # Marcar que estamos esperando el límite
    context.user_data['waiting_for_spotify_limit'] = user['id']

    return message, keyboard

async def handle_spotify_change_user(query, user: Dict, context):
    """Maneja el cambio de usuario de Spotify"""
    message = (
        "👤 *Cambiar usuario de Spotify*\n\n"
        "Envía tu nuevo nombre de usuario de Spotify:"
    )

    keyboard = [[{"text": "❌ Cancelar", "callback_data": f"spotify_cancel_{user['id']}"}]]

    # Marcar que estamos esperando el nuevo usuario
    context.user_data['waiting_for_spotify_change_user'] = user['id']

    return message, keyboard

# ===========================
# FUNCIONES DE PAGINACIÓN
# ===========================

async def show_artists_page(update, user_id: int, followed_artists: List[Dict], display_name: str,
                           page: int = 0, edit_message: bool = True, database = None):
    """Muestra una página específica de artistas con navegación"""
    artists_per_page = 15
    total_pages = (len(followed_artists) + artists_per_page - 1) // artists_per_page

    # Validar página
    if page >= total_pages:
        page = total_pages - 1
    elif page < 0:
        page = 0

    start_idx = page * artists_per_page
    end_idx = min(start_idx + artists_per_page, len(followed_artists))
    page_artists = followed_artists[start_idx:end_idx]

    # Construir mensaje
    message_lines = [
        f"🎵 *Artistas seguidos por {display_name}*",
        f"📄 Página {page + 1} de {total_pages} | Total: {len(followed_artists)} artistas\n"
    ]

    for i, artist in enumerate(page_artists, start_idx + 1):
        # Nombre del artista
        artist_name = artist['name']

        # Crear línea con enlace si está disponible
        if artist['musicbrainz_url']:
            line = f"{i}. [{artist_name}]({artist['musicbrainz_url']})"
        else:
            line = f"{i}. *{artist_name}*"

        # Añadir información adicional si está disponible
        details = []
        if artist['country']:
            details.append(f"🌍 {artist['country']}")
        if artist['formed_year']:
            details.append(f"📅 {artist['formed_year']}")
        if artist['total_works'] and artist['total_works'] > 0:
            details.append(f"📝 {artist['total_works']} obras")
        if artist['artist_type']:
            details.append(f"🎭 {artist['artist_type'].title()}")

        if details:
            line += f" ({', '.join(details)})"

        message_lines.append(line)

    response = "\n".join(message_lines)

    # Crear botones de navegación
    keyboard = []
    nav_buttons = []

    # Botón anterior
    if page > 0:
        nav_buttons.append({
            "text": "⬅️ Anterior",
            "callback_data": f"list_page_{page-1}_{user_id}"
        })

    # Botón de página actual
    nav_buttons.append({
        "text": f"📄 {page + 1}/{total_pages}",
        "callback_data": "current_list_page"
    })

    # Botón siguiente
    if page < total_pages - 1:
        nav_buttons.append({
            "text": "Siguiente ➡️",
            "callback_data": f"list_page_{page+1}_{user_id}"
        })

    if nav_buttons:
        keyboard.append(nav_buttons)

    return response, keyboard

async def show_artists_without_pagination(update, followed_artists: List[Dict], display_name: str):
    """Muestra artistas sin paginación (comportamiento original para listas pequeñas)"""
    # Formatear la lista usando Markdown normal
    message_lines = [f"🎵 *Artistas seguidos por {display_name}:*\n"]

    for i, artist in enumerate(followed_artists, 1):
        # Nombre del artista
        artist_name = artist['name']

        # Crear línea con enlace si está disponible
        if artist['musicbrainz_url']:
            line = f"{i}. [{artist_name}]({artist['musicbrainz_url']})"
        else:
            line = f"{i}. *{artist_name}*"

        # Añadir información adicional si está disponible
        details = []
        if artist['country']:
            details.append(f"🌍 {artist['country']}")
        if artist['formed_year']:
            details.append(f"📅 {artist['formed_year']}")
        if artist['total_works'] and artist['total_works'] > 0:
            details.append(f"📝 {artist['total_works']} obras")
        if artist['artist_type']:
            details.append(f"🎭 {artist['artist_type'].title()}")

        if details:
            line += f" ({', '.join(details)})"

        message_lines.append(line)

    message_lines.append(f"\n📊 Total: {len(followed_artists)} artistas")

    # Unir mensaje
    response = "\n".join(message_lines)
    return response


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



# CORRECCIÓN CON DEBUG para handle_spotify_real_artists en handlers_helpers.py

async def handle_spotify_real_artists(query, user: Dict, services, database):
    """Maneja mostrar artistas realmente seguidos (con OAuth) - VERSIÓN CON DEBUG"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== INICIANDO handle_spotify_real_artists para usuario {user['id']} ===")

    spotify_service = services.get('spotify_service')
    logger.info(f"Spotify service disponible: {spotify_service is not None}")

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    logger.info(f"Verificando autenticación para usuario {user['id']}")
    is_authenticated = spotify_service.is_user_authenticated(user['id'])
    logger.info(f"Usuario autenticado: {is_authenticated}")

    if not is_authenticated:
        await query.edit_message_text(
            "❌ No estás autenticado. Usa la opción 'Autenticar cuenta' primero."
        )
        return

    # Obtener configuración
    logger.info("Obteniendo configuración de usuario Spotify...")
    spotify_user = database.get_user_spotify(user['id'])
    logger.info(f"Spotify user config: {spotify_user}")

    if not spotify_user:
        await query.edit_message_text("❌ No tienes usuario de Spotify configurado.")
        return

    artists_limit = spotify_user.get('artists_limit', 20)
    logger.info(f"Límite de artistas: {artists_limit}")

    # Mensaje de estado
    await query.edit_message_text(
        f"🔍 Obteniendo tus artistas seguidos en Spotify...\n"
        f"Límite: {artists_limit} artistas\n"
        f"Esto puede tardar un momento..."
    )

    try:
        logger.info("Llamando a get_user_followed_artists_real...")
        # Obtener artistas reales
        artists, status_message = spotify_service.get_user_followed_artists_real(user['id'], artists_limit)

        logger.info(f"Resultado: {len(artists)} artistas obtenidos")
        logger.info(f"Status message: {status_message}")
        logger.info(f"Primeros 3 artistas: {[a.get('name', 'sin nombre') for a in artists[:3]]}")

        if not artists:
            logger.warning("No se encontraron artistas")
            await query.edit_message_text(
                f"📭 No se encontraron artistas seguidos.\n"
                f"Estado: {status_message}\n\n"
                f"💡 Consejos:\n"
                f"• Asegúrate de seguir artistas en Spotify\n"
                f"• Verifica que diste permisos de 'user-follow-read'\n"
                f"• Intenta revocar y autenticar de nuevo"
            )
            return

        logger.info("Guardando artistas pendientes...")
        # Guardar artistas pendientes
        save_result = database.save_pending_spotify_artists(user['id'], artists)
        logger.info(f"Artistas guardados: {save_result}")

        logger.info("Mostrando página de artistas...")
        # Mostrar primera página - IMPORTAR LA FUNCIÓN AQUÍ
        from handlers_helpers import show_spotify_artists_page
        await show_spotify_artists_page(query, user, artists, page=0, is_real=True, services=services)
        logger.info("Página mostrada correctamente")

    except Exception as e:
        logger.error(f"Error obteniendo artistas reales: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")

        await query.edit_message_text(
            f"❌ Error obteniendo tus artistas seguidos.\n"
            f"Error: {str(e)}\n\n"
            f"💡 Posibles soluciones:\n"
            f"• Revocar acceso y autenticar de nuevo\n"
            f"• Verificar que sigues artistas en Spotify\n"
            f"• Contactar al administrador si persiste"
        )


# CORRECCIÓN para get_user_followed_artists_real en spotify.py
# AÑADIR/REEMPLAZAR esta función en spotify.py:

def get_user_followed_artists_real(self, user_id: int, limit: int = 50) -> tuple[list, str]:
    """
    Obtiene los artistas realmente seguidos por el usuario autenticado
    VERSIÓN CON DEBUG MEJORADO

    Args:
        user_id: ID del usuario
        limit: Límite de artistas a obtener

    Returns:
        Tupla (lista_artistas, mensaje_estado)
    """
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== get_user_followed_artists_real: usuario {user_id}, límite {limit} ===")

    try:
        logger.info("Obteniendo cliente autenticado...")
        sp = self.get_authenticated_client(user_id)

        if not sp:
            logger.error("No se pudo obtener cliente autenticado")
            return [], "Usuario no autenticado. Usa el comando de autenticación."

        logger.info("Cliente autenticado obtenido correctamente")

        artists = []
        after = None
        total_fetched = 0

        logger.info(f"Iniciando bucle de obtención de artistas (límite: {limit})")

        # Spotify limita a 50 por request para followed artists
        iteration = 0
        while total_fetched < limit:
            iteration += 1
            batch_limit = min(50, limit - total_fetched)

            logger.info(f"Iteración {iteration}: obteniendo {batch_limit} artistas (after={after})")

            try:
                response = sp.current_user_followed_artists(
                    limit=batch_limit,
                    after=after
                )

                logger.info(f"Respuesta de Spotify recibida: {type(response)}")

                if not response:
                    logger.warning("Respuesta vacía de Spotify")
                    break

                artists_data = response.get('artists', {})
                logger.info(f"Datos de artistas: {type(artists_data)}, keys: {list(artists_data.keys()) if artists_data else 'None'}")

                artist_items = artists_data.get('items', [])
                logger.info(f"Items de artistas: {len(artist_items)}")

                if not artist_items:
                    logger.info("No hay más artistas en esta respuesta")
                    break

                for i, artist in enumerate(artist_items):
                    logger.info(f"Procesando artista {i+1}: {artist.get('name', 'sin nombre')}")

                    artist_info = {
                        'id': artist.get('id'),
                        'name': artist.get('name'),
                        'followers': artist.get('followers', {}).get('total', 0),
                        'popularity': artist.get('popularity', 0),
                        'genres': artist.get('genres', []),
                        'external_urls': artist.get('external_urls', {}),
                        'images': artist.get('images', [])
                    }

                    artists.append(artist_info)
                    total_fetched += 1

                    if total_fetched >= limit:
                        logger.info(f"Límite alcanzado: {total_fetched}")
                        break

                # Preparar para siguiente batch
                cursors = artists_data.get('cursors', {})
                after = cursors.get('after')

                logger.info(f"Cursor 'after' para siguiente batch: {after}")

                if not after:
                    logger.info("No hay más páginas disponibles")
                    break

                # Evitar bucle infinito
                if iteration > 10:
                    logger.warning("Máximo de iteraciones alcanzado")
                    break

            except Exception as e:
                logger.error(f"Error en batch {iteration}: {e}")
                import traceback
                logger.error(f"Traceback del batch: {traceback.format_exc()}")
                break

        logger.info(f"Obtención completada: {len(artists)} artistas obtenidos")

        # Log de algunos artistas para verificar
        for i, artist in enumerate(artists[:3]):
            logger.info(f"Artista {i+1}: {artist.get('name')} (followers: {artist.get('followers', 0)})")

        return artists, f"Se obtuvieron {len(artists)} artistas seguidos"

    except Exception as e:
        logger.error(f"Error crítico en get_user_followed_artists_real: {e}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
        return [], f"Error: {str(e)}"


# CORRECCIÓN para show_spotify_artists_page en handlers_helpers.py
# AÑADIR/REEMPLAZAR esta función:

async def show_spotify_artists_page(query, user: Dict, artists: List[Dict], page: int = 0,
                                   is_real: bool = False, services: Dict = None):
    """Muestra una página de artistas de Spotify con paginación - VERSIÓN CON DEBUG"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== show_spotify_artists_page: {len(artists)} artistas, página {page}, real={is_real} ===")

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

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

    # Título según el tipo
    title = "🎵 *Tus artistas seguidos en Spotify*" if is_real else "🎵 *Artistas populares (simulación)*"

    # Obtener username de la base de datos
    try:
        from database import ArtistTrackerDatabase
        temp_db = ArtistTrackerDatabase()
        spotify_user = temp_db.get_user_spotify(user['id'])
        username = spotify_user['spotify_username'] if spotify_user else user.get('spotify_username', 'Usuario')
    except Exception as e:
        logger.error(f"Error obteniendo username: {e}")
        username = user.get('spotify_username', 'Usuario')

    # Construir texto
    message_lines = [
        f"{title}",
        f"👤 Usuario: {username}",
        f"🔢 Total: {len(artists)} artistas",
        f"📄 Página {page + 1} de {total_pages}\n"
    ]

    if not is_real:
        message_lines.append("⚠️ *Estos son artistas populares, no tus seguidos reales.*")
        message_lines.append("🔐 *Usa autenticación completa para ver tus artistas reales.*\n")

    for i, artist in enumerate(page_artists, start_idx + 1):
        name = artist.get("name", "Nombre desconocido")
        followers = artist.get("followers", 0)
        popularity = artist.get("popularity", 0)

        # Escapar caracteres especiales para Markdown
        safe_name = name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        line = f"{i}. *{safe_name}*"

        # Añadir información
        if followers > 0:
            line += f" ({followers:,} seguidores)"

        if popularity > 0:
            line += f" - {popularity}% popularidad"

        # Añadir géneros si están disponibles
        genres = artist.get("genres", [])
        if genres:
            genre_text = ", ".join(genres[:2])
            line += f" _{genre_text}_"

        message_lines.append(line)

    # Crear botones
    keyboard = []
    nav_buttons = []

    # Botón anterior
    if page > 0:
        callback_prefix = "spotify_real_page" if is_real else "spotify_page"
        nav_buttons.append(InlineKeyboardButton(
            "⬅️ Anterior",
            callback_data=f"{callback_prefix}_{page-1}_{user['id']}"
        ))

    # Botón de página actual
    nav_buttons.append(InlineKeyboardButton(
        f"📄 {page + 1}/{total_pages}",
        callback_data="current_spotify_page"
    ))

    # Botón siguiente
    if page < total_pages - 1:
        callback_prefix = "spotify_real_page" if is_real else "spotify_page"
        nav_buttons.append(InlineKeyboardButton(
            "Siguiente ➡️",
            callback_data=f"{callback_prefix}_{page+1}_{user['id']}"
        ))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Botones de acción
    action_buttons = []
    action_buttons.append(InlineKeyboardButton(
        "➕ Añadir todos",
        callback_data=f"spotify_add_{user['id']}"
    ))

    if is_real:
        action_buttons.append(InlineKeyboardButton(
            "🔗 Seguir en Spotify",
            callback_data=f"spotify_follow_{user['id']}"
        ))
    else:
        action_buttons.append(InlineKeyboardButton(
            "🔐 Autenticar para más",
            callback_data=f"spotify_auth_{user['id']}"
        ))

    keyboard.append(action_buttons)

    # Botón para volver
    keyboard.append([InlineKeyboardButton(
        "🔙 Volver",
        callback_data=f"spotify_menu_{user['id']}"
    )])

    message = "\n".join(message_lines)

    logger.info(f"Enviando mensaje con {len(keyboard)} filas de botones")

    # Actualizar mensaje
    try:
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info("Mensaje enviado correctamente")
    except Exception as e:
        logger.error(f"Error enviando mensaje: {e}")
        await query.edit_message_text("❌ Error mostrando artistas. Inténtalo de nuevo.")



# ===========================
# UTILIDADES
# ===========================
def escape_markdown_v2(text):
    """Escapa caracteres especiales para MarkdownV2"""
    # Caracteres que necesitan escape en MarkdownV2
    escape_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']

    if not text:
        return ""

    for char in escape_chars:
        text = text.replace(char, f'\\{char}')

    return text

def extract_auth_code_from_input(user_input: str) -> str:
    """
    Extrae el código de autorización de diferentes formatos de entrada

    Args:
        user_input: Entrada del usuario (URL, código, o texto)

    Returns:
        Código de autorización extraído o cadena vacía
    """
    if not user_input:
        return ""

    user_input = user_input.strip()

    # Método 1: URL completa con parámetros
    if 'code=' in user_input:
        try:
            # Buscar patrón code=XXXXXXX
            code_match = re.search(r'code=([^&\s]+)', user_input)
            if code_match:
                code = code_match.group(1)
                # Decodificar URL si es necesario
                code = unquote(code)
                logger.info(f"Código extraído por regex: {code[:10]}...")
                return code

            # Método alternativo: parsear como URL
            if user_input.startswith('http'):
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(user_input)
                params = parse_qs(parsed.query)
                if 'code' in params:
                    code = params['code'][0]
                    logger.info(f"Código extraído por URL parse: {code[:10]}...")
                    return code
        except Exception as e:
            logger.error(f"Error parseando URL: {e}")

    # Método 2: Buscar en texto libre (para casos como "Authorization successful: ABC123")
    auth_patterns = [
        r'authorization\s+successful[:\s]+([a-zA-Z0-9_-]+)',
        r'code[:\s]+([a-zA-Z0-9_-]+)',
        r'token[:\s]+([a-zA-Z0-9_-]+)',
    ]

    for pattern in auth_patterns:
        match = re.search(pattern, user_input, re.IGNORECASE)
        if match:
            code = match.group(1)
            if len(code) > 10:  # Los códigos suelen ser largos
                logger.info(f"Código extraído por patrón: {code[:10]}...")
                return code

    # Método 3: Si parece ser solo el código (string largo sin espacios)
    if (len(user_input) > 20 and
        not ' ' in user_input and
        not user_input.startswith('http') and
        re.match(r'^[a-zA-Z0-9_-]', user_input)):
        logger.info(f"Asumiendo que es código directo: {user_input[:10]}...")
        return user_input

    # Método 4: Buscar cualquier string alfanumérico largo
    long_strings = re.findall(r'[a-zA-Z0-9_-]{20,}', user_input)
    if long_strings:
        code = long_strings[0]
        logger.info(f"Código extraído como string largo: {code[:10]}...")
        return code

    logger.warning(f"No se pudo extraer código de: {user_input[:50]}...")
    return ""


async def handle_spotify_playlists(query, user: Dict, services, database):
    """Maneja mostrar playlists del usuario"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== handle_spotify_playlists para usuario {user['id']} ===")

    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    if not spotify_service.is_user_authenticated(user['id']):
        await query.edit_message_text(
            "❌ No estás autenticado. Usa la opción 'Autenticar cuenta' primero."
        )
        return

    # Obtener configuración
    spotify_user = database.get_user_spotify(user['id'])
    if not spotify_user:
        await query.edit_message_text("❌ No tienes usuario de Spotify configurado.")
        return

    # Mensaje de estado
    await query.edit_message_text(
        f"🎵 Obteniendo tus playlists de Spotify...\n"
        f"Esto puede tardar un momento..."
    )

    try:
        # Obtener playlists
        playlists, status_message = spotify_service.get_user_playlists_detailed(user['id'], 100)

        logger.info(f"Playlists obtenidas: {len(playlists)}")

        if not playlists:
            await query.edit_message_text(
                f"📭 No se encontraron playlists.\n"
                f"Estado: {status_message}\n\n"
                f"💡 Crea algunas playlists en Spotify y vuelve a intentar."
            )
            return

        # Guardar playlists pendientes
        database.save_pending_playlists(user['id'], playlists)

        # Mostrar primera página
        await show_spotify_playlists_page(query, user, playlists, page=0, services=services)

    except Exception as e:
        logger.error(f"Error obteniendo playlists: {e}")
        await query.edit_message_text(
            f"❌ Error obteniendo playlists.\n"
            f"Error: {str(e)}"
        )

async def show_spotify_playlists_page(query, user: Dict, playlists: List[Dict], page: int = 0, services: Dict = None):
    """Muestra una página de playlists de Spotify con paginación"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== show_spotify_playlists_page: {len(playlists)} playlists, página {page} ===")

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    playlists_per_page = 10
    total_pages = (len(playlists) + playlists_per_page - 1) // playlists_per_page

    if page >= total_pages:
        page = total_pages - 1
    elif page < 0:
        page = 0

    start_idx = page * playlists_per_page
    end_idx = min(start_idx + playlists_per_page, len(playlists))
    page_playlists = playlists[start_idx:end_idx]

    # Obtener username
    try:
        from database import ArtistTrackerDatabase
        temp_db = ArtistTrackerDatabase()
        spotify_user = temp_db.get_user_spotify(user['id'])
        username = spotify_user['spotify_username'] if spotify_user else 'Usuario'
    except:
        username = 'Usuario'

    # Construir texto
    message_lines = [
        f"🎵 *Playlists de {username}*",
        f"📄 Página {page + 1} de {total_pages} | Total: {len(playlists)} playlists\n"
    ]

    for i, playlist in enumerate(page_playlists, start_idx + 1):
        name = playlist.get("name", "Playlist sin nombre")
        tracks_total = playlist.get("tracks_total", 0)
        owner = playlist.get("owner", "")

        # Escapar caracteres especiales
        safe_name = name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        line = f"{i}. *{safe_name}*"

        if tracks_total > 0:
            line += f" ({tracks_total} canciones)"

        if owner and owner != username:
            line += f" - by {owner}"

        message_lines.append(line)

    # Crear botones de navegación
    keyboard = []
    nav_buttons = []

    # Botón anterior
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            "⬅️ Anterior",
            callback_data=f"spotify_playlists_page_{page-1}_{user['id']}"
        ))

    # Botón de página actual
    nav_buttons.append(InlineKeyboardButton(
        f"📄 {page + 1}/{total_pages}",
        callback_data="current_spotify_playlists_page"
    ))

    # Botón siguiente
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            "Siguiente ➡️",
            callback_data=f"spotify_playlists_page_{page+1}_{user['id']}"
        ))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Botones de playlists individuales
    for i, playlist in enumerate(page_playlists):
        playlist_id = playlist.get('id', '')
        playlist_name = playlist.get('name', f'Playlist {start_idx + i + 1}')

        # Truncar nombre si es muy largo
        button_text = playlist_name[:30] + "..." if len(playlist_name) > 30 else playlist_name

        keyboard.append([InlineKeyboardButton(
            f"🎵 {button_text}",
            callback_data=f"spotify_playlist_view_{playlist_id}_{user['id']}"
        )])

    # Botón para volver
    keyboard.append([InlineKeyboardButton(
        "🔙 Volver al menú",
        callback_data=f"spotify_menu_{user['id']}"
    )])

    message = "\n".join(message_lines)

    # Actualizar mensaje
    try:
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        logger.info("Página de playlists mostrada correctamente")
    except Exception as e:
        logger.error(f"Error mostrando playlists: {e}")
        await query.edit_message_text("❌ Error mostrando playlists.")

async def handle_spotify_playlist_view(query, user: Dict, playlist_id: str, services, database):
    """Maneja la visualización de una playlist específica"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== handle_spotify_playlist_view: playlist {playlist_id} ===")

    spotify_service = services.get('spotify_service')

    if not spotify_service:
        await query.edit_message_text("❌ Servicio de Spotify no disponible.")
        return

    # Mensaje de estado
    await query.edit_message_text(
        f"🔍 Obteniendo artistas de la playlist...\n"
        f"Esto puede tardar un momento..."
    )

    try:
        # Obtener información de la playlist
        playlists = database.get_pending_playlists(user['id'])
        playlist_info = None

        if playlists:
            playlist_info = next((p for p in playlists if p.get('id') == playlist_id), None)

        if not playlist_info:
            await query.edit_message_text("❌ No se encontró información de la playlist.")
            return

        # Obtener artistas de la playlist
        artists, status_message = spotify_service.get_playlist_tracks(user['id'], playlist_id)

        logger.info(f"Artistas obtenidos: {len(artists)}")

        if not artists:
            await query.edit_message_text(
                f"📭 No se encontraron artistas en esta playlist.\n"
                f"Estado: {status_message}"
            )
            return

        # Guardar artistas de la playlist
        database.save_pending_playlist_artists(
            user['id'],
            playlist_id,
            playlist_info.get('name', 'Playlist'),
            artists
        )

        # Mostrar primera página de artistas
        await show_spotify_playlist_artists_page(
            query, user, playlist_id, playlist_info, artists, page=0, services=services
        )

    except Exception as e:
        logger.error(f"Error obteniendo artistas de playlist: {e}")
        await query.edit_message_text(
            f"❌ Error obteniendo artistas de la playlist.\n"
            f"Error: {str(e)}"
        )

async def show_spotify_playlist_artists_page(query, user: Dict, playlist_id: str, playlist_info: Dict,
                                           artists: List[Dict], page: int = 0, services: Dict = None):
    """Muestra una página de artistas de una playlist específica"""
    import logging
    logger = logging.getLogger(__name__)

    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    artists_per_page = 15
    total_pages = (len(artists) + artists_per_page - 1) // artists_per_page

    if page >= total_pages:
        page = total_pages - 1
    elif page < 0:
        page = 0

    start_idx = page * artists_per_page
    end_idx = min(start_idx + artists_per_page, len(artists))
    page_artists = artists[start_idx:end_idx]

    playlist_name = playlist_info.get('name', 'Playlist')
    playlist_tracks_total = playlist_info.get('tracks_total', 0)

    # Construir texto
    message_lines = [
        f"🎵 *Artistas en: {playlist_name}*",
        f"📊 Total canciones: {playlist_tracks_total}",
        f"🎤 Artistas únicos: {len(artists)}",
        f"📄 Página {page + 1} de {total_pages}\n"
    ]

    for i, artist in enumerate(page_artists, start_idx + 1):
        name = artist.get("name", "Artista desconocido")
        tracks = artist.get("tracks", [])

        # Escapar caracteres especiales
        safe_name = name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

        line = f"{i}. *{safe_name}*"

        if tracks:
            line += f" ({len(tracks)} canción{'es' if len(tracks) > 1 else ''})"

        message_lines.append(line)

    # Crear botones de navegación
    keyboard = []
    nav_buttons = []

    # Botón anterior
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(
            "⬅️ Anterior",
            callback_data=f"spotify_playlist_artists_page_{playlist_id}_{page-1}_{user['id']}"
        ))

    # Botón de página actual
    nav_buttons.append(InlineKeyboardButton(
        f"📄 {page + 1}/{total_pages}",
        callback_data="current_playlist_artists_page"
    ))

    # Botón siguiente
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(
            "Siguiente ➡️",
            callback_data=f"spotify_playlist_artists_page_{playlist_id}_{page+1}_{user['id']}"
        ))

    if nav_buttons:
        keyboard.append(nav_buttons)

    # Botón para seguir todos los artistas
    keyboard.append([InlineKeyboardButton(
        f"➕ Seguir todos ({len(artists)} artistas)",
        callback_data=f"spotify_playlist_follow_all_{playlist_id}_{user['id']}"
    )])

    # Botones de navegación
    keyboard.append([
        InlineKeyboardButton("🔙 Volver a playlists", callback_data=f"spotify_playlists_{user['id']}"),
        InlineKeyboardButton("🏠 Menú principal", callback_data=f"spotify_menu_{user['id']}")
    ])

    message = "\n".join(message_lines)

    # Actualizar mensaje
    try:
        await query.edit_message_text(
            message,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except Exception as e:
        logger.error(f"Error mostrando artistas de playlist: {e}")
        await query.edit_message_text("❌ Error mostrando artistas.")

async def handle_spotify_playlist_follow_all(query, user: Dict, playlist_id: str, database):
    """Maneja seguir todos los artistas de una playlist"""
    import logging
    logger = logging.getLogger(__name__)

    logger.info(f"=== handle_spotify_playlist_follow_all: playlist {playlist_id} ===")

    # Obtener artistas de la playlist
    playlist_data = database.get_pending_playlist_artists(user['id'], playlist_id)

    if not playlist_data:
        await query.edit_message_text("❌ No se encontraron datos de la playlist.")
        return

    artists = playlist_data.get('artists', [])
    playlist_name = playlist_data.get('playlist_name', 'Playlist')

    if not artists:
        await query.edit_message_text("❌ No hay artistas en esta playlist.")
        return

    # Mensaje de estado
    await query.edit_message_text(
        f"⏳ Añadiendo {len(artists)} artistas de '{playlist_name}'...\n"
        f"Esto puede tardar un momento."
    )

    try:
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
                    await query.edit_message_text(progress_msg)
                except:
                    pass

            if not artist_name:
                error_count += 1
                continue

            try:
                # Buscar candidatos en MusicBrainz
                candidates = database.search_artist_candidates(artist_name)

                if not candidates:
                    skipped_count += 1
                    continue

                # Usar el mejor candidato
                best_candidate = candidates[0]
                artist_id = database.create_artist_from_candidate(best_candidate)

                if not artist_id:
                    error_count += 1
                    continue

                # Añadir a seguimiento
                was_new = database.add_followed_artist(user['id'], artist_id)

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

        # Mensaje de resultado
        message = (
            f"✅ *Sincronización de playlist completada*\n\n"
            f"🎵 Playlist: {playlist_name}\n"
            f"➕ Artistas añadidos: {added_count}\n"
            f"⏭️ Ya seguidos: {skipped_count}\n"
        )

        if error_count > 0:
            message += f"❌ Errores: {error_count}\n"

        # Calcular porcentaje de éxito
        success_rate = ((added_count + skipped_count) / len(artists)) * 100 if artists else 0
        message += f"📈 Tasa de éxito: {success_rate:.1f}%\n"

        message += f"\nUsa `/list` para ver todos tus artistas seguidos."

        keyboard = [
            [InlineKeyboardButton("🔙 Volver a playlists", callback_data=f"spotify_playlists_{user['id']}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(message, parse_mode='Markdown', reply_markup=reply_markup)

    except Exception as e:
        logger.error(f"Error en seguir artistas de playlist: {e}")
        await query.edit_message_text(
            f"❌ Error durante la sincronización.\n"
            f"Error: {str(e)}"
        )
