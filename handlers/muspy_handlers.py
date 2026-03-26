#!/usr/bin/env python3
"""
Handlers específicos para funcionalidades de Muspy
"""

import logging
import asyncio
import json
from datetime import datetime, date
from typing import Dict, List, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler

logger = logging.getLogger(__name__)

# Estados para ConversationHandler
MUSPY_EMAIL, MUSPY_PASSWORD, MUSPY_USERID = range(3)

class MuspyHandlers:
    """Clase que contiene todos los handlers de Muspy"""

    def __init__(self, database, muspy_service):
        self.db = database
        self.muspy_service = muspy_service

        # Almacenamiento temporal por usuario
        self.user_artists_cache = {}
        self.user_releases_cache = {}  # Añadir caché para releases

    async def muspy_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Comando /muspy - Panel principal de configuración"""
        user_id = self._get_or_create_user_id(update)
        if not user_id:
            await update.message.reply_text(
                "❌ Primero debes registrarte con `/adduser <tu_nombre>`",
                parse_mode='Markdown'
            )
            return

        # Verificar si tiene credenciales configuradas
        credentials = self.db.get_muspy_credentials(user_id)

        text = "🎵 *Panel de Muspy*\n\n"

        if credentials:
            text += "✅ *Cuenta configurada*\n"
            text += f"📧 Email: `{credentials[0]}`\n"
            text += f"🆔 User ID: `{credentials[2]}`\n\n"
        else:
            text += "❌ *Cuenta no configurada*\n"
            text += "Configura tu cuenta de Muspy para acceder a todas las funcionalidades.\n\n"

        text += "*Selecciona una opción:*"

        keyboard = [
            [InlineKeyboardButton("🔄 Nuevos lanzamientos", callback_data=f"muspy_releases_{user_id}")],
            [InlineKeyboardButton("👥 Artistas Muspy", callback_data=f"muspy_artists_{user_id}")],
            [InlineKeyboardButton("🎵 Artistas bot", callback_data=f"muspy_bot_artists_{user_id}")],
        ]

        # Cambiar texto del botón según el estado de login
        login_button_text = "🔑 Cambiar cuenta Muspy" if credentials else "🔑 Login Muspy"
        keyboard.append([InlineKeyboardButton(login_button_text, callback_data=f"muspy_login_{user_id}")])

        if credentials:
            keyboard.extend([
                [InlineKeyboardButton("➕ Añadir a Muspy", callback_data=f"muspy_add_artists_{user_id}")],
                [InlineKeyboardButton("⬇️ Seguir artistas de Muspy", callback_data=f"muspy_import_artists_{user_id}")],
                [InlineKeyboardButton("🗑️ Desconectar cuenta", callback_data=f"muspy_disconnect_{user_id}")]
            ])

        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

    async def muspy_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Maneja los callbacks del panel de Muspy"""
        query = update.callback_query
        await query.answer()

        callback_data = query.data
        parts = callback_data.split("_")

        if len(parts) < 3:
            await query.edit_message_text("❌ Error en el callback.")
            return

        action = parts[1]
        user_id = int(parts[-1])

        # Verificar usuario
        if not self._verify_user(update, user_id):
            await query.edit_message_text("❌ Error de autenticación.")
            return

        try:
            if action == "releases":
                if len(parts) > 2 and parts[2] == "page":
                    # Navegación de páginas de releases: muspy_releases_page_X_USERID
                    page = int(parts[3]) if len(parts) > 3 else 0
                    if user_id in self.user_releases_cache:
                        releases = self.user_releases_cache[user_id]
                        await self._show_releases_page(query, user_id, releases, page)
                    else:
                        await query.edit_message_text("❌ Lista de releases expirada. Usa `/muspy` de nuevo.")
                else:
                    # Ver releases por primera vez
                    await self._handle_releases(query, user_id)
            elif action == "artists":
                if len(parts) > 2 and parts[2] == "page":
                    # Navegación de páginas de artistas: muspy_artists_page_X_USERID
                    page = int(parts[3]) if len(parts) > 3 else 0
                    if user_id in self.user_artists_cache:
                        artists = self.user_artists_cache[user_id]
                        await self._show_artists_page(query, user_id, artists, page)
                    else:
                        await query.edit_message_text("❌ Lista de artistas expirada. Usa `/muspy` de nuevo.")
                else:
                    # Listar artistas por primera vez
                    await self._handle_artists(query, user_id)
            elif action == "bot" and len(parts) > 2 and parts[2] == "artists":
                # Mostrar artistas del bot (ejecutar /list)
                await self._handle_bot_artists(query, user_id)
            elif action == "login":
                # Esto iniciará el ConversationHandler
                await self._start_muspy_login(update, context)
                return
            elif action == "add" and len(parts) > 2 and parts[2] == "artists":
                await self._handle_add_artists(query, user_id)
            elif action == "import" and len(parts) > 2 and parts[2] == "artists":
                await self._handle_import_artists(query, user_id)
            elif action == "disconnect":
                await self._handle_disconnect(query, user_id)
            elif action == "menu":
                # Volver al menú principal
                fake_update = type('obj', (object,), {
                    'message': query.message,
                    'effective_chat': query.message.chat
                })()
                await self.muspy_command(fake_update, context)
            elif callback_data == "muspy_current_page":
                # No hacer nada si presiona el botón de página actual
                return
            else:
                await query.edit_message_text("❌ Acción no reconocida.")

        except Exception as e:
            logger.error(f"Error en callback de Muspy: {e}")
            await query.edit_message_text("❌ Error procesando la solicitud.")

    async def _start_muspy_login(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Inicia el proceso de login de Muspy"""
        if hasattr(update, 'callback_query') and update.callback_query:
            query = update.callback_query
            user_id = int(query.data.split("_")[-1])

            await query.edit_message_text(
                "🔑 *Configuración de Muspy*\n\n"
                "Para conectar tu cuenta de Muspy, necesito tus credenciales.\n\n"
                "📧 Envía tu email de Muspy:",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(
                "🔑 *Configuración de Muspy*\n\n"
                "Para conectar tu cuenta de Muspy, necesito tus credenciales.\n\n"
                "📧 Envía tu email de Muspy:",
                parse_mode='Markdown'
            )
            user_id = self._get_or_create_user_id(update)

        # Guardar user_id en context para el ConversationHandler
        context.user_data['muspy_user_id'] = user_id
        return MUSPY_EMAIL

    async def login_email_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Maneja la entrada del email"""
        email = update.message.text.strip()

        if not email or '@' not in email:
            await update.message.reply_text(
                "❌ Email inválido. Envía un email válido:"
            )
            return MUSPY_EMAIL

        context.user_data['muspy_email'] = email
        await update.message.reply_text(
            "✅ Email guardado.\n\n"
            "🔒 Ahora envía tu contraseña de Muspy:"
        )
        return MUSPY_PASSWORD

    async def login_password_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Maneja la entrada de la contraseña"""
        password = update.message.text.strip()

        if not password:
            await update.message.reply_text(
                "❌ Contraseña vacía. Envía tu contraseña:"
            )
            return MUSPY_PASSWORD

        context.user_data['muspy_password'] = password
        await update.message.reply_text(
            "✅ Contraseña guardada.\n\n"
            "🆔 Finalmente, envía tu User ID de Muspy\n"
            "(lo puedes encontrar en tu perfil de Muspy):"
        )
        return MUSPY_USERID

    async def login_userid_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Maneja la entrada del User ID y completa la configuración"""
        userid = update.message.text.strip()

        if not userid:
            await update.message.reply_text(
                "❌ User ID vacío. Envía tu User ID de Muspy:"
            )
            return MUSPY_USERID

        # Obtener datos del context
        email = context.user_data.get('muspy_email')
        password = context.user_data.get('muspy_password')
        user_id = context.user_data.get('muspy_user_id')

        if not all([email, password, user_id]):
            await update.message.reply_text(
                "❌ Error en los datos. Proceso cancelado."
            )
            return ConversationHandler.END

        # Verificar credenciales
        status_message = await update.message.reply_text(
            "🔍 Verificando credenciales con Muspy..."
        )

        try:
            success, message = self.muspy_service.verify_credentials(email, password, userid)

            if success:
                # Guardar credenciales
                if self.db.save_muspy_credentials(user_id, email, password, userid):
                    await status_message.edit_text(
                        "✅ *¡Cuenta de Muspy configurada correctamente!*\n\n"
                        f"📧 Email: {email}\n"
                        f"🆔 User ID: {userid}\n\n"
                        "Usa `/muspy` para acceder a todas las funciones.",
                        parse_mode='Markdown'
                    )
                else:
                    await status_message.edit_text(
                        "❌ Error guardando las credenciales. Inténtalo de nuevo."
                    )
            else:
                await status_message.edit_text(
                    f"❌ Error de verificación: {message}\n\n"
                    "Verifica tus credenciales e inténtalo de nuevo con `/muspy`."
                )

        except Exception as e:
            logger.error(f"Error verificando credenciales Muspy: {e}")
            await status_message.edit_text(
                "❌ Error de conexión con Muspy. Inténtalo más tarde."
            )

        # Limpiar datos sensibles
        context.user_data.clear()
        return ConversationHandler.END

    async def cancel_login(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancela el proceso de login"""
        await update.message.reply_text(
            "❌ Configuración de Muspy cancelada."
        )
        context.user_data.clear()
        return ConversationHandler.END

    async def _handle_releases(self, query, user_id: int):
        """Maneja la consulta de nuevos lanzamientos"""
        credentials = self.db.get_muspy_credentials(user_id)
        if not credentials:
            await query.edit_message_text(
                "❌ No tienes credenciales de Muspy configuradas.\n"
                "Usa el botón 'Login Muspy' para configurarlas."
            )
            return

        await query.edit_message_text("🔍 Obteniendo lanzamientos de Muspy...")

        try:
            email, password, userid = credentials
            releases, status_message = self.muspy_service.get_user_releases(email, password, userid)

            if not releases:
                await query.edit_message_text(
                    f"📭 No se encontraron lanzamientos.\n"
                    f"Estado: {status_message}"
                )
                return

            # Filtrar solo lanzamientos futuros
            today = date.today().strftime("%Y-%m-%d")
            future_releases = [r for r in releases if r.get('date', '0000-00-00') >= today]

            if not future_releases:
                await query.edit_message_text(
                    f"📭 No hay próximos lanzamientos anunciados.\n"
                    f"📊 Total de lanzamientos en la base de datos: {len(releases)}"
                )
                return

            # Guardar en caché y mostrar primera página
            self.user_releases_cache[user_id] = future_releases
            await self._show_releases_page(query, user_id, future_releases, page=0)

        except Exception as e:
            logger.error(f"Error obteniendo releases: {e}")
            await query.edit_message_text("❌ Error obteniendo lanzamientos de Muspy.")

    async def _handle_artists(self, query, user_id: int):
        """Maneja la consulta de artistas de Muspy"""
        credentials = self.db.get_muspy_credentials(user_id)
        if not credentials:
            await query.edit_message_text(
                "❌ No tienes credenciales de Muspy configuradas.\n"
                "Usa el botón 'Login Muspy' para configurarlas."
            )
            return

        await query.edit_message_text("🔍 Obteniendo artistas de Muspy...")

        try:
            email, password, userid = credentials
            artists, status_message = self.muspy_service.get_user_artists(email, password, userid)

            if not artists:
                await query.edit_message_text(
                    f"📭 No se encontraron artistas.\n"
                    f"Estado: {status_message}"
                )
                return

            # Guardar en caché y mostrar primera página
            self.user_artists_cache[user_id] = artists
            await self._show_artists_page(query, user_id, artists, page=0)

        except Exception as e:
            logger.error(f"Error obteniendo artistas: {e}")
            await query.edit_message_text("❌ Error obteniendo artistas de Muspy.")

    async def _handle_add_artists(self, query, user_id: int):
        """Maneja la adición de artistas seguidos del bot a Muspy"""
        credentials = self.db.get_muspy_credentials(user_id)
        if not credentials:
            await query.edit_message_text(
                "❌ No tienes credenciales de Muspy configuradas."
            )
            return

        # Obtener artistas seguidos que no estén en Muspy
        followed_artists = self.db.get_user_followed_artists(user_id)
        artists_with_mbid = [a for a in followed_artists if a.get('mbid')]

        if not artists_with_mbid:
            await query.edit_message_text(
                "📭 No tienes artistas seguidos con MBID válido para añadir a Muspy."
            )
            return

        await query.edit_message_text(
            f"⏳ Añadiendo {len(artists_with_mbid)} artistas a Muspy...\n"
            f"Esto puede tardar un momento."
        )

        try:
            email, password, userid = credentials

            # Función de callback para progreso
            async def progress_callback(current, total, added, errors):
                if current % 5 == 0 or current == total:
                    await query.edit_message_text(
                        f"⏳ Progreso: {current}/{total}\n"
                        f"✅ Añadidos: {added}\n"
                        f"❌ Errores: {errors}"
                    )

            # Sincronizar artistas
            added_count, error_count, errors = await self.muspy_service.sync_artists_to_muspy(
                email, password, userid, artists_with_mbid, progress_callback
            )

            # Mensaje final
            message = (
                f"🎉 *Sincronización completada*\n\n"
                f"✅ Artistas añadidos: {added_count}\n"
                f"❌ Errores: {error_count}\n"
                f"📊 Total procesados: {len(artists_with_mbid)}"
            )

            if errors and len(errors) <= 5:  # Mostrar errores si son pocos
                message += "\n\n*Errores:*\n" + "\n".join(errors[:5])
            elif errors:
                message += f"\n\n⚠️ {len(errors)} errores (ver logs para detalles)"

            await query.edit_message_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error añadiendo artistas a Muspy: {e}")
            await query.edit_message_text("❌ Error durante la sincronización.")

    async def _handle_import_artists(self, query, user_id: int):
        """Maneja la importación de artistas desde Muspy al bot"""
        credentials = self.db.get_muspy_credentials(user_id)
        if not credentials:
            await query.edit_message_text(
                "❌ No tienes credenciales de Muspy configuradas."
            )
            return

        await query.edit_message_text("🔍 Obteniendo artistas de Muspy...")

        try:
            email, password, userid = credentials
            muspy_artists, status_message = self.muspy_service.get_user_artists(email, password, userid)

            if not muspy_artists:
                await query.edit_message_text(
                    f"📭 No se encontraron artistas en Muspy.\n"
                    f"Estado: {status_message}"
                )
                return

            await query.edit_message_text(
                f"⏳ Importando {len(muspy_artists)} artistas desde Muspy...\n"
                f"Esto puede tardar un momento."
            )

            added_count = 0
            error_count = 0

            for i, muspy_artist in enumerate(muspy_artists, 1):
                # Actualizar progreso cada 10 artistas
                if i % 10 == 0 or i == len(muspy_artists):
                    await query.edit_message_text(
                        f"⏳ Progreso: {i}/{len(muspy_artists)}\n"
                        f"✅ Añadidos: {added_count}\n"
                        f"❌ Errores: {error_count}"
                    )

                try:
                    mbid = muspy_artist.get('mbid')
                    if not mbid:
                        error_count += 1
                        continue

                    # Verificar si ya existe en la BD
                    existing_artist_id = self.db.get_artist_by_mbid(mbid)

                    if existing_artist_id:
                        # Ya existe, solo añadir a seguimiento
                        was_new = self.db.add_user_followed_artist_muspy(user_id, existing_artist_id, muspy=True)
                        if was_new:
                            added_count += 1
                    else:
                        # Crear nuevo artista a partir de datos de Muspy
                        candidate = {
                            'mbid': mbid,
                            'name': muspy_artist.get('name', ''),
                            'disambiguation': muspy_artist.get('disambiguation', ''),
                            'type': muspy_artist.get('type', ''),
                            'country': muspy_artist.get('country', ''),
                            'score': 100  # Score alto para artistas de Muspy
                        }

                        artist_id = self.db.create_artist_from_candidate(candidate)
                        if artist_id:
                            self.db.add_user_followed_artist_muspy(user_id, artist_id, muspy=True)
                            added_count += 1
                        else:
                            error_count += 1

                    # Pausa breve
                    await asyncio.sleep(0.1)

                except Exception as e:
                    logger.error(f"Error procesando artista {muspy_artist.get('name')}: {e}")
                    error_count += 1

            # Mensaje final
            message = (
                f"🎉 *Importación completada*\n\n"
                f"✅ Artistas importados: {added_count}\n"
                f"❌ Errores: {error_count}\n"
                f"📊 Total procesados: {len(muspy_artists)}\n\n"
                f"Usa `/list` para ver tus artistas seguidos."
            )

            await query.edit_message_text(message, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error importando artistas: {e}")
            await query.edit_message_text("❌ Error durante la importación.")

    async def _handle_disconnect(self, query, user_id: int):
        """Maneja la desconexión de la cuenta de Muspy"""
        if self.db.clear_muspy_credentials(user_id):
            await query.edit_message_text(
                "✅ Cuenta de Muspy desconectada correctamente.\n"
                "Tus datos han sido eliminados de forma segura."
            )
        else:
            await query.edit_message_text(
                "❌ Error al desconectar la cuenta."
            )

    async def _handle_bot_artists(self, query, user_id: int):
        """Maneja la visualización de artistas seguidos del bot (equivalente a /list)"""
        try:
            # Obtener artistas seguidos del usuario
            followed_artists = self.db.get_user_followed_artists(user_id)

            if not followed_artists:
                await query.edit_message_text(
                    "📭 No tienes artistas seguidos aún.\n"
                    "Usa `/addartist <nombre>` para empezar a seguir artistas."
                )
                return

            # Formatear mensaje similar a /list
            message_lines = [f"🎵 *Artistas seguidos del bot* ({len(followed_artists)})\n"]

            # Mostrar hasta 15 artistas para no sobrecargar
            display_artists = followed_artists[:15]

            for i, artist in enumerate(display_artists, 1):
                line = f"{i}. *{artist['name']}*"

                details = []
                if artist.get('country'):
                    details.append(f"🌍 {artist['country']}")
                if artist.get('formed_year'):
                    details.append(f"📅 {artist['formed_year']}")
                if artist.get('disambiguation'):
                    details.append(f"({artist['disambiguation']})")

                if details:
                    line += f" - {' • '.join(details)}"

                # Indicar si está en Muspy
                if artist.get('muspy'):
                    line += " 🎵"

                message_lines.append(line)

            if len(followed_artists) > 15:
                message_lines.append(f"\n_...y {len(followed_artists) - 15} más_")

            message_lines.append(f"\n💡 Usa `/list` para ver la lista completa con enlaces.")

            # Botón para volver
            keyboard = [[
                InlineKeyboardButton("🔙 Volver al menú", callback_data=f"muspy_menu_{user_id}")
            ]]
            reply_markup = InlineKeyboardMarkup(keyboard)

            message = "\n".join(message_lines)
            await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

        except Exception as e:
            logger.error(f"Error mostrando artistas del bot: {e}")
            await query.edit_message_text("❌ Error al obtener artistas del bot.")

    async def _show_releases_page(self, query, user_id: int, releases: List[Dict], page: int = 0):
        """Muestra una página de releases agrupados por fecha con paginación"""
        releases_per_page = 15

        # Agrupar releases por fecha
        releases_by_date = {}
        for release in releases:
            date_str = release.get('date', 'Fecha desconocida')
            if date_str not in releases_by_date:
                releases_by_date[date_str] = []
            releases_by_date[date_str].append(release)

        # Ordenar fechas
        sorted_dates = sorted(releases_by_date.keys(), key=lambda x: x if x != 'Fecha desconocida' else '9999-99-99')

        # Aplanar releases manteniendo agrupación por fecha para paginación
        paginated_items = []
        for date_str in sorted_dates:
            date_releases = releases_by_date[date_str]
            paginated_items.append(('date_header', date_str))
            for release in date_releases:
                paginated_items.append(('release', release))

        # Calcular paginación
        total_pages = (len(paginated_items) + releases_per_page - 1) // releases_per_page

        if page >= total_pages:
            page = total_pages - 1
        elif page < 0:
            page = 0

        start_idx = page * releases_per_page
        end_idx = min(start_idx + releases_per_page, len(paginated_items))
        page_items = paginated_items[start_idx:end_idx]

        # Construir mensaje
        message_lines = [
            f"🎵 *Próximos lanzamientos de Muspy*",
            f"📊 Total: {len(releases)} lanzamientos",
            f"📄 Página {page + 1} de {total_pages}\n"
        ]

        current_date = None
        release_count = 0

        for item_type, item_data in page_items:
            if item_type == 'date_header':
                # Formatear fecha para mostrar
                date_str = item_data
                try:
                    if date_str != 'Fecha desconocida':
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                        formatted_date = date_obj.strftime("%d/%m/%Y")
                    else:
                        formatted_date = date_str
                except:
                    formatted_date = date_str

                if current_date != date_str:
                    current_date = date_str
                    message_lines.append(f"\n📅 *{formatted_date}*")

            elif item_type == 'release':
                release = item_data
                release_count += 1

                artist_name = self.muspy_service.extract_artist_name(release)
                title = self.muspy_service.extract_title(release)
                release_type = self.muspy_service.extract_release_type(release)

                release_line = f"  {release_count}. *{artist_name}* - {title}"
                if release_type != 'Release':
                    release_line += f" _{release_type}_"

                message_lines.append(release_line)

        # Crear botones de navegación
        keyboard = []
        nav_buttons = []

        # Botón anterior
        if page > 0:
            nav_buttons.append(
                InlineKeyboardButton("⬅️ Anterior", callback_data=f"muspy_releases_page_{page-1}_{user_id}")
            )

        # Botón de página actual
        nav_buttons.append(
            InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="muspy_current_page")
        )

        # Botón siguiente
        if page < total_pages - 1:
            nav_buttons.append(
                InlineKeyboardButton("Siguiente ➡️", callback_data=f"muspy_releases_page_{page+1}_{user_id}")
            )

        if nav_buttons:
            keyboard.append(nav_buttons)

        # Botón para volver al menú principal
        keyboard.append([
            InlineKeyboardButton("🔙 Volver al menú", callback_data=f"muspy_menu_{user_id}")
        ])

        message = "\n".join(message_lines)
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

    async def _send_releases_message(self, query, releases: List[Dict]):
        """Envía mensaje formateado con lanzamientos"""
        # Agrupar por artista para estadísticas
        artists_with_releases = set()
        for release in releases:
            artist_name = self.muspy_service.extract_artist_name(release)
            if artist_name != 'Artista desconocido':
                artists_with_releases.add(artist_name)

        header = f"🎵 *Próximos lanzamientos de Muspy*\n"
        header += f"📊 {len(releases)} lanzamientos de {len(artists_with_releases)} artistas\n\n"

        current_text = header

        for i, release in enumerate(releases[:20], 1):  # Limitar a 20 para no sobrecargar
            artist_name = self.muspy_service.extract_artist_name(release)
            title = self.muspy_service.extract_title(release)
            date_str = release.get('date', 'Fecha desconocida')
            release_type = self.muspy_service.extract_release_type(release)

            # Formatear fecha
            try:
                if date_str != 'Fecha desconocida' and date_str:
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    formatted_date = date_obj.strftime("%d/%m/%Y")
                else:
                    formatted_date = date_str
            except:
                formatted_date = date_str

            release_text = f"{i}. *{artist_name}* - *{title}*\n"
            release_text += f"   📅 {formatted_date} • 💿 {release_type}\n\n"

            current_text += release_text

        if len(releases) > 20:
            current_text += f"_...y {len(releases) - 20} lanzamientos más_"

        await query.edit_message_text(current_text, parse_mode='Markdown')

    async def _show_artists_page(self, query, user_id: int, artists: List[Dict], page: int = 0):
        """Muestra una página de artistas de Muspy"""
        artists_per_page = 15
        total_pages = (len(artists) + artists_per_page - 1) // artists_per_page

        if page >= total_pages:
            page = total_pages - 1
        elif page < 0:
            page = 0

        start_idx = page * artists_per_page
        end_idx = min(start_idx + artists_per_page, len(artists))
        page_artists = artists[start_idx:end_idx]

        # Construir texto
        message_lines = [
            f"👥 *Artistas de Muspy*",
            f"📊 Total: {len(artists)} artistas",
            f"📄 Página {page + 1} de {total_pages}\n"
        ]

        for i, artist in enumerate(page_artists, start_idx + 1):
            name = artist.get('name', 'Sin nombre')
            disambiguation = artist.get('disambiguation', '')

            artist_line = f"{i}. *{name}*"
            if disambiguation:
                artist_line += f" _{disambiguation}_"

            message_lines.append(artist_line)

        # Crear botones de navegación
        keyboard = []
        nav_buttons = []

        # Botón anterior
        if page > 0:
            nav_buttons.append(
                InlineKeyboardButton("⬅️ Anterior", callback_data=f"muspy_artists_page_{page-1}_{user_id}")
            )

        # Botón de página actual
        nav_buttons.append(
            InlineKeyboardButton(f"📄 {page + 1}/{total_pages}", callback_data="muspy_current_page")
        )

        # Botón siguiente
        if page < total_pages - 1:
            nav_buttons.append(
                InlineKeyboardButton("Siguiente ➡️", callback_data=f"muspy_artists_page_{page+1}_{user_id}")
            )

        if nav_buttons:
            keyboard.append(nav_buttons)

        # Botón para volver al menú principal
        keyboard.append([
            InlineKeyboardButton("🔙 Volver al menú", callback_data=f"muspy_menu_{user_id}")
        ])

        message = "\n".join(message_lines)
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(message, reply_markup=reply_markup, parse_mode='Markdown')

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
