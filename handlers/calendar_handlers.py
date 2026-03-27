#!/usr/bin/env python3
"""
Handlers para funcionalidades de calendario (/cal).
Genera archivos ICS y sube eventos a Radicale (CalDAV).
"""

import logging
import tempfile
import os
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import admin_notify

logger = logging.getLogger(__name__)


class CalendarHandlers:

    def __init__(self, database, muspy_service):
        self.db = database
        self.muspy_service = muspy_service

    def _get_username(self, user_id: int) -> str:
        try:
            conn = self.db.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            row = cur.fetchone()
            conn.close()
            return row[0] if row else str(user_id)
        except Exception:
            return str(user_id)

    # ─── /cal panel principal ─────────────────────────────────────────────────

    async def cal_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        user_id = self._get_user_id(update)
        if not user_id:
            await update.message.reply_text(
                "❌ Primero debes registrarte con `/adduser <tu_nombre>`",
                parse_mode='Markdown'
            )
            return

        radicale_cfg = self.db.get_radicale_config(user_id)
        radicale_status = f"✅ {radicale_cfg['calendar']}" if radicale_cfg else "❌ No configurado"

        text = (
            "📅 *Generador de Calendarios*\n\n"
            "🎵 *Conciertos*: todos los conciertos de tus artistas en tus países\n"
            "💿 *Discos*: próximos lanzamientos de Muspy\n\n"
            f"☁️ Radicale: {radicale_status}\n\n"
            "Los archivos .ics son compatibles con Google Calendar, Apple Calendar, Outlook, etc."
        )

        keyboard = [
            [
                InlineKeyboardButton("🎵 Conciertos (ICS)", callback_data=f"cal_concerts_{user_id}"),
                InlineKeyboardButton("💿 Discos (ICS)", callback_data=f"cal_releases_{user_id}"),
            ],
        ]
        if radicale_cfg:
            keyboard.append([
                InlineKeyboardButton("☁️ Conciertos → Radicale", callback_data=f"cal_rad_concerts_{user_id}"),
                InlineKeyboardButton("☁️ Discos → Radicale", callback_data=f"cal_rad_releases_{user_id}"),
            ])

        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

    # ─── Router de callbacks ──────────────────────────────────────────────────

    async def cal_callback_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        query = update.callback_query
        await query.answer()

        data = query.data  # e.g. "cal_concerts_42" or "cal_rad_concerts_42"
        parts = data.split("_")

        # Formato: cal_{action}_{user_id}  o  cal_rad_{action}_{user_id}
        try:
            if parts[1] == "rad":
                action = parts[2]          # concerts / releases
                user_id = int(parts[3])
                radicale = True
            else:
                action = parts[1]          # concerts / releases
                user_id = int(parts[2])
                radicale = False
        except (IndexError, ValueError):
            await query.edit_message_text("❌ Error en el callback.")
            return

        if not self._verify_user(update, user_id):
            await query.edit_message_text("❌ Error de autenticación.")
            return

        try:
            if action == "concerts":
                if radicale:
                    await self._handle_radicale_concerts(query, user_id)
                else:
                    await self._handle_concerts_calendar(query, user_id)
            elif action == "releases":
                if radicale:
                    await self._handle_radicale_releases(query, user_id)
                else:
                    await self._handle_releases_calendar(query, user_id)
            else:
                await query.edit_message_text("❌ Acción no reconocida.")
        except Exception as e:
            logger.error(f"Error en cal_callback_handler: {e}")
            await query.edit_message_text("❌ Error generando el calendario.")

    # ─── ICS: conciertos ──────────────────────────────────────────────────────

    async def _handle_concerts_calendar(self, query, user_id: int):
        await query.edit_message_text("📂 Obteniendo conciertos de la base de datos...")

        try:
            from user_services import UserServices
            user_config = UserServices(self.db).get_user_services(user_id)
            user_countries = user_config.get('countries', set())

            concerts = self._fetch_concerts_from_db(user_id, user_countries)

            if not concerts:
                countries_text = ", ".join(sorted(user_countries)) if user_countries else "ninguno"
                await query.edit_message_text(
                    f"📭 No se encontraron conciertos futuros en tus países ({countries_text}).\n"
                    f"💡 Usa /search primero para buscar conciertos."
                )
                return

            await query.edit_message_text(f"📅 Generando calendario con {len(concerts)} conciertos...")
            ics_content = self._generate_concerts_ics(concerts)
            await self._send_ics_file(
                query,
                ics_content,
                f"conciertos_{datetime.now().strftime('%Y%m%d')}.ics",
                f"📅 *Calendario de Conciertos*\n\n"
                f"🎵 {len(concerts)} conciertos\n"
                f"🌍 Países: {', '.join(sorted(user_countries))}",
            )
            await query.edit_message_text("✅ Calendario de conciertos enviado.")
            await admin_notify.notify_async(
                "calendario",
                f"ICS conciertos · {len(concerts)} eventos",
                username=self._get_username(user_id),
            )

        except Exception as e:
            logger.error(f"Error generando calendario de conciertos: {e}", exc_info=True)
            await query.edit_message_text("❌ Error generando el calendario de conciertos.")

    # ─── ICS: discos ──────────────────────────────────────────────────────────

    async def _handle_releases_calendar(self, query, user_id: int):
        await query.edit_message_text("🔍 Obteniendo lanzamientos...")

        try:
            releases = await self._fetch_releases(user_id)
            if not releases:
                msg = "📭 No se encontraron próximos lanzamientos."
                credentials = self.db.get_muspy_credentials(user_id)
                if not credentials:
                    msg += "\n\n💡 Configura tu cuenta de Muspy con `/muspy`."
                await query.edit_message_text(msg)
                return

            await query.edit_message_text(f"📅 Generando calendario con {len(releases)} lanzamientos...")
            ics_content = self._generate_releases_ics(releases)
            await self._send_ics_file(
                query,
                ics_content,
                f"lanzamientos_{datetime.now().strftime('%Y%m%d')}.ics",
                f"📅 *Calendario de Lanzamientos*\n\n💿 {len(releases)} lanzamientos",
            )
            await query.edit_message_text("✅ Calendario de lanzamientos enviado.")

        except Exception as e:
            logger.error(f"Error generando calendario de releases: {e}")
            await query.edit_message_text("❌ Error generando el calendario de lanzamientos.")

    # ─── Radicale: conciertos ─────────────────────────────────────────────────

    async def _handle_radicale_concerts(self, query, user_id: int):
        await query.edit_message_text("📂 Obteniendo conciertos de la base de datos...")

        radicale_cfg = self.db.get_radicale_config(user_id)
        if not radicale_cfg:
            await query.edit_message_text(
                "❌ Radicale no configurado.\nUsa `/radicale` para configurarlo."
            )
            return

        from user_services import UserServices
        user_config = UserServices(self.db).get_user_services(user_id)
        user_countries = user_config.get('countries', set())

        concerts = self._fetch_concerts_from_db(user_id, user_countries)

        if not concerts:
            await query.edit_message_text(
                "📭 No se encontraron conciertos futuros en la base de datos.\n"
                "💡 Usa /search primero para buscar conciertos."
            )
            return

        await query.edit_message_text(f"☁️ Subiendo {len(concerts)} conciertos a Radicale...")

        from apis.radicale import RadicaleClient
        import asyncio
        client = RadicaleClient(
            url=radicale_cfg['url'],
            username=radicale_cfg['username'],
            password=radicale_cfg['password'],
            calendar=radicale_cfg['calendar'],
        )

        loop = asyncio.get_event_loop()
        pushed, errors, error_msgs = await loop.run_in_executor(
            None, lambda: client.push_events_bulk(concerts, event_type='concert')
        )

        msg = (
            f"☁️ *Subida a Radicale completada*\n\n"
            f"✅ Eventos subidos: {pushed}\n"
            f"❌ Errores: {errors}"
        )
        if error_msgs:
            msg += f"\n\n_Errores: {'; '.join(error_msgs[:3])}_"

        await query.edit_message_text(msg, parse_mode='Markdown')
        if pushed > 0:
            await admin_notify.notify_async(
                "calendario",
                f"Radicale conciertos · {pushed} subidos · {radicale_cfg.get('calendar', '')}",
                username=self._get_username(user_id),
            )

    # ─── Radicale: discos ─────────────────────────────────────────────────────

    async def _handle_radicale_releases(self, query, user_id: int):
        await query.edit_message_text("🔍 Preparando lanzamientos para Radicale...")

        radicale_cfg = self.db.get_radicale_config(user_id)
        if not radicale_cfg:
            await query.edit_message_text(
                "❌ Radicale no configurado.\nUsa `/radicale` para configurarlo."
            )
            return

        releases = await self._fetch_releases(user_id)
        if not releases:
            await query.edit_message_text("📭 No se encontraron próximos lanzamientos.")
            return

        await query.edit_message_text(f"☁️ Subiendo {len(releases)} lanzamientos a Radicale...")

        from apis.radicale import RadicaleClient
        import asyncio
        client = RadicaleClient(
            url=radicale_cfg['url'],
            username=radicale_cfg['username'],
            password=radicale_cfg['password'],
            calendar=radicale_cfg['calendar'],
        )

        loop = asyncio.get_event_loop()
        pushed, errors, error_msgs = await loop.run_in_executor(
            None, lambda: client.push_events_bulk(releases, event_type='release')
        )

        msg = (
            f"☁️ *Subida a Radicale completada*\n\n"
            f"✅ Eventos subidos: {pushed}\n"
            f"❌ Errores: {errors}"
        )
        if error_msgs:
            msg += f"\n\n_Errores: {'; '.join(error_msgs[:3])}_"

        await query.edit_message_text(msg, parse_mode='Markdown')

    # ─── Helpers de fetch ─────────────────────────────────────────────────────

    def _fetch_concerts_from_db(self, user_id: int, user_countries) -> List[Dict]:
        """Lee conciertos futuros del DB para los artistas seguidos, filtrados por país."""
        followed_artists = self.db.get_user_followed_artists(user_id)
        if not followed_artists:
            return []

        today_str = date.today().isoformat()
        artist_names_lower = [a['name'].lower() for a in followed_artists]

        conn = self.db.get_connection()
        try:
            cursor = conn.cursor()
            placeholders = ','.join('?' * len(artist_names_lower))
            cursor.execute(f"""
                SELECT DISTINCT artist_name, concert_name, venue, city, country, country_code,
                       date, time, url, source
                FROM concerts
                WHERE date >= ? AND LOWER(artist_name) IN ({placeholders})
                ORDER BY date ASC
            """, [today_str] + artist_names_lower)
            rows = [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

        if not user_countries:
            return rows

        # Filtrar por país (acepta código ISO o nombre completo)
        upper_countries = {c.upper() for c in user_countries}
        filtered = []
        for r in rows:
            country_val = (r.get('country') or '').upper()
            code_val = (r.get('country_code') or '').upper()
            if not country_val and not code_val:
                filtered.append(r)
            elif country_val in upper_countries or code_val in upper_countries:
                filtered.append(r)
        return filtered

    async def _fetch_releases(self, user_id: int) -> List[Dict]:
        """Obtiene lanzamientos futuros de Muspy."""
        credentials = self.db.get_muspy_credentials(user_id)
        if not credentials:
            return []

        email, password, userid = credentials
        try:
            releases, _ = self.muspy_service.get_user_releases(email, password, userid)
            today_str = date.today().strftime("%Y-%m-%d")
            return sorted(
                [r for r in releases if r.get('date', '0000-00-00') >= today_str],
                key=lambda r: r.get('date', '')
            )
        except Exception as e:
            logger.error(f"Error obteniendo releases de Muspy para usuario {user_id}: {e}")
            return []

    async def _send_ics_file(self, query, ics_content: str, filename: str, caption: str):
        """Escribe el ICS en un fichero temporal y lo envía como documento."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ics', delete=False, encoding='utf-8') as f:
            f.write(ics_content)
            tmp_path = f.name
        try:
            with open(tmp_path, 'rb') as f:
                await query.message.reply_document(
                    document=f,
                    filename=filename,
                    caption=caption,
                    parse_mode='Markdown',
                )
        finally:
            os.unlink(tmp_path)

    # ─── Generación ICS ───────────────────────────────────────────────────────

    def _generate_concerts_ics(self, concerts: List[Dict]) -> str:
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//tumtumpa//bot_conciertos//ES",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Conciertos",
        ]

        now_stamp = datetime.now().strftime('%Y%m%dT%H%M%SZ')

        for concert in concerts:
            artist = concert.get('artist', concert.get('artist_name', 'Artista desconocido'))
            venue = concert.get('venue', '')
            city = concert.get('city', '')
            country = concert.get('country', '')
            date_str = concert.get('date', '')
            time_str = concert.get('time', '')
            url = concert.get('url', '')
            source = concert.get('source', '')

            summary = artist
            if venue:
                summary += f" @ {venue}"

            location_parts = [p for p in [venue, city, country] if p]
            location = ', '.join(location_parts)

            desc_parts = [f"Artista: {artist}"]
            if venue:
                desc_parts.append(f"Recinto: {venue}")
            if city:
                desc_parts.append(f"Ciudad: {city}")
            if source:
                desc_parts.append(f"Fuente: {source}")
            if url:
                desc_parts.append(f"Entradas: {url}")

            if not date_str or len(date_str) < 10:
                continue
            try:
                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
            except ValueError:
                continue

            if time_str and len(time_str) >= 5:
                try:
                    t = datetime.strptime(time_str[:5], '%H:%M')
                    start_dt = date_obj.replace(hour=t.hour, minute=t.minute)
                    end_dt = start_dt + timedelta(hours=3)
                    dtstart = f"DTSTART:{start_dt.strftime('%Y%m%dT%H%M%S')}"
                    dtend = f"DTEND:{end_dt.strftime('%Y%m%dT%H%M%S')}"
                except ValueError:
                    dtstart = f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}"
                    dtend = f"DTEND;VALUE=DATE:{(date_obj + timedelta(days=1)).strftime('%Y%m%d')}"
            else:
                dtstart = f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}"
                dtend = f"DTEND;VALUE=DATE:{(date_obj + timedelta(days=1)).strftime('%Y%m%d')}"

            import uuid
            uid = str(uuid.uuid4())
            event = [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{now_stamp}",
                dtstart,
                dtend,
                f"SUMMARY:{self._esc(summary)}",
                f"LOCATION:{self._esc(location)}",
                f"DESCRIPTION:{self._esc(' | '.join(desc_parts))}",
            ]
            if url:
                event.append(f"URL:{url}")
            event.append("END:VEVENT")
            lines.extend(event)

        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    def _generate_releases_ics(self, releases: List[Dict]) -> str:
        lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//tumtumpa//bot_conciertos//ES",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-CALNAME:Lanzamientos",
        ]

        now_stamp = datetime.now().strftime('%Y%m%dT%H%M%SZ')

        for release in releases:
            artist = self.muspy_service.extract_artist_name(release) if self.muspy_service else release.get('artist', '')
            title = self.muspy_service.extract_title(release) if self.muspy_service else release.get('title', '')
            rel_type = self.muspy_service.extract_release_type(release) if self.muspy_service else ''
            date_str = release.get('date', '')

            summary = f"{artist} — {title}"
            if rel_type:
                summary += f" [{rel_type}]"

            if not date_str or len(date_str) < 10:
                continue
            try:
                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
            except ValueError:
                continue

            import uuid
            uid = str(uuid.uuid4())
            lines.extend([
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{now_stamp}",
                f"DTSTART;VALUE=DATE:{date_obj.strftime('%Y%m%d')}",
                f"DTEND;VALUE=DATE:{(date_obj + timedelta(days=1)).strftime('%Y%m%d')}",
                f"SUMMARY:{self._esc(summary)}",
                f"DESCRIPTION:Lanzamiento: {self._esc(artist)} - {self._esc(title)}",
                "END:VEVENT",
            ])

        lines.append("END:VCALENDAR")
        return "\r\n".join(lines)

    def _esc(self, text: str) -> str:
        return (str(text)
                .replace('\\', '\\\\')
                .replace('\n', '\\n')
                .replace(',', '\\,')
                .replace(';', '\\;'))

    # ─── Autenticación ────────────────────────────────────────────────────────

    def _get_user_id(self, update: Update) -> Optional[int]:
        if hasattr(update, 'callback_query') and update.callback_query:
            chat_id = update.callback_query.message.chat_id
        else:
            chat_id = update.effective_chat.id
        user = self.db.get_user_by_chat_id(chat_id)
        return user['id'] if user else None

    def _verify_user(self, update: Update, expected_user_id: int) -> bool:
        return self._get_user_id(update) == expected_user_id
