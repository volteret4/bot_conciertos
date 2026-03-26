#!/usr/bin/env python3
"""
Notificaciones de administrador.

Envía un mensaje a un chat de Telegram cuando ocurre un evento relevante en el bot.

Configura en .env:
    ADMIN_CHAT_ID=<chat_id del admin>
    ADMIN_BOT_TOKEN=<token del bot admin>  # opcional; usa el bot principal si no se define
"""

import os
import logging
from datetime import datetime

import requests

logger = logging.getLogger(__name__)

_ADMIN_CHAT_ID: str = ""
_ADMIN_BOT_TOKEN: str = ""
_initialized: bool = False


def _init():
    global _ADMIN_CHAT_ID, _ADMIN_BOT_TOKEN, _initialized
    if _initialized:
        return
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    _ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "")
    _ADMIN_BOT_TOKEN = (
        os.environ.get("ADMIN_BOT_TOKEN")
        or os.environ.get("TELEGRAM_BOT_CONCIERTOS")
        or os.environ.get("TELEGRAM_BOT_TOKEN", "")
    )
    if _ADMIN_CHAT_ID and _ADMIN_BOT_TOKEN:
        _initialized = True
        logger.info(f"Admin notify configurado: chat_id={_ADMIN_CHAT_ID[:4]}…")
    else:
        logger.warning(
            f"Admin notify no configurado — ADMIN_CHAT_ID={'✓' if _ADMIN_CHAT_ID else '✗'} "
            f"ADMIN_BOT_TOKEN={'✓' if _ADMIN_BOT_TOKEN else '✗'}"
        )


# Emojis de cabecera por usuario (rotación por hash del nombre)
_USER_COLORS = ["🔴", "🟠", "🟡", "🟢", "🔵", "🟣", "🟤", "⚪", "🔶", "🔷"]


def _user_header(username: str) -> str:
    """Cabecera colorida con el nombre de usuario."""
    color = _USER_COLORS[hash(username) % len(_USER_COLORS)]
    return f"{color} *{username}*"


def notify(event: str, details: str = "", username: str = "", silent: bool = False) -> bool:
    """
    Envía una notificación al administrador.

    Args:
        event:    Nombre del evento (p.ej. "artista_añadido")
        details:  Información adicional (artista, configuración, etc.)
        username: Nombre del usuario que generó el evento (encabeza el mensaje)
        silent:   Si True, no loguea errores
    """
    _init()

    if not _ADMIN_CHAT_ID or not _ADMIN_BOT_TOKEN:
        return False

    now = datetime.now().strftime("%d/%m/%Y %H:%M")
    icon = _event_icon(event)

    lines = []
    if username:
        lines.append(_user_header(username))
        lines.append("")
    lines.append(f"{icon} *{event}*  _{now}_")
    if details:
        lines.append("")
        lines.append(details)

    text = "\n".join(lines)

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{_ADMIN_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": _ADMIN_CHAT_ID,
                "text": text,
                "parse_mode": "Markdown",
                "disable_notification": False,
            },
            timeout=8,
        )
        if resp.status_code != 200:
            logger.warning(f"Admin notify HTTP {resp.status_code}: {resp.text}")
        return resp.status_code == 200
    except Exception as e:
        if not silent:
            logger.warning(f"No se pudo enviar notificación admin: {e}")
        return False


async def notify_async(event: str, details: str = "", username: str = "") -> bool:
    """Versión async."""
    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: notify(event, details, username))


def _event_icon(event: str) -> str:
    icons = {
        "nuevo_usuario":        "👤",
        "artista_añadido":      "🎵",
        "artista_eliminado":    "🗑️",
        "muspy_conectado":      "🔗",
        "muspy_desconectado":   "🔌",
        "muspy_importacion":    "📥",
        "lastfm_conectado":     "🎧",
        "lastfm_importacion":   "📥",
        "radicale_configurado": "📅",
        "notificaciones":       "🔔",
        "busqueda":             "🔍",
        "error":                "❌",
    }
    return icons.get(event, "ℹ️")
