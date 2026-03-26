#!/usr/bin/env python3
"""
Cliente CalDAV para Radicale.
Autenticación básica HTTP. Permite subir eventos ICS a un calendario Radicale.
"""

import logging
import uuid
import requests
from requests.auth import HTTPBasicAuth
from typing import List, Dict, Optional, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)


class RadicaleClient:
    """Cliente CalDAV mínimo para subir eventos a Radicale con autenticación básica."""

    def __init__(self, url: str, username: str, password: str, calendar: str):
        """
        Args:
            url: URL base del servidor Radicale, ej. 'http://localhost:5232'
            username: Nombre de usuario Radicale
            password: Contraseña Radicale
            calendar: Nombre/ruta del calendario, ej. 'conciertos'
        """
        self.base_url = url.rstrip('/')
        self.username = username
        self.password = password
        self.calendar = calendar.strip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth

    def _calendar_url(self) -> str:
        return f"{self.base_url}/{self.username}/{self.calendar}/"

    def test_connection(self) -> Tuple[bool, str]:
        """Verifica que la conexión y las credenciales son correctas."""
        try:
            resp = self.session.request(
                'PROPFIND',
                self._calendar_url(),
                headers={'Depth': '0', 'Content-Type': 'application/xml'},
                data='''<?xml version="1.0" encoding="utf-8"?>
<propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>''',
                timeout=10,
            )
            if resp.status_code in (200, 207):
                return True, "Conexión correcta"
            elif resp.status_code == 401:
                return False, "Credenciales incorrectas (401)"
            elif resp.status_code == 404:
                return False, f"Calendario '{self.calendar}' no encontrado (404)"
            else:
                return False, f"Error inesperado: HTTP {resp.status_code}"
        except requests.exceptions.ConnectionError:
            return False, f"No se puede conectar a {self.base_url}"
        except requests.exceptions.Timeout:
            return False, "Timeout conectando a Radicale"
        except Exception as e:
            return False, f"Error: {e}"

    def push_ics_event(self, ics_content: str, event_uid: str = None) -> Tuple[bool, str]:
        """
        Sube un único evento ICS al calendario.

        Args:
            ics_content: Contenido ICS completo (VCALENDAR con un VEVENT)
            event_uid: UID del evento. Si no se proporciona, se genera uno aleatorio.

        Returns:
            (éxito, mensaje)
        """
        if not event_uid:
            event_uid = str(uuid.uuid4())

        event_url = f"{self._calendar_url()}{event_uid}.ics"

        try:
            resp = self.session.put(
                event_url,
                data=ics_content.encode('utf-8'),
                headers={'Content-Type': 'text/calendar; charset=utf-8'},
                timeout=10,
            )
            if resp.status_code in (200, 201, 204):
                return True, f"Evento subido correctamente"
            elif resp.status_code == 401:
                return False, "Credenciales incorrectas (401)"
            else:
                return False, f"Error HTTP {resp.status_code}: {resp.text[:200]}"
        except Exception as e:
            return False, f"Error subiendo evento: {e}"

    def push_events_bulk(self, events: List[Dict], event_type: str = 'concert') -> Tuple[int, int, List[str]]:
        """
        Sube múltiples eventos al calendario Radicale.

        Args:
            events: Lista de dicts de conciertos o discos
            event_type: 'concert' o 'release'

        Returns:
            (subidos, errores, lista_de_errores)
        """
        pushed = 0
        errors = 0
        error_msgs = []

        for event in events:
            try:
                ics, uid = _build_event_ics(event, event_type)
                ok, msg = self.push_ics_event(ics, uid)
                if ok:
                    pushed += 1
                else:
                    errors += 1
                    error_msgs.append(msg)
            except Exception as e:
                errors += 1
                error_msgs.append(str(e))

        return pushed, errors, error_msgs

    def list_calendars(self) -> Tuple[List[str], str]:
        """Lista los calendarios disponibles para el usuario."""
        user_url = f"{self.base_url}/{self.username}/"
        try:
            resp = self.session.request(
                'PROPFIND',
                user_url,
                headers={'Depth': '1', 'Content-Type': 'application/xml'},
                data='''<?xml version="1.0" encoding="utf-8"?>
<propfind xmlns="DAV:"><prop><displayname/><resourcetype/></prop></propfind>''',
                timeout=10,
            )
            if resp.status_code not in (200, 207):
                return [], f"HTTP {resp.status_code}"

            # Parseo básico: buscar href de calendarios
            import re
            hrefs = re.findall(r'<[Dd]:\s*href[^>]*>([^<]+)</[Dd]:\s*href>', resp.text)
            calendars = [
                h.rstrip('/').split('/')[-1]
                for h in hrefs
                if h.rstrip('/') != user_url.rstrip('/')
                and h.rstrip('/') != f"/{self.username}"
            ]
            return [c for c in calendars if c], "OK"
        except Exception as e:
            return [], str(e)


# ──────────────────────────────────────────────
# Helpers para construir ICS de un solo evento
# ──────────────────────────────────────────────

def _escape(text: str) -> str:
    """Escapa texto para RFC 5545."""
    return str(text).replace('\\', '\\\\').replace('\n', '\\n').replace(',', '\\,').replace(';', '\\;')


def _build_event_ics(event: Dict, event_type: str) -> Tuple[str, str]:
    """
    Construye un VCALENDAR con un único VEVENT a partir de un dict de concierto o disco.

    Returns:
        (ics_content, uid)
    """
    uid = str(uuid.uuid4())
    now = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')

    if event_type == 'release':
        return _build_release_ics(event, uid, now), uid
    else:
        return _build_concert_ics(event, uid, now), uid


def _build_concert_ics(event: Dict, uid: str, dtstamp: str) -> str:
    artist = _escape(event.get('artist_name', event.get('artist', 'Artista desconocido')))
    venue = _escape(event.get('venue', ''))
    city = _escape(event.get('city', ''))
    country = _escape(event.get('country', ''))
    date_str = event.get('date', '')
    time_str = event.get('time', '')
    url = event.get('url', '')

    summary = f"{artist}"
    if venue:
        summary += f" @ {venue}"

    location_parts = [p for p in [venue, city, country] if p]
    location = _escape(', '.join(location_parts))

    description_parts = [f"Artista: {artist}"]
    if venue:
        description_parts.append(f"Recinto: {venue}")
    if city:
        description_parts.append(f"Ciudad: {city}")
    if url:
        description_parts.append(f"Entradas: {url}")
    description = _escape(' | '.join(description_parts))

    # Fecha/hora del evento
    if date_str and len(date_str) >= 10:
        date_clean = date_str[:10].replace('-', '')
        if time_str and len(time_str) >= 5:
            time_clean = time_str[:5].replace(':', '') + '00'
            dtstart = f"DTSTART:{date_clean}T{time_clean}"
            dtend_hour = int(time_str[:2]) + 3
            dtend = f"DTEND:{date_clean}T{dtend_hour:02d}{time_str[3:5]}00"
        else:
            dtstart = f"DTSTART;VALUE=DATE:{date_clean}"
            dtend = f"DTEND;VALUE=DATE:{date_clean}"
    else:
        dtstart = f"DTSTART;VALUE=DATE:{dtstamp[:8]}"
        dtend = f"DTEND;VALUE=DATE:{dtstamp[:8]}"

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//tumtumpa//bot_conciertos//ES",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        dtstart,
        dtend,
        f"SUMMARY:{_escape(summary)}",
        f"LOCATION:{location}",
        f"DESCRIPTION:{description}",
    ]
    if url:
        lines.append(f"URL:{url}")
    lines += ["END:VEVENT", "END:VCALENDAR"]

    return "\r\n".join(lines) + "\r\n"


def _build_release_ics(event: Dict, uid: str, dtstamp: str) -> str:
    from apis.muspy_service import MuspyService
    svc = MuspyService()
    artist = _escape(svc.extract_artist_name(event))
    title = _escape(svc.extract_title(event))
    release_type = _escape(svc.extract_release_type(event))
    date_str = event.get('date', '')

    summary = f"{artist} - {title}"
    if release_type:
        summary += f" [{release_type}]"

    if date_str and len(date_str) >= 10:
        date_clean = date_str[:10].replace('-', '')
        dtstart = f"DTSTART;VALUE=DATE:{date_clean}"
        dtend = f"DTEND;VALUE=DATE:{date_clean}"
    else:
        dtstart = f"DTSTART;VALUE=DATE:{dtstamp[:8]}"
        dtend = f"DTEND;VALUE=DATE:{dtstamp[:8]}"

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//tumtumpa//bot_conciertos//ES",
        "BEGIN:VEVENT",
        f"UID:{uid}",
        f"DTSTAMP:{dtstamp}",
        dtstart,
        dtend,
        f"SUMMARY:{_escape(summary)}",
        f"DESCRIPTION:Lanzamiento: {artist} - {title}",
        "END:VEVENT",
        "END:VCALENDAR",
    ]

    return "\r\n".join(lines) + "\r\n"
