#!/usr/bin/env python3
"""Google Calendar API integration via OAuth2."""

import json
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/calendar.events']
REDIRECT_URI = 'http://localhost'

_CLIENT_CONFIG_TEMPLATE = {
    "installed": {
        "redirect_uris": [REDIRECT_URI],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}


class GoogleCalendarService:
    """Gestiona OAuth2 y operaciones de Google Calendar API."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._client_config = {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uris": [REDIRECT_URI],
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        }

    def get_auth_url(self) -> str:
        """Genera la URL de autorización OAuth2 de Google."""
        from google_auth_oauthlib.flow import Flow
        flow = Flow.from_client_config(
            self._client_config, scopes=SCOPES, redirect_uri=REDIRECT_URI
        )
        auth_url, _ = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true',
            prompt='consent',
        )
        return auth_url

    def exchange_code(self, code: str) -> Dict:
        """
        Intercambia el código de autorización por tokens usando el endpoint de Google directamente.
        Evita la verificación de 'state' de google-auth-oauthlib que falla en bots.
        """
        import requests as _requests
        resp = _requests.post(
            'https://oauth2.googleapis.com/token',
            data={
                'code': code.strip(),
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'redirect_uri': REDIRECT_URI,
                'grant_type': 'authorization_code',
            },
            timeout=15,
        )
        resp.raise_for_status()
        token_json = resp.json()

        if 'error' in token_json:
            raise ValueError(
                f"Error de Google OAuth: {token_json.get('error_description', token_json['error'])}"
            )

        return {
            'token': token_json.get('access_token'),
            'refresh_token': token_json.get('refresh_token'),
            'token_uri': 'https://oauth2.googleapis.com/token',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'scopes': token_json.get('scope', '').split() or list(SCOPES),
            'expiry': None,
        }

    def _build_credentials(self, token_data: Dict):
        from google.oauth2.credentials import Credentials
        expiry = None
        if token_data.get('expiry'):
            try:
                expiry = datetime.fromisoformat(token_data['expiry'])
            except (ValueError, TypeError):
                pass
        return Credentials(
            token=token_data.get('token'),
            refresh_token=token_data.get('refresh_token'),
            token_uri=token_data.get('token_uri', 'https://oauth2.googleapis.com/token'),
            client_id=token_data.get('client_id', self.client_id),
            client_secret=token_data.get('client_secret', self.client_secret),
            scopes=token_data.get('scopes', list(SCOPES)),
            expiry=expiry,
        )

    def _get_service(self, token_data: Dict):
        """Construye el cliente de Calendar API, refrescando el token si es necesario."""
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
        creds = self._build_credentials(token_data)
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
        return build('calendar', 'v3', credentials=creds), creds

    def get_refreshed_token_data(self, token_data: Dict) -> Optional[Dict]:
        """Refresca el token si ha expirado. Devuelve token_data actualizado o None si falla."""
        try:
            _, creds = self._get_service(token_data)
            return {
                **token_data,
                'token': creds.token,
                'expiry': creds.expiry.isoformat() if creds.expiry else None,
            }
        except Exception as e:
            logger.error(f"Error refrescando token de Google: {e}")
            return None

    def push_concerts(
        self, token_data: Dict, concerts: List[Dict], calendar_id: str = 'primary'
    ) -> Tuple[int, int, int, Dict]:
        """
        Sube conciertos a Google Calendar.
        Usa iCalUID basado en concert_hash para evitar duplicados.
        Devuelve (nuevos, ya_existían, errores, token_actualizado).
        """
        try:
            from googleapiclient.errors import HttpError
            service, creds = self._get_service(token_data)
            updated_token = {
                **token_data,
                'token': creds.token,
                'expiry': creds.expiry.isoformat() if creds.expiry else None,
            }
        except Exception as e:
            logger.error(f"Error construyendo servicio de Google Calendar: {e}")
            return 0, 0, len(concerts), token_data

        from googleapiclient.errors import HttpError
        new_count, already_count, errors = 0, 0, 0
        for concert in concerts:
            try:
                event = _concert_to_gcal_event(concert)
                service.events().insert(calendarId=calendar_id, body=event).execute()
                new_count += 1
            except HttpError as e:
                if e.resp.status == 409:
                    already_count += 1
                else:
                    logger.error(f"HttpError añadiendo concierto a GCal: {e}")
                    errors += 1
            except Exception as e:
                logger.error(f"Error añadiendo concierto a GCal: {e}")
                errors += 1
        return new_count, already_count, errors, updated_token

    def push_releases(
        self, token_data: Dict, releases: List[Dict],
        calendar_id: str = 'primary', muspy_service=None
    ) -> Tuple[int, int, int, Dict]:
        """
        Sube lanzamientos a Google Calendar.
        Usa iCalUID basado en mb_release_id para evitar duplicados.
        Devuelve (nuevos, ya_existían, errores, token_actualizado).
        """
        try:
            from googleapiclient.errors import HttpError
            service, creds = self._get_service(token_data)
            updated_token = {
                **token_data,
                'token': creds.token,
                'expiry': creds.expiry.isoformat() if creds.expiry else None,
            }
        except Exception as e:
            logger.error(f"Error construyendo servicio de Google Calendar: {e}")
            return 0, 0, len(releases), token_data

        from googleapiclient.errors import HttpError
        new_count, already_count, errors = 0, 0, 0
        for release in releases:
            try:
                event = _release_to_gcal_event(release, muspy_service)
                service.events().insert(calendarId=calendar_id, body=event).execute()
                new_count += 1
            except HttpError as e:
                if e.resp.status == 409:
                    already_count += 1
                else:
                    logger.error(f"HttpError añadiendo lanzamiento a GCal: {e}")
                    errors += 1
            except Exception as e:
                logger.error(f"Error añadiendo lanzamiento a GCal: {e}")
                errors += 1
        return new_count, already_count, errors, updated_token


def _make_ical_uid(prefix: str, key: str) -> str:
    """Genera un iCalUID determinista para evitar duplicados en Google Calendar."""
    import hashlib
    h = hashlib.md5(key.encode()).hexdigest()
    return f"{prefix}-{h}@tumtumpa.bot"


def _concert_to_gcal_event(concert: Dict) -> Dict:
    artist = concert.get('artist') or concert.get('artist_name', '')
    venue = concert.get('venue', '')
    city = concert.get('city', '')
    country = concert.get('country', '')
    date_str = concert.get('date', '')
    time_str = concert.get('time', '')
    url = concert.get('url', '')

    summary = f"{artist} @ {venue}" if venue else artist
    location_parts = [p for p in [venue, city, country] if p]
    location = ', '.join(location_parts)

    if time_str and len(time_str) >= 5:
        try:
            start_dt = datetime.strptime(f"{date_str[:10]} {time_str[:5]}", '%Y-%m-%d %H:%M')
            end_dt = start_dt + timedelta(hours=3)
            event_start = {'dateTime': start_dt.isoformat(), 'timeZone': 'Europe/Madrid'}
            event_end = {'dateTime': end_dt.isoformat(), 'timeZone': 'Europe/Madrid'}
        except ValueError:
            event_start = {'date': date_str[:10]}
            event_end = {'date': date_str[:10]}
    else:
        event_start = {'date': date_str[:10]}
        event_end = {'date': date_str[:10]}

    desc_parts = [f"Concierto: {artist}"]
    if venue:
        desc_parts.append(f"Recinto: {venue}")
    if city:
        desc_parts.append(f"Ciudad: {city}")
    if url:
        desc_parts.append(f"Entradas: {url}")

    # iCalUID determinista: usa concert_hash si está disponible, si no lo genera
    concert_hash = concert.get('concert_hash') or ''
    if not concert_hash:
        concert_hash = f"{artist.lower()}-{venue.lower()}-{date_str}"
    ical_uid = _make_ical_uid('concert', concert_hash)

    event: Dict = {
        'iCalUID': ical_uid,
        'summary': summary,
        'location': location,
        'start': event_start,
        'end': event_end,
        'description': '\n'.join(desc_parts),
    }
    if url:
        event['source'] = {'url': url, 'title': 'Entradas - Ticketmaster'}
    return event


def _release_to_gcal_event(release: Dict, muspy_service=None) -> Dict:
    if muspy_service:
        artist = muspy_service.extract_artist_name(release)
        title = muspy_service.extract_title(release)
        rel_type = muspy_service.extract_release_type(release)
        mb_id = muspy_service.extract_release_mbid(release) or ''
    else:
        artist = release.get('artist', '')
        title = release.get('title', '')
        rel_type = ''
        mb_id = release.get('mbid', '')

    date_str = (release.get('date', '') or '')[:10]
    summary = f"💿 {artist} — {title}"
    if rel_type:
        summary += f" [{rel_type}]"

    # iCalUID determinista: usa mb_release_id si está disponible
    uid_key = mb_id or f"{artist.lower()}-{title.lower()}-{date_str}"
    ical_uid = _make_ical_uid('release', uid_key)

    return {
        'iCalUID': ical_uid,
        'summary': summary,
        'start': {'date': date_str},
        'end': {'date': date_str},
        'description': f"Lanzamiento: {artist}\nÁlbum: {title}\nFuente: Muspy",
    }
