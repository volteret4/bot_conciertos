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
        """Intercambia el código de autorización por tokens. Devuelve dict con los tokens."""
        from google_auth_oauthlib.flow import Flow
        os.environ.setdefault('OAUTHLIB_INSECURE_TRANSPORT', '1')
        flow = Flow.from_client_config(
            self._client_config, scopes=SCOPES, redirect_uri=REDIRECT_URI
        )
        flow.fetch_token(code=code.strip())
        creds = flow.credentials
        return {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': list(creds.scopes) if creds.scopes else list(SCOPES),
            'expiry': creds.expiry.isoformat() if creds.expiry else None,
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
    ) -> Tuple[int, int, Dict]:
        """Sube conciertos a Google Calendar. Devuelve (éxitos, errores, token_actualizado)."""
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
            return 0, len(concerts), token_data

        from googleapiclient.errors import HttpError
        success, errors = 0, 0
        for concert in concerts:
            try:
                event = _concert_to_gcal_event(concert)
                service.events().insert(calendarId=calendar_id, body=event).execute()
                success += 1
            except HttpError as e:
                logger.error(f"HttpError añadiendo concierto a GCal: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"Error añadiendo concierto a GCal: {e}")
                errors += 1
        return success, errors, updated_token

    def push_releases(
        self, token_data: Dict, releases: List[Dict],
        calendar_id: str = 'primary', muspy_service=None
    ) -> Tuple[int, int, Dict]:
        """Sube lanzamientos a Google Calendar. Devuelve (éxitos, errores, token_actualizado)."""
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
            return 0, len(releases), token_data

        from googleapiclient.errors import HttpError
        success, errors = 0, 0
        for release in releases:
            try:
                event = _release_to_gcal_event(release, muspy_service)
                service.events().insert(calendarId=calendar_id, body=event).execute()
                success += 1
            except HttpError as e:
                logger.error(f"HttpError añadiendo lanzamiento a GCal: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"Error añadiendo lanzamiento a GCal: {e}")
                errors += 1
        return success, errors, updated_token


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

    event: Dict = {
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
    else:
        artist = release.get('artist', '')
        title = release.get('title', '')
        rel_type = ''

    date_str = (release.get('date', '') or '')[:10]
    summary = f"💿 {artist} — {title}"
    if rel_type:
        summary += f" [{rel_type}]"

    return {
        'summary': summary,
        'start': {'date': date_str},
        'end': {'date': date_str},
        'description': f"Lanzamiento: {artist}\nÁlbum: {title}\nFuente: Muspy",
    }
