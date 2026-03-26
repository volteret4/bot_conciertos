#!/usr/bin/env python3
"""
Servicio de Muspy para el bot de seguimiento de artistas
Maneja todas las interacciones con la API de Muspy
"""

import logging
import requests
import asyncio
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

class MuspyService:
    """Servicio para interactuar con la API de Muspy"""

    def __init__(self):
        self.base_url = "https://muspy.com/api/1"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ArtistTrackerBot/1.0'
        })
        self.executor = ThreadPoolExecutor(max_workers=3)

    def verify_credentials(self, email: str, password: str, userid: str) -> Tuple[bool, str]:
        """
        Verifica las credenciales de Muspy

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy

        Returns:
            Tupla (éxito, mensaje)
        """
        try:
            url = f"{self.base_url}/artists/{userid}"
            auth = (email, password)

            response = self.session.get(url, auth=auth, timeout=10)

            if response.status_code == 200:
                return True, "Credenciales verificadas correctamente"
            elif response.status_code == 401:
                return False, "Credenciales incorrectas"
            elif response.status_code == 404:
                return False, "User ID no encontrado"
            else:
                return False, f"Error del servidor: {response.status_code}"

        except requests.RequestException as e:
            logger.error(f"Error verificando credenciales Muspy: {e}")
            return False, f"Error de conexión: {str(e)}"

    def get_user_artists(self, email: str, password: str, userid: str) -> Tuple[List[Dict], str]:
        """
        Obtiene los artistas seguidos de un usuario en Muspy

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy

        Returns:
            Tupla (lista_artistas, mensaje_estado)
        """
        try:
            url = f"{self.base_url}/artists/{userid}"
            auth = (email, password)

            response = self.session.get(url, auth=auth, timeout=15)

            if response.status_code == 200:
                artists = response.json()
                # Ordenar por nombre para mejor navegación
                artists.sort(key=lambda x: x.get('sort_name', x.get('name', '')).lower())
                return artists, f"Se obtuvieron {len(artists)} artistas"
            elif response.status_code == 401:
                return [], "Credenciales incorrectas"
            elif response.status_code == 404:
                return [], "Usuario no encontrado"
            else:
                return [], f"Error del servidor: {response.status_code}"

        except requests.RequestException as e:
            logger.error(f"Error obteniendo artistas de Muspy: {e}")
            return [], f"Error de conexión: {str(e)}"

    def get_user_releases(self, email: str, password: str, userid: str) -> Tuple[List[Dict], str]:
        """
        Obtiene todos los lanzamientos del usuario desde Muspy

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy

        Returns:
            Tupla (lista_releases, mensaje_estado)
        """
        try:
            url = f"{self.base_url}/releases/{userid}"
            auth = (email, password)

            response = self.session.get(url, auth=auth, timeout=30)

            if response.status_code == 200:
                releases = response.json()
                # Ordenar por fecha
                releases.sort(key=lambda x: x.get('date', '9999-99-99'))
                return releases, f"Se obtuvieron {len(releases)} lanzamientos"
            elif response.status_code == 401:
                return [], "Credenciales incorrectas"
            elif response.status_code == 404:
                return [], "No se encontraron lanzamientos"
            else:
                return [], f"Error del servidor: {response.status_code}"

        except requests.RequestException as e:
            logger.error(f"Error obteniendo releases de Muspy: {e}")
            return [], f"Error de conexión: {str(e)}"

    def add_artist_to_muspy(self, email: str, password: str, userid: str, mbid: str) -> Tuple[bool, str]:
        """
        Añade un artista a Muspy

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy
            mbid: MusicBrainz ID del artista

        Returns:
            Tupla (éxito, mensaje)
        """
        try:
            url = f"{self.base_url}/artists/{userid}"
            auth = (email, password)
            data = {'mbid': mbid}

            response = self.session.put(url, auth=auth, data=data, timeout=10)

            if response.status_code in [200, 201]:
                return True, "Artista añadido correctamente"
            elif response.status_code == 400:
                # El artista ya está seguido o hay un error con el MBID
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', 'Ya seguido o MBID inválido')
                    return True, error_msg  # Considerar como éxito si ya está seguido
                except:
                    return True, "Ya seguido o MBID inválido"
            elif response.status_code == 401:
                return False, "Credenciales incorrectas"
            else:
                return False, f"Error del servidor: {response.status_code}"

        except requests.RequestException as e:
            logger.error(f"Error añadiendo artista a Muspy: {e}")
            return False, f"Error de conexión: {str(e)}"

    async def sync_artists_to_muspy(self, email: str, password: str, userid: str,
                                   artists: List[Dict], progress_callback=None) -> Tuple[int, int, List[str]]:
        """
        Sincroniza múltiples artistas con Muspy de forma asíncrona

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy
            artists: Lista de artistas con MBID
            progress_callback: Función de callback para progreso

        Returns:
            Tupla (añadidos, errores, lista_errores)
        """
        added_count = 0
        error_count = 0
        errors = []

        for i, artist in enumerate(artists, 1):
            if not artist.get('mbid'):
                error_count += 1
                errors.append(f"❌ {artist.get('name', 'Sin nombre')} - Sin MBID")
                continue

            try:
                # Ejecutar en thread pool para no bloquear
                loop = asyncio.get_event_loop()
                success, message = await loop.run_in_executor(
                    self.executor,
                    self.add_artist_to_muspy,
                    email, password, userid, artist['mbid']
                )

                if success:
                    added_count += 1
                else:
                    error_count += 1
                    errors.append(f"❌ {artist.get('name', 'Sin nombre')} - {message}")

                # Callback de progreso
                if progress_callback and (i % 5 == 0 or i == len(artists)):
                    await progress_callback(i, len(artists), added_count, error_count)

                # Pausa para no sobrecargar la API
                await asyncio.sleep(0.5)

            except Exception as e:
                error_count += 1
                errors.append(f"❌ {artist.get('name', 'Sin nombre')} - Error: {str(e)}")
                logger.error(f"Error procesando artista {artist.get('name')}: {e}")

        return added_count, error_count, errors

    def get_artist_releases(self, email: str, password: str, mbid: str) -> Tuple[List[Dict], str]:
        """
        Obtiene lanzamientos de un artista específico

        Args:
            email: Email de Muspy
            password: Contraseña de Muspy
            mbid: MusicBrainz ID del artista

        Returns:
            Tupla (lista_releases, mensaje_estado)
        """
        try:
            url = f"{self.base_url}/releases"
            params = {"mbid": mbid}
            auth = (email, password)

            response = self.session.get(url, auth=auth, params=params, timeout=15)

            if response.status_code == 200:
                releases = response.json()
                # Filtrar solo lanzamientos futuros
                today = date.today().strftime("%Y-%m-%d")
                future_releases = [r for r in releases if r.get('date', '0000-00-00') >= today]

                return future_releases, f"Se encontraron {len(future_releases)} lanzamientos futuros"
            elif response.status_code == 401:
                return [], "Credenciales incorrectas"
            elif response.status_code == 404:
                return [], "No se encontraron lanzamientos"
            else:
                return [], f"Error del servidor: {response.status_code}"

        except requests.RequestException as e:
            logger.error(f"Error obteniendo releases del artista: {e}")
            return [], f"Error de conexión: {str(e)}"

    def format_release_info(self, release: Dict) -> str:
        """
        Formatea la información de un lanzamiento

        Args:
            release: Diccionario con datos del lanzamiento

        Returns:
            String formateado con la información
        """
        title = release.get('title', 'Sin título')
        date_str = release.get('date', 'Fecha desconocida')
        release_type = release.get('type', 'Release').title()

        # Formatear fecha
        try:
            if date_str != 'Fecha desconocida':
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                formatted_date = date_obj.strftime("%d/%m/%Y")
            else:
                formatted_date = date_str
        except:
            formatted_date = date_str

        info = f"*{title}*\n"
        info += f"📅 {formatted_date} • 💿 {release_type}"

        # Añadir información adicional si está disponible
        if release.get('format'):
            info += f"\n🎧 Formato: {release['format']}"

        if release.get('country'):
            info += f"\n🌍 País: {release['country']}"

        return info

    def extract_artist_name(self, release: Dict) -> str:
        """Extrae el nombre del artista desde diferentes posibles campos"""
        # Opción 1: artist_credit (común en MusicBrainz)
        if 'artist_credit' in release and isinstance(release['artist_credit'], list) and len(release['artist_credit']) > 0:
            artist = release['artist_credit'][0]
            if isinstance(artist, dict):
                return artist.get('name', artist.get('artist', {}).get('name', 'Artista desconocido'))
            elif isinstance(artist, str):
                return artist

        # Opción 2: artist_name directo
        if 'artist_name' in release and release['artist_name']:
            return release['artist_name']

        # Opción 3: artist como objeto
        if 'artist' in release:
            artist = release['artist']
            if isinstance(artist, dict):
                return artist.get('name', 'Artista desconocido')
            elif isinstance(artist, str):
                return artist

        # Campos alternativos
        for field in ['performer', 'creator', 'artist_display_name']:
            if field in release and release[field]:
                return release[field]

        return 'Artista desconocido'

    def extract_title(self, release: Dict) -> str:
        """Extrae el título del lanzamiento"""
        for field in ['title', 'name', 'album', 'release_name']:
            if field in release and release[field]:
                return release[field]
        return 'Sin título'

    def extract_release_type(self, release: Dict) -> str:
        """Extrae el tipo de lanzamiento"""
        for field in ['type', 'release_type', 'primary_type']:
            if field in release and release[field]:
                return release[field].title()

        # Si hay información de grupo de release
        if 'release_group' in release:
            rg = release['release_group']
            if isinstance(rg, dict):
                for field in ['type', 'primary_type']:
                    if field in rg and rg[field]:
                        return rg[field].title()

        return 'Release'

    def __del__(self):
        """Cleanup al destruir el objeto"""
        if hasattr(self, 'executor'):
            self.executor.shutdown(wait=False)
