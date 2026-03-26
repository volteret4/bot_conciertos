import os
import json
import time
import requests
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Tuple

class LastFmService:
    """Servicio para interactuar con la API de Last.fm"""

    def __init__(self, api_key: str, cache_dir: str, cache_duration: int = 24):
        """
        Inicializa el servicio de Last.fm

        Args:
            api_key: Clave de API de Last.fm
            cache_dir: Directorio para caché
            cache_duration: Duración del caché en horas
        """
        self.api_key = api_key
        self.base_url = "http://ws.audioscrobbler.com/2.0/"
        self.cache_dir = Path(cache_dir)
        self.cache_duration = cache_duration
        self.last_error = None

        # Crear directorio de caché si no existe
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.last_error = f"Error creando directorio de caché: {str(e)}"
            print(f"❌ {self.last_error}")

    def setup(self) -> bool:
        """
        Configura y valida el servicio de Last.fm

        Returns:
            True si la configuración es exitosa
        """
        if not self.api_key:
            self.last_error = "API key de Last.fm no configurada"
            print(f"❌ {self.last_error}")
            return False

        # Verificar que la API key funciona
        try:
            response = self._make_request("chart.getTopArtists", {"limit": 1})
            if response:
                print("✅ Last.fm configurado correctamente")
                return True
            else:
                self.last_error = "API key de Last.fm inválida"
                return False
        except Exception as e:
            self.last_error = f"Error validando API key: {str(e)}"
            print(f"❌ {self.last_error}")
            return False

    def check_user_exists(self, username: str) -> bool:
        """
        Verifica si un usuario existe en Last.fm

        Args:
            username: Nombre de usuario de Last.fm

        Returns:
            True si el usuario existe
        """
        try:
            response = self._make_request("user.getInfo", {"user": username})
            return response is not None and "user" in response
        except Exception as e:
            print(f"❌ Error verificando usuario {username}: {e}")
            return False

    def get_user_info(self, username: str) -> Optional[Dict]:
        """
        Obtiene información básica de un usuario

        Args:
            username: Nombre de usuario de Last.fm

        Returns:
            Diccionario con información del usuario o None si no existe
        """
        try:
            response = self._make_request("user.getInfo", {"user": username})
            if response and "user" in response:
                user_info = response["user"]
                return {
                    "name": user_info.get("name", username),
                    "realname": user_info.get("realname", ""),
                    "playcount": int(user_info.get("playcount", 0)),
                    "registered": user_info.get("registered", {}).get("#text", ""),
                    "url": user_info.get("url", "")
                }
        except Exception as e:
            print(f"❌ Error obteniendo info de usuario {username}: {e}")

        return None

    def get_top_artists(self, username: str, period: str = "overall", limit: int = 50) -> Tuple[List[Dict], str]:
        """
        Obtiene los artistas más escuchados de un usuario con información completa incluyendo MBID

        Args:
            username: Nombre de usuario de Last.fm
            period: Período de tiempo (overall, 12month, 6month, 3month, 1month, 7day)
            limit: Número máximo de artistas a obtener

        Returns:
            Tupla con (lista de artistas, mensaje de estado)
        """
        print(f"🎵 Obteniendo top artistas de {username} (período: {period}, límite: {limit})")

        # Verificar caché primero
        cache_file = self._get_cache_file_path(f"top_artists_{username}_{period}_{limit}")
        cached_data = self._load_from_cache(cache_file)

        if cached_data:
            print(f"🔄 Usando caché para top artistas de {username}")
            return cached_data, f"Top artistas de {username} obtenidos (caché)"

        try:
            params = {
                "user": username,
                "period": period,
                "limit": min(limit, 1000)  # Last.fm tiene límite de 1000
            }

            response = self._make_request("user.getTopArtists", params)

            if not response or "topartists" not in response:
                return [], f"No se pudieron obtener artistas para {username}"

            artists_data = response["topartists"].get("artist", [])

            if not artists_data:
                return [], f"No se encontraron artistas para {username}"

            # Normalizar datos (Last.fm devuelve diferentes estructuras según la cantidad)
            if isinstance(artists_data, dict):
                artists_data = [artists_data]

            artists = []
            artists_with_mbid = 0
            artists_enhanced = 0

            for artist_data in artists_data:
                try:
                    artist_name = artist_data.get("name", "")
                    if not artist_name:
                        continue

                    artist = {
                        "name": artist_name,
                        "playcount": int(artist_data.get("playcount", 0)),
                        "mbid": artist_data.get("mbid", ""),
                        "url": artist_data.get("url", ""),
                        "rank": int(artist_data.get("@attr", {}).get("rank", len(artists) + 1)),
                        "lastfm_url": artist_data.get("url", "")
                    }

                    # Si tiene MBID, obtener información adicional
                    if artist["mbid"]:
                        artists_with_mbid += 1
                        enhanced_info = self._get_artist_info_from_lastfm(artist_name, artist["mbid"])
                        if enhanced_info:
                            artist.update(enhanced_info)
                            artists_enhanced += 1
                    else:
                        # Si no tiene MBID, intentar buscarlo
                        mbid = self._search_artist_mbid(artist_name)
                        if mbid:
                            artist["mbid"] = mbid
                            artists_with_mbid += 1
                            enhanced_info = self._get_artist_info_from_lastfm(artist_name, mbid)
                            if enhanced_info:
                                artist.update(enhanced_info)
                                artists_enhanced += 1

                    artists.append(artist)

                except (ValueError, KeyError) as e:
                    print(f"⚠️ Error procesando artista {artist_name}: {e}")
                    continue

            # Ordenar por ranking
            artists.sort(key=lambda x: x["rank"])

            # Guardar en caché
            if artists:
                self._save_to_cache(cache_file, artists)

            print(f"✅ Obtenidos {len(artists)} artistas para {username}")
            print(f"📊 {artists_with_mbid}/{len(artists)} artistas con MBID ({artists_enhanced} con info extra)")

            return artists, f"Se encontraron {len(artists)} artistas ({artists_with_mbid} con MBID)"

        except Exception as e:
            print(f"❌ Error obteniendo top artistas para {username}: {e}")
            return [], f"Error obteniendo artistas: {str(e)}"

    def _search_artist_mbid(self, artist_name: str) -> str:
        """
        Busca el MBID de un artista usando la API de Last.fm

        Args:
            artist_name: Nombre del artista

        Returns:
            MBID del artista o cadena vacía si no se encuentra
        """
        try:
            params = {
                "artist": artist_name,
                "limit": 1
            }

            response = self._make_request("artist.search", params)

            if not response or "results" not in response:
                return ""

            artist_matches = response["results"].get("artistmatches", {}).get("artist", [])

            if not artist_matches:
                return ""

            # Si es una lista, tomar el primer elemento; si es dict, usarlo directamente
            if isinstance(artist_matches, list):
                if len(artist_matches) > 0:
                    first_match = artist_matches[0]
                else:
                    return ""
            else:
                first_match = artist_matches

            mbid = first_match.get("mbid", "")
            if mbid:
                print(f"🔍 MBID encontrado para {artist_name}: {mbid}")

            return mbid

        except Exception as e:
            print(f"⚠️ Error buscando MBID para {artist_name}: {e}")
            return ""

    def _get_artist_info_from_lastfm(self, artist_name: str, mbid: str) -> Optional[Dict]:
        """
        Obtiene información adicional del artista desde Last.fm usando el MBID

        Args:
            artist_name: Nombre del artista
            mbid: MusicBrainz ID del artista

        Returns:
            Diccionario con información adicional o None
        """
        try:
            params = {
                "mbid": mbid
            }

            response = self._make_request("artist.getInfo", params)

            if not response or "artist" not in response:
                return None

            artist_info = response["artist"]

            # Extraer información relevante
            enhanced_info = {}

            # Información básica
            if "bio" in artist_info:
                bio = artist_info["bio"]
                enhanced_info["bio_summary"] = bio.get("summary", "")[:500]  # Limitar tamaño
                enhanced_info["bio_content"] = bio.get("content", "")[:1000]  # Limitar tamaño

            # Tags/géneros
            if "tags" in artist_info:
                tags = artist_info["tags"].get("tag", [])
                if isinstance(tags, list):
                    enhanced_info["genres"] = [tag.get("name", "") for tag in tags[:5]]  # Top 5 géneros
                elif isinstance(tags, dict):
                    enhanced_info["genres"] = [tags.get("name", "")]

            # Estadísticas
            stats = artist_info.get("stats", {})
            enhanced_info["listeners"] = int(stats.get("listeners", 0))
            enhanced_info["total_playcount"] = int(stats.get("playcount", 0))

            # Artistas similares
            if "similar" in artist_info:
                similar = artist_info["similar"].get("artist", [])
                if isinstance(similar, list):
                    enhanced_info["similar_artists"] = [art.get("name", "") for art in similar[:3]]  # Top 3 similares
                elif isinstance(similar, dict):
                    enhanced_info["similar_artists"] = [similar.get("name", "")]

            print(f"📈 Info extra obtenida para {artist_name}: {len(enhanced_info)} campos")
            return enhanced_info

        except Exception as e:
            print(f"⚠️ Error obteniendo info extra para {artist_name}: {e}")
            return None

    def get_period_display_name(self, period: str) -> str:
        """
        Convierte el código de período a nombre legible

        Args:
            period: Código de período de Last.fm

        Returns:
            Nombre legible del período
        """
        period_names = {
            "overall": "De siempre",
            "12month": "Último año",
            "6month": "Últimos 6 meses",
            "3month": "Últimos 3 meses",
            "1month": "Último mes",
            "7day": "Última semana"
        }
        return period_names.get(period, period)

    def format_artists_preview(self, artists: List[Dict], limit: int = 10) -> str:
        """
        Formatea una vista previa de artistas

        Args:
            artists: Lista de artistas
            limit: Número máximo de artistas a mostrar

        Returns:
            String formateado con los artistas
        """
        if not artists:
            return "No se encontraron artistas"

        lines = []
        display_artists = artists[:limit]

        for i, artist in enumerate(display_artists, 1):
            playcount = artist.get("playcount", 0)
            name = artist.get("name", "Nombre desconocido")

            # Escapar caracteres especiales para Markdown
            safe_name = name.replace("*", "\\*").replace("_", "\\_").replace("`", "\\`").replace("[", "\\[")

            line = f"{i}. *{safe_name}*"
            if playcount > 0:
                line += f" ({playcount:,} reproducciones)"

            lines.append(line)

        if len(artists) > limit:
            lines.append(f"_...y {len(artists) - limit} más_")

        return "\n".join(lines)

    def _make_request(self, method: str, params: Dict) -> Optional[Dict]:
        """
        Realiza una petición a la API de Last.fm

        Args:
            method: Método de la API
            params: Parámetros adicionales

        Returns:
            Respuesta de la API o None si hay error
        """
        try:
            # Parámetros base
            api_params = {
                "method": method,
                "api_key": self.api_key,
                "format": "json"
            }

            # Añadir parámetros específicos
            api_params.update(params)

            # Realizar petición
            response = requests.get(self.base_url, params=api_params, timeout=10)
            response.raise_for_status()

            data = response.json()

            # Verificar si hay error en la respuesta
            if "error" in data:
                error_code = data.get("error", 0)
                error_message = data.get("message", "Error desconocido")
                print(f"❌ Error de Last.fm API (código {error_code}): {error_message}")
                return None

            return data

        except requests.exceptions.RequestException as e:
            print(f"❌ Error de conexión con Last.fm: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"❌ Error decodificando respuesta de Last.fm: {e}")
            return None
        except Exception as e:
            print(f"❌ Error inesperado en petición a Last.fm: {e}")
            return None

    def _get_cache_file_path(self, cache_key: str) -> Path:
        """
        Genera ruta al archivo de caché

        Args:
            cache_key: Clave del caché

        Returns:
            Path al archivo de caché
        """
        # Crear un hash para claves muy largas
        if len(cache_key) > 100:
            cache_key = hashlib.md5(cache_key.encode()).hexdigest()

        # Limpiar caracteres no válidos
        safe_key = "".join(x for x in cache_key if x.isalnum() or x in " _-").rstrip()
        safe_key = safe_key.replace(" ", "_").lower()

        return self.cache_dir / f"lastfm_{safe_key}.json"

    def _load_from_cache(self, cache_file: Path) -> Optional[List[Dict]]:
        """
        Carga datos del caché si existen y son válidos

        Args:
            cache_file: Archivo de caché

        Returns:
            Datos del caché o None si no son válidos
        """
        if not cache_file.exists():
            return None

        try:
            # Verificar edad del archivo
            file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            cache_age = datetime.now() - file_time

            if cache_age > timedelta(hours=self.cache_duration):
                return None

            # Cargar datos
            with open(cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # Verificar estructura del caché
            if isinstance(cache_data, dict) and 'timestamp' in cache_data:
                cache_time = datetime.fromisoformat(cache_data['timestamp'])
                if (datetime.now() - cache_time) > timedelta(hours=self.cache_duration):
                    return None
                return cache_data.get('data', [])
            else:
                return cache_data if isinstance(cache_data, list) else None

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            print(f"⚠️ Error leyendo caché {cache_file}: {e}")
            return None

    def _save_to_cache(self, cache_file: Path, data: List[Dict]):
        """
        Guarda datos en caché

        Args:
            cache_file: Archivo de caché
            data: Datos a guardar
        """
        try:
            cache_data = {
                'timestamp': datetime.now().isoformat(),
                'data': data
            }

            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"⚠️ Error guardando caché {cache_file}: {e}")

    def clear_cache(self, pattern: Optional[str] = None):
        """
        Limpia archivos de caché

        Args:
            pattern: Patrón para filtrar archivos (opcional)
        """
        try:
            if pattern:
                files = list(self.cache_dir.glob(f"lastfm_{pattern}*.json"))
            else:
                files = list(self.cache_dir.glob("lastfm_*.json"))

            for file in files:
                file.unlink()

            print(f"🧹 Limpiados {len(files)} archivos de caché de Last.fm")

        except Exception as e:
            print(f"⚠️ Error limpiando caché: {e}")
