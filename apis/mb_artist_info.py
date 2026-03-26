#!/usr/bin/env python3
import sqlite3
import requests
import json
import os
import time
import datetime
import sys
from pathlib import Path
import musicbrainzngs
import re
import logging
from typing import Dict, List, Tuple, Optional, Any, Union

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('mb_artist_info')

# Variables globales que serán configuradas por db_creator.py
INTERACTIVE_MODE = False
FORCE_UPDATE = False
CONFIG = {}

# Cache global para MusicBrainz
mb_cache = None

class APICache:
    """
    Cache para consultas a la API de MusicBrainz
    """
    
    def __init__(self, name="generic", cache_file=None, cache_duration=30):
        """
        Inicializa el cache.
        
        Args:
            name: Nombre del cache (para logs)
            cache_file: Ruta del archivo para persistir el cache
            cache_duration: Duración del cache en días
        """
        self.name = name
        self.cache = {}
        self.cache_file = cache_file
        self.cache_duration = cache_duration  # en días
        
        if cache_file and os.path.exists(cache_file):
            try:
                # Leer el archivo con manejo robusto de errores
                with open(cache_file, 'r', encoding='utf-8') as f:
                    try:
                        loaded_cache = json.load(f)
                        logger.info(f"Cargando cache desde archivo: {cache_file}")
                        
                        # Verificar que el cache cargado es un diccionario
                        if not isinstance(loaded_cache, dict):
                            logger.warning(f"El archivo de cache contiene tipo de datos inválido: {type(loaded_cache)}. Usando cache vacío.")
                            loaded_cache = {}
                            
                    except json.JSONDecodeError as je:
                        logger.error(f"Error al decodificar archivo de cache ({cache_file}): {je}")
                        logger.info("Intentando recuperar datos parciales...")
                        
                        # Intentar recuperar datos parciales
                        f.seek(0)  # Volver al inicio del archivo
                        loaded_cache = self._recover_partial_json(f)
                        
                        # Si no pudimos recuperar nada, comenzar con cache vacío
                        if not loaded_cache:
                            logger.warning("No se pudo recuperar ningún dato. Usando cache vacío.")
                            loaded_cache = {}
                        else:
                            logger.info(f"Recuperadas {len(loaded_cache)} entradas parciales de cache.")
                    
                    # Filtrar entradas expiradas
                    now = time.time()
                    valid_entries = 0
                    
                    # Crear un nuevo cache para evitar modificar durante la iteración
                    filtered_cache = {}
                    
                    # Procesar cada par clave-valor
                    for key, entry in loaded_cache.items():
                        # Omitir entradas inválidas
                        if not isinstance(entry, dict):
                            logger.warning(f"Omitiendo entrada de cache inválida para clave {key}: {type(entry)}")
                            continue
                            
                        # Verificar expiración
                        if 'timestamp' in entry:
                            age_days = (now - entry['timestamp']) / (60 * 60 * 24)
                            if age_days <= self.cache_duration:
                                filtered_cache[key] = entry
                                valid_entries += 1
                        else:
                            # Si no hay timestamp, asumir que es reciente
                            filtered_cache[key] = entry
                            valid_entries += 1
                    
                    # Asignar el cache filtrado            
                    self.cache = filtered_cache
                    logger.info(f"{self.name}Cache: Cargadas {valid_entries} entradas válidas de {len(loaded_cache)} totales")
            except Exception as e:
                logger.error(f"Error al cargar archivo de cache para {self.name}: {e}")
                logger.warning("Usando cache vacío")
                self.cache = {}
    
    def get(self, key_parts):
        """
        Obtiene un resultado del cache si está disponible y no expirado.
        
        Args:
            key_parts: Elementos para formar la clave del cache (lista, tupla o diccionario)
            
        Returns:
            Datos en cache o None si no existe o expiró
        """
        try:
            cache_key = self._make_key(key_parts)
            entry = self.cache.get(cache_key)
            
            if not entry:
                return None
                
            # Verificar expiración
            if 'timestamp' in entry:
                age_days = (time.time() - entry['timestamp']) / (60 * 60 * 24)
                if age_days > self.cache_duration:
                    # Expirado, eliminar y devolver None
                    del self.cache[cache_key]
                    return None
            
            return entry.get('data')
        except Exception as e:
            # Si algo sale mal, solo devolver None y registrar el error
            logger.error(f"Error al recuperar del cache: {e}")
            return None
    
    def put(self, key_parts, result):
        """
        Almacena un resultado en el cache.
        
        Args:
            key_parts: Elementos para formar la clave del cache
            result: Resultado a almacenar
        """
        try:
            # No almacenar resultados None o respuestas de error
            if result is None or (isinstance(result, dict) and 'error' in result):
                return
                
            cache_key = self._make_key(key_parts)
            
            # Almacenar con timestamp para expiración
            self.cache[cache_key] = {
                'data': result,
                'timestamp': time.time()
            }
            
            # Guardar en archivo si está configurado
            self._save_cache()
        except Exception as e:
            # Si el almacenamiento en cache falla, solo registrar el error y continuar
            logger.error(f"Error al almacenar en cache: {e}")
    
    def clear(self, save=True):
        """Limpia todo el cache"""
        self.cache = {}
        if save and self.cache_file:
            self._save_cache()
    
    def _save_cache(self):
        """Guarda el cache en disco si un archivo está configurado"""
        if not self.cache_file:
            return
            
        try:
            # Crear directorio si no existe
            cache_dir = os.path.dirname(self.cache_file)
            if cache_dir and not os.path.exists(cache_dir):
                os.makedirs(cache_dir)
                
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Error al guardar archivo de cache para {self.name}: {e}")
    
    def _make_key(self, key_parts):
        """
        Crea una clave única para el cache a partir de los elementos proporcionados.
        
        Args:
            key_parts: Elementos para formar la clave (lista, tupla, diccionario, etc.)
            
        Returns:
            String único que identifica la consulta
        """
        try:
            # Si es un diccionario, normalizar y ordenar
            if isinstance(key_parts, dict):
                # Filtrar claves irrelevantes como api_keys
                filtered = {k.lower(): v for k, v in key_parts.items() 
                        if k.lower() not in ('api_key', 'format')}
                key_str = json.dumps(filtered, sort_keys=True)
            elif isinstance(key_parts, (list, tuple)):
                # Convertir elementos a strings y unir
                key_str = ":".join(str(k).lower() for k in key_parts if k)
            else:
                key_str = str(key_parts).lower()
            
            # Usar hash para claves muy largas
            if len(key_str) > 200:
                import hashlib
                key_hash = hashlib.md5(key_str.encode('utf-8')).hexdigest()
                return key_hash
            
            return key_str
        except Exception as e:
            # Si la generación de la clave falla, usar un mecanismo de fallback
            logger.error(f"Error al crear clave de cache: {e}")
            import hashlib
            # Crear un hash simple de la representación string
            return hashlib.md5(str(key_parts).encode('utf-8')).hexdigest()

    def _recover_partial_json(self, file_obj):
        """
        Intenta recuperar datos JSON parciales de un archivo corrupto.
        
        Args:
            file_obj: Objeto de archivo abierto
            
        Returns:
            Diccionario con datos que pudieron ser recuperados
        """
        try:
            # Leer todo el contenido
            content = file_obj.read()
            
            # Buscar pares clave-valor JSON completos
            import re
            recovered_data = {}
            
            # Patrón para encontrar pares "key": value
            pattern = r'"([^"]+)"\s*:\s*(\{[^{}]*\}|\[[^\[\]]*\]|"[^"]*"|null|true|false|\d+(?:\.\d+)?)'
            matches = re.findall(pattern, content)
            
            for key, value in matches:
                try:
                    # Intentar parsearlo como JSON válido
                    parsed_value = json.loads(value)
                    recovered_data[key] = parsed_value
                except json.JSONDecodeError:
                    # Si falla, omitir este par
                    continue
            
            return recovered_data
        except Exception as e:
            logger.error(f"Error al intentar recuperar datos parciales: {e}")
            return {}

def setup_cache(cache_directory=None):
    """Configura el sistema de cache para MusicBrainz"""
    global mb_cache
    
    # Inicializar cache (en memoria por defecto)
    if mb_cache is None:
        try:
            mb_cache = APICache(name="MusicBrainz", cache_duration=30)  # Mayor duración para MusicBrainz
        except Exception as e:
            logger.error(f"Error inicializando cache MusicBrainz: {e}")
            mb_cache = APICache(name="MusicBrainz")  # Fallback con configuración por defecto
    
    # Si se proporciona un directorio de cache, configurar persistencia
    if cache_directory:
        try:
            os.makedirs(cache_directory, exist_ok=True)
            
            mb_cache_file = os.path.join(cache_directory, "musicbrainz_cache.json")
            
            logger.info(f"Configurando cache en: {cache_directory}")
            logger.info(f"Archivo de cache MusicBrainz: {mb_cache_file}")
            
            # Crear nuevas instancias con archivos de cache
            try:
                mb_cache = APICache(name="MusicBrainz", cache_file=mb_cache_file, cache_duration=30)
                
                logger.info(f"Cache configurada en: {cache_directory}")
                logger.info(f"Entradas en cache MusicBrainz: {len(mb_cache.cache)}")
            except Exception as e:
                logger.error(f"Error configurando cache con archivos: {e}")
                logger.warning("Usando cache en memoria en su lugar")
                
                # Si falla la carga desde archivos, reinicializar con cache en memoria
                mb_cache = APICache(name="MusicBrainz", cache_duration=30)
        except Exception as e:
            logger.error(f"Error configurando cache persistente: {e}")
            logger.warning("Usando cache en memoria")
    else:
        logger.info("Cache configurada en memoria (no persistente)")
    
    # Validar objetos de cache
    if not isinstance(mb_cache.cache, dict):
        logger.warning(f"Cache MusicBrainz es inválido. Reinicializando.")
        mb_cache.cache = {}

def get_artists_for_update(conn, limit=50, force_update=False, only_networks=False):
    """
    Obtiene lista de artistas que necesitan actualización
    
    Args:
        conn: Conexión a la base de datos
        limit: Número máximo de artistas a procesar
        force_update: Si se debe forzar la actualización de todos los artistas
        only_networks: Si solo se deben obtener artistas para actualizar networks
        
    Returns:
        Lista de tuplas (id, name, mbid) de artistas a actualizar
    """
    cursor = conn.cursor()
    
    query_conditions = ["mbid IS NOT NULL"]  # Debe tener un MBID
    
    if only_networks:
        # Obtener artistas para actualizar solo networks
        query_conditions.append("""
            NOT EXISTS (
                SELECT 1 FROM artists_networks 
                WHERE artists_networks.artist_id = artists.id 
                AND artists_networks.musicbrainz IS NOT NULL
            )
        """)
    elif not force_update:
        # Condiciones para actualizar solo los artistas que lo necesitan
        query_conditions.extend([
            "(formed_year IS NULL OR total_albums IS NULL)"
        ])
    
    # Construir la consulta
    query = """
        SELECT id, name, mbid 
        FROM artists
        WHERE {}
        ORDER BY id DESC
        LIMIT ?
    """.format(" AND ".join(query_conditions))
    
    try:
        cursor.execute(query, (limit,))
        artists = cursor.fetchall()
        logger.info(f"Encontrados {len(artists)} artistas para actualizar")
        return artists
    except sqlite3.Error as e:
        logger.error(f"Error al obtener artistas para actualizar: {e}")
        return []


def setup_musicbrainz(user_agent=None, cache_directory=None):
    """
    Configura el cliente de MusicBrainz y el sistema de caché
    
    Args:
        user_agent: Diccionario con la información del agente de usuario
        cache_directory: Directorio para almacenar la caché
    """
    # Configurar cliente de MusicBrainz con valores por defecto si no se proporcionan
    if user_agent is None:
        user_agent = {
            "app": "MusicBrainzUpdater",
            "version": "1.0",
            "contact": "dev@example.com"
        }
        
    # Asegurarse de que user_agent tenga todos los campos necesarios
    if isinstance(user_agent, dict):
        app = user_agent.get("app", "MusicBrainzUpdater")
        version = user_agent.get("version", "1.0")
        contact = user_agent.get("contact", "dev@example.com")
        musicbrainzngs.set_useragent(app, version, contact)
    else:
        # Si user_agent no es un diccionario, usar valores por defecto
        musicbrainzngs.set_useragent("MusicBrainzUpdater", "1.0", "dev@example.com")
        
    # Configurar tiempo de espera y reintentos
    musicbrainzngs.set_rate_limit(limit_or_interval=1.0, new_requests=1)
    
    # Silenciar advertencias sobre atributos no capturados
    # Modificamos el nivel de log para el módulo musicbrainzngs
    musicbrainz_logger = logging.getLogger('musicbrainzngs')
    musicbrainz_logger.setLevel(logging.WARNING)  # Solo mostrar advertencias y errores, no INFO
    
    # Inicializar caché
    setup_cache(cache_directory)

def get_artist_from_musicbrainz(mbid: str) -> Optional[Dict]:
    """
    Obtiene información extendida de un artista desde MusicBrainz usando su MBID, usando cache
    
    Args:
        mbid: MusicBrainz ID del artista
        
    Returns:
        Diccionario con información del artista o None si no se encuentra
    """
    global mb_cache
    
    if not mbid:
        return None
    
    # Verificar en cache primero
    if mb_cache:
        cached_result = mb_cache.get({"type": "artist", "id": mbid})
        if cached_result:
            logger.info(f"Usando datos en cache para artista con MBID {mbid}")
            return cached_result
    
    try:
        logger.info(f"Consultando MusicBrainz para artista con MBID {mbid}")
        # Incluir más información relevante
        artist_data = musicbrainzngs.get_artist_by_id(
            mbid, 
            includes=["tags", "url-rels", "aliases", "annotation", "releases", "release-groups"]
        )
        result = artist_data.get("artist")
        
        # Guardar en cache
        if mb_cache and result:
            mb_cache.put({"type": "artist", "id": mbid}, result)
            
        return result
    except musicbrainzngs.WebServiceError as e:
        logger.error(f"Error al consultar MusicBrainz para artista con MBID {mbid}: {e}")
        return None

def search_artist_in_musicbrainz(artist_name: str) -> List[Dict]:
    """
    Busca un artista en MusicBrainz por nombre
    
    Args:
        artist_name: Nombre del artista a buscar
        
    Returns:
        Lista de artistas encontrados o lista vacía si no hay resultados
    """
    global mb_cache
    
    if not artist_name:
        return []
    
    # Verificar en cache primero
    if mb_cache:
        cached_result = mb_cache.get({"type": "artist-search", "query": artist_name})
        if cached_result:
            logger.info(f"Usando datos en cache para búsqueda de artista '{artist_name}'")
            return cached_result
    
    try:
        logger.info(f"Buscando artista en MusicBrainz: {artist_name}")
        result = musicbrainzngs.search_artists(artist=artist_name, limit=5)
        
        if result and 'artist-list' in result:
            # Guardar en cache
            if mb_cache:
                mb_cache.put({"type": "artist-search", "query": artist_name}, result['artist-list'])
            return result['artist-list']
        return []
    except musicbrainzngs.WebServiceError as e:
        logger.error(f"Error al buscar artista en MusicBrainz: {e}")
        return []

def get_artists_for_update(conn: sqlite3.Connection, limit: int = 50, force_update: bool = False) -> List[Tuple]:
    """
    Obtiene lista de artistas que necesitan actualización desde MusicBrainz
    
    Args:
        conn: Conexión a la base de datos
        limit: Número máximo de artistas a procesar
        force_update: Si se debe forzar la actualización de todos los artistas
        
    Returns:
        Lista de tuplas (id, name, mbid) de artistas a actualizar
    """
    cursor = conn.cursor()
    
    query_conditions = []
    
    if not force_update:
        # Condiciones para actualizar solo los artistas que lo necesitan
        query_conditions.extend([
            "mbid IS NOT NULL",  # Debe tener un MBID
            "(formed_year IS NULL OR total_albums IS NULL OR origin IS NULL )"
        ])
    else:
        # Con force_update solo requerimos que tengan un MBID
        query_conditions.append("mbid IS NOT NULL")
    
    # Construir la consulta
    query = """
        SELECT id, name, mbid 
        FROM artists
        WHERE {}
        ORDER BY id DESC
        LIMIT ?
    """.format(" AND ".join(query_conditions) if query_conditions else "1=1")
    
    try:
        cursor.execute(query, (limit,))
        artists = cursor.fetchall()
        logger.info(f"Encontrados {len(artists)} artistas para actualizar")
        return artists
    except sqlite3.Error as e:
        logger.error(f"Error al obtener artistas para actualizar: {e}")
        return []

def count_release_groups(artist_data: Dict) -> int:
    """
    Cuenta el número de grupos de lanzamientos (álbumes únicos) de un artista
    
    Args:
        artist_data: Datos del artista de MusicBrainz
        
    Returns:
        Número total de grupos de lanzamientos o 0 si no hay datos
    """
    if 'release-group-count' in artist_data:
        try:
            return int(artist_data['release-group-count'])
        except (ValueError, TypeError):
            pass
    
    # Alternativamente, contar directamente si están incluidos
    if 'release-group-list' in artist_data:
        return len(artist_data['release-group-list'])
    
    return 0

def extract_musicbrainz_url(artist_data: Dict) -> Optional[str]:
    """
    Extrae la URL de MusicBrainz del artista
    
    Args:
        artist_data: Datos del artista de MusicBrainz
        
    Returns:
        URL de MusicBrainz o None si no se encuentra
    """
    # URL oficial basada en el MBID
    if 'id' in artist_data:
        return f"https://musicbrainz.org/artist/{artist_data['id']}"
    
    # Buscar en las relaciones de URL
    if 'url-relation-list' in artist_data:
        for url_rel in artist_data['url-relation-list']:
            if url_rel.get('type') == 'musicbrainz' and 'target' in url_rel:
                return url_rel['target']
    
    return None

def update_artist_networks(conn: sqlite3.Connection, artist_id: int, musicbrainz_url: str) -> bool:
    """
    Actualiza o crea un registro en artists_networks con la URL de MusicBrainz
    
    Args:
        conn: Conexión a la base de datos
        artist_id: ID del artista
        musicbrainz_url: URL de MusicBrainz para el artista
        
    Returns:
        True si la operación fue exitosa, False en caso contrario
    """
    if not artist_id or not musicbrainz_url:
        return False
    
    cursor = conn.cursor()
    
    try:
        # Verificar si ya existe un registro para este artista
        cursor.execute("SELECT id FROM artists_networks WHERE artist_id = ?", (artist_id,))
        result = cursor.fetchone()
        
        if result:
            # Actualizar registro existente
            cursor.execute("""
                UPDATE artists_networks 
                SET musicbrainz = ?, last_updated = CURRENT_TIMESTAMP
                WHERE artist_id = ?
            """, (musicbrainz_url, artist_id))
        else:
            # Crear nuevo registro
            cursor.execute("""
                INSERT INTO artists_networks (artist_id, musicbrainz, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (artist_id, musicbrainz_url))
        
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Error al actualizar artists_networks para artista {artist_id}: {e}")
        return False

def update_artist_with_musicbrainz(conn, artist_id, artist_name, artist_mbid):
    """
    Actualiza un artista con datos de MusicBrainz
    
    Args:
        conn: Conexión a la base de datos
        artist_id: ID del artista en la base de datos
        artist_name: Nombre del artista
        artist_mbid: MBID del artista
        
    Returns:
        True si la operación fue exitosa, False en caso contrario
    """
    # Obtener datos
    artist_data = get_artist_from_musicbrainz(artist_mbid)
    if not artist_data:
        logger.warning(f"No se encontraron datos para {artist_name}")
        return False
        
    # Extraer datos relevantes
    formed_year = extract_artist_year_formed(artist_data)
    ended_year = extract_artist_year_ended(artist_data)
    total_albums = count_release_groups(artist_data)
    origin = extract_country(artist_data)  # Esta es la columna "origin", no "origen"
    musicbrainz_url = f"https://musicbrainz.org/artist/{artist_mbid}"
    
    # Verificar si la columna ended_year existe, si no, crearla
    cursor = conn.cursor()
    try:
        # Verificar si la columna 'ended_year' existe en artists
        cursor.execute("PRAGMA table_info(artists)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'ended_year' not in columns:
            cursor.execute("ALTER TABLE artists ADD COLUMN ended_year INTEGER")
            logger.info("Columna ended_year añadida a artists")
    
        # Actualizar artists con todos los datos
        # Preparar los campos y valores a actualizar
        update_fields = []
        params = []
        
        if formed_year is not None:
            update_fields.append("formed_year = ?")
            params.append(formed_year)
            
        if ended_year is not None:
            update_fields.append("ended_year = ?")
            params.append(ended_year)
            
        if origin is not None:
            update_fields.append("origin = ?")  # Esta es la columna "origin", no "origen"
            params.append(origin)
            
        if total_albums is not None:
            update_fields.append("total_albums = ?")
            params.append(total_albums)
            
        # Construir y ejecutar la consulta
        if update_fields:
            query = f"UPDATE artists SET {', '.join(update_fields)} WHERE id = ?"
            params.append(artist_id)
            cursor.execute(query, params)
        
        # Actualizar artists_networks
        # Primero verificar que la tabla existe y tiene la estructura correcta
        update_artists_networks_table(conn)
        
        # Luego actualizar la URL de MusicBrainz
        update_artists_networks(conn, artist_id, musicbrainz_url)
        
        conn.commit()
        logger.info(f"Actualizado {artist_name} (ID: {artist_id}) - País: {origin}, Formado: {formed_year}, Finalizado: {ended_year}")
        return True
        
    except Exception as e:
        logger.error(f"Error al actualizar {artist_name}: {e}")
        conn.rollback()
        return False



def update_artists_networks_table(conn):
    """
    Verifica y actualiza la estructura de la tabla artists_networks
    
    Args:
        conn: Conexión a la base de datos
    """
    cursor = conn.cursor()
    
    # Verificar si la tabla existe
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='artists_networks'")
    if not cursor.fetchone():
        # Crear la tabla con la estructura completa
        cursor.execute("""
            CREATE TABLE artists_networks (
                id INTEGER PRIMARY KEY,
                artist_id INTEGER NOT NULL,
                allmusic TEXT,
                bandcamp TEXT,
                boomkat TEXT,
                facebook TEXT,
                twitter TEXT,
                mastodon TEXT,
                bluesky TEXT,
                instagram TEXT,
                spotify TEXT,
                lastfm TEXT,
                wikipedia TEXT,
                juno TEXT,
                soundcloud TEXT,
                youtube TEXT,
                imdb TEXT,
                progarchives TEXT,
                setlist_fm TEXT,
                who_sampled TEXT,
                vimeo TEXT,
                genius TEXT,
                myspace TEXT,
                tumblr TEXT,
                resident_advisor TEXT,
                rateyourmusic TEXT,
                enlaces TEXT,
                last_updated TIMESTAMP,
                musicbrainz TEXT
            )
        """)
        logger.info("Tabla artists_networks creada")
        return
    
    # Verificar si existe la columna musicbrainz
    cursor.execute("PRAGMA table_info(artists_networks)")
    columns = [col[1] for col in cursor.fetchall()]
    
    # Si no existe la columna musicbrainz, simplemente la agregamos
    if 'musicbrainz' not in columns:
        cursor.execute("ALTER TABLE artists_networks ADD COLUMN musicbrainz TEXT")
        logger.info("Columna musicbrainz añadida a artists_networks")


def update_artists_networks(conn, artist_id, musicbrainz_url):
    """
    Actualiza o crea un registro en artists_networks con la URL de MusicBrainz
    
    Args:
        conn: Conexión a la base de datos
        artist_id: ID del artista
        musicbrainz_url: URL de MusicBrainz
    """
    if not artist_id or not musicbrainz_url:
        return
    
    cursor = conn.cursor()
    
    try:
        # Verificar si ya existe un registro
        cursor.execute("SELECT id FROM artists_networks WHERE artist_id = ?", (artist_id,))
        result = cursor.fetchone()
        
        if result:
            # Actualizar registro existente
            cursor.execute("""
                UPDATE artists_networks 
                SET musicbrainz = ?, last_updated = CURRENT_TIMESTAMP
                WHERE artist_id = ?
            """, (musicbrainz_url, artist_id))
            logger.debug(f"Actualizada URL de MusicBrainz para artista ID {artist_id}")
        else:
            # Crear nuevo registro
            cursor.execute("""
                INSERT INTO artists_networks (artist_id, musicbrainz, last_updated)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (artist_id, musicbrainz_url))
            logger.debug(f"Creado nuevo registro en networks para artista ID {artist_id}")
    except sqlite3.Error as e:
        logger.error(f"Error al actualizar artists_networks: {e}")

def update_batch_artists(conn, limit=50):
    """
    Procesa un lote de artistas
    
    Args:
        conn: Conexión a la base de datos
        limit: Número máximo de artistas a procesar
        
    Returns:
        Tupla (total_procesados, total_actualizados)
    """
    # Obtener artistas para actualizar
    artists = get_artists_for_update(conn, limit, FORCE_UPDATE)
    
    logger.info(f"Encontrados {len(artists)} artistas para actualizar")
    
    updated = 0
    for artist in artists:
        artist_id, artist_name, artist_mbid = artist[0], artist[1], artist[2]
        
        if update_artist_with_musicbrainz(conn, artist_id, artist_name, artist_mbid):
            updated += 1
            
        # Pausa para no sobrecargar la API
        time.sleep(1)
        
    return len(artists), updated

def create_indices(conn):
    """
    Crea índices optimizados para consultas relacionadas con MusicBrainz y otras operaciones comunes
    
    Args:
        conn: Conexión a la base de datos
        
    Returns:
        Número de índices creados
    """
    cursor = conn.cursor()
    indices_created = 0
    
    # Índices para consultas comunes
    indices = [
        # Índices para la tabla artists
        "CREATE INDEX IF NOT EXISTS idx_artists_mbid ON artists(mbid)",
        "CREATE INDEX IF NOT EXISTS idx_artists_formed_year ON artists(formed_year)",
        "CREATE INDEX IF NOT EXISTS idx_artists_total_albums ON artists(total_albums)",
        "CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name)",
        
        # Índices para artists_networks
        "CREATE INDEX IF NOT EXISTS idx_artists_networks_artist_id ON artists_networks(artist_id)",
        "CREATE INDEX IF NOT EXISTS idx_artists_networks_musicbrainz ON artists_networks(musicbrainz)",
        
        # Índices para consultas comunes sobre álbumes
        "CREATE INDEX IF NOT EXISTS idx_albums_artist_id ON albums(artist_id)",
        "CREATE INDEX IF NOT EXISTS idx_albums_name ON albums(name)",
        "CREATE INDEX IF NOT EXISTS idx_albums_year ON albums(year)",
        "CREATE INDEX IF NOT EXISTS idx_albums_mbid ON albums(mbid)",
        
        # Índices para consultas sobre canciones
        "CREATE INDEX IF NOT EXISTS idx_songs_artist ON songs(artist)",
        "CREATE INDEX IF NOT EXISTS idx_songs_album ON songs(album)",
        "CREATE INDEX IF NOT EXISTS idx_songs_mbid ON songs(mbid)",
        
        # Índices para búsqueda de letras
        "CREATE INDEX IF NOT EXISTS idx_lyrics_track_id ON lyrics(track_id)",
        
        # Índices para consultas sobre scrobbles
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_name ON scrobbles(artist_name)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_scrobble_date ON scrobbles(scrobble_date)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_id ON scrobbles(artist_id)",
        
        # Índices compuestos para consultas específicas frecuentes
        "CREATE INDEX IF NOT EXISTS idx_songs_artist_album ON songs(artist, album)",
        "CREATE INDEX IF NOT EXISTS idx_scrobbles_artist_date ON scrobbles(artist_name, scrobble_date)",
        
        # Índices para las relaciones entre etiquetas
        "CREATE INDEX IF NOT EXISTS idx_label_relationships_source ON label_relationships(source_label_id)",
        "CREATE INDEX IF NOT EXISTS idx_label_relationships_target ON label_relationships(target_label_id)",
        
        # Índices para grupos de lanzamiento de MusicBrainz
        "CREATE INDEX IF NOT EXISTS idx_mb_release_group_mbid ON mb_release_group(mbid)",
        "CREATE INDEX IF NOT EXISTS idx_mb_release_group_album_id ON mb_release_group(album_id)",
        "CREATE INDEX IF NOT EXISTS idx_mb_release_group_artist_id ON mb_release_group(artist_id)"
    ]
    
    for index_sql in indices:
        try:
            cursor.execute(index_sql)
            indices_created += 1
            logger.info(f"Índice creado: {index_sql}")
        except sqlite3.OperationalError as e:
            logger.warning(f"No se pudo crear índice {index_sql}: {e}")
    
    # Analizar la base de datos para optimizar las consultas
    try:
        cursor.execute("ANALYZE")
        logger.info("Base de datos analizada para optimizar consultas")
    except sqlite3.OperationalError:
        logger.warning("No se pudo ejecutar ANALYZE en la base de datos")
    
    conn.commit()
    return indices_created

def find_artist_mbid(conn: sqlite3.Connection, artist_name: str) -> Optional[str]:
    """
    Busca el MBID de un artista en MusicBrainz si no existe en la base de datos
    
    Args:
        conn: Conexión a la base de datos
        artist_name: Nombre del artista
        
    Returns:
        MBID encontrado o None si no se encuentra
    """
    cursor = conn.cursor()
    
    # Primero verificar si ya existe en la base de datos
    try:
        cursor.execute("SELECT mbid FROM artists WHERE name = ? AND mbid IS NOT NULL", (artist_name,))
        result = cursor.fetchone()
        if result and result[0]:
            return result[0]
    except sqlite3.Error as e:
        logger.error(f"Error al buscar MBID en base de datos: {e}")
    
    # Si no existe, buscar en MusicBrainz
    search_results = search_artist_in_musicbrainz(artist_name)
    
    if search_results:
        # Tomar el primer resultado con mayor puntuación
        return search_results[0].get('id')
    
    return None

def update_missing_mbids(conn: sqlite3.Connection, limit: int = 50) -> Tuple[int, int]:
    """
    Actualiza MBIDs faltantes para artistas en la base de datos
    
    Args:
        conn: Conexión a la base de datos
        limit: Límite de artistas a procesar
        
    Returns:
        Tupla (total_procesados, total_actualizados)
    """
    cursor = conn.cursor()
    
    # Obtener artistas sin MBID
    try:
        cursor.execute("""
            SELECT id, name FROM artists
            WHERE mbid IS NULL
            LIMIT ?
        """, (limit,))
        
        artists = cursor.fetchall()
        logger.info(f"Encontrados {len(artists)} artistas sin MBID")
        
        total_processed = 0
        total_updated = 0
        
        for artist_id, artist_name in artists:
            total_processed += 1
            logger.info(f"Buscando MBID para {artist_name} ({total_processed}/{len(artists)})")
            
            mbid = find_artist_mbid(conn, artist_name)
            
            if mbid:
                try:
                    cursor.execute("UPDATE artists SET mbid = ? WHERE id = ?", (mbid, artist_id))
                    conn.commit()
                    total_updated += 1
                    logger.info(f"Actualizado MBID para {artist_name}: {mbid}")
                except sqlite3.Error as e:
                    logger.error(f"Error al actualizar MBID para {artist_name}: {e}")
            else:
                logger.warning(f"No se encontró MBID para {artist_name}")
            
            # Pausa para no sobrecargar la API
            time.sleep(0.5)
        
        return total_processed, total_updated
    
    except sqlite3.Error as e:
        logger.error(f"Error al obtener artistas sin MBID: {e}")
        return 0, 0

def extract_country(artist_data):
    """Extrae el país de origen del artista"""
    if 'country' in artist_data:
        return artist_data['country']
    return None

def extract_artist_year_formed(artist_data):
    """Extrae el año de formación del artista"""
    if 'life-span' in artist_data and 'begin' in artist_data['life-span']:
        begin_date = artist_data['life-span']['begin']
        if begin_date and len(begin_date) >= 4:
            try:
                return int(begin_date[:4])
            except ValueError:
                pass
    return None

def extract_artist_year_ended(artist_data):
    """Extrae el año de disolución del artista"""
    if 'life-span' in artist_data and 'end' in artist_data['life-span']:
        end_date = artist_data['life-span']['end']
        if end_date and len(end_date) >= 4:
            try:
                return int(end_date[:4])
            except ValueError:
                pass
    return None



def main(config=None):
    """
    Función principal para actualizar la información de artistas desde MusicBrainz
    
    Args:
        config: Diccionario con la configuración
        
    Returns:
        Diccionario con los resultados o False en caso de error
    """
    global FORCE_UPDATE, INTERACTIVE_MODE, CONFIG
    
    # Usar configuración proporcionada
    if config:
        CONFIG = config
        FORCE_UPDATE = config.get('force_update', False)
        INTERACTIVE_MODE = config.get('interactive', False)
        
        # Asegurarnos de que se ejecuta algo
        logger.info(f"Iniciando actualización de artistas con MusicBrainz")
        logger.info(f"FORCE_UPDATE: {FORCE_UPDATE}, INTERACTIVE: {INTERACTIVE_MODE}")
        
        # Conectar a la base de datos
        db_path = config.get('db_path')
        if not db_path:
            logger.error("Error: No se proporcionó ruta a la base de datos")
            return False
            
        # Configurar MusicBrainz
        setup_musicbrainz(
            user_agent=config.get('user_agent'), 
            cache_directory=config.get('cache_directory')
        )
        
        # Conectar a base de datos y actualizar artistas
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            
            # Creamos índices optimizados si se solicita
            if config.get('create_indices', True):
                indices_created = create_indices(conn)
                logger.info(f"Creados {indices_created} índices para optimizar consultas")
            
            # Elegir la operación a realizar
            operation = config.get('operation', 'update')
            
            results = {}
            
            if operation == 'update_mbids':
                # Actualizar MBIDs faltantes
                limit = config.get('limit', 50)
                processed, updated = update_missing_mbids(conn, limit)
                results = {"processed": processed, "updated": updated, "operation": "update_mbids"}
                logger.info(f"Actualizados {updated} de {processed} MBIDs faltantes")
            
            elif operation == 'update_networks':
                # Actualizar solo la tabla de networks
                limit = config.get('limit', 50)
                artists = get_artists_for_update(conn, limit, only_networks=True)
                updated = 0
                
                for artist_id, artist_name, artist_mbid in artists:
                    musicbrainz_url = f"https://musicbrainz.org/artist/{artist_mbid}"
                    # Asegurar que la tabla tiene la estructura correcta
                    update_artists_networks_table(conn)
                    # Actualizar el registro
                    update_artists_networks(conn, artist_id, musicbrainz_url)
                    updated += 1
                    # Pausa para no sobrecargar
                    time.sleep(0.5)
                    
                conn.commit()
                results = {"processed": len(artists), "updated": updated, "operation": "update_networks"}
                logger.info(f"Actualizadas {updated} URLs de MusicBrainz en networks")
            
            else:
                # Operación por defecto: actualizar toda la información
                limit = config.get('limit', 50)
                total, updated = update_batch_artists(conn, limit)
                results = {"processed": total, "updated": updated, "operation": "update"}
                logger.info(f"Actualización completada: {updated} de {total} artistas")
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Error en procesamiento: {e}")
            return False
    else:
        logger.error("Error: No se proporcionó configuración")
        return False

# Condicional para ejecución directa
if __name__ == "__main__":
    main()