#!/usr/bin/env python3
"""
Módulo de gestión de base de datos para el sistema de seguimiento de artistas
Contiene todas las operaciones de base de datos y lógica de datos
"""

import sqlite3
import logging
import json
import hashlib
from datetime import datetime
from typing import Optional, List, Dict, Tuple
import asyncio
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager


logger = logging.getLogger(__name__)

class ArtistTrackerDatabase:
    """Clase para manejar la base de datos de usuarios y artistas seguidos"""

    def __init__(self, db_path: str = "artist_tracker.db"):
        """
        Inicializa la base de datos

        Args:
            db_path: Ruta del archivo de base de datos
        """
        self.db_path = db_path
        self.init_database()

    def get_connection(self) -> sqlite3.Connection:
        """Obtiene una conexión a la base de datos"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_database(self):
        """Inicializa las tablas de la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Tabla de usuarios
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT NOT NULL UNIQUE,
                    chat_id INTEGER NOT NULL UNIQUE,
                    notification_time TEXT DEFAULT '09:00',
                    notification_enabled BOOLEAN DEFAULT 1,
                    country_filter TEXT DEFAULT 'ES',
                    service_ticketmaster BOOLEAN DEFAULT 1,
                    service_spotify BOOLEAN DEFAULT 1,
                    service_setlistfm BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Verificar si las nuevas columnas existen, si no, añadirlas
            cursor.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'country_filter' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN country_filter TEXT DEFAULT 'ES'")
                logger.info("Columna country_filter añadida a users")

            if 'service_ticketmaster' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN service_ticketmaster BOOLEAN DEFAULT 1")
                logger.info("Columna service_ticketmaster añadida a users")

            if 'service_spotify' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN service_spotify BOOLEAN DEFAULT 1")
                logger.info("Columna service_spotify añadida a users")

            if 'service_setlistfm' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN service_setlistfm BOOLEAN DEFAULT 1")
                logger.info("Columna service_setlistfm añadida a users")

            # Tabla de artistas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS artists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    mbid TEXT UNIQUE,
                    country TEXT,
                    formed_year INTEGER,
                    ended_year INTEGER,
                    total_works INTEGER,
                    musicbrainz_url TEXT,
                    artist_type TEXT,
                    disambiguation TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, mbid)
                )
            """)

            # Tabla de relación usuarios-artistas seguidos
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_followed_artists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    artist_id INTEGER NOT NULL,
                    followed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (artist_id) REFERENCES artists (id) ON DELETE CASCADE,
                    UNIQUE(user_id, artist_id)
                )
            """)

            # Tabla temporal para selecciones de artistas pendientes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_artist_selections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    search_results TEXT NOT NULL,
                    original_query TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Nueva tabla para conciertos encontrados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS concerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    artist_name TEXT NOT NULL,
                    concert_name TEXT NOT NULL,
                    venue TEXT,
                    city TEXT,
                    country TEXT,
                    date TEXT,
                    time TEXT,
                    url TEXT,
                    source TEXT,
                    concert_hash TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Nueva tabla para notificaciones enviadas
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS notifications_sent (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    concert_id INTEGER NOT NULL,
                    notification_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (concert_id) REFERENCES concerts (id) ON DELETE CASCADE,
                    UNIQUE(user_id, concert_id)
                )
            """)

            # Nueva tabla para caché de búsquedas de usuario
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_search_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    search_type TEXT NOT NULL,
                    search_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
                )
            """)

            # Nueva tabla para usuarios de Last.fm
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_lastfm (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    lastfm_username TEXT NOT NULL,
                    lastfm_playcount INTEGER DEFAULT 0,
                    lastfm_registered TEXT DEFAULT '',
                    sync_limit INTEGER DEFAULT 20,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    UNIQUE(user_id)
                )
            """)

            # Tabla para almacenar selecciones pendientes de Last.fm
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_lastfm_sync (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    period TEXT NOT NULL,
                    artists_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    UNIQUE(user_id, period)
                )
            """)

            # Nueva tabla para usuarios de Spotify
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_spotify (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    spotify_username TEXT NOT NULL,
                    spotify_display_name TEXT DEFAULT '',
                    spotify_followers INTEGER DEFAULT 0,
                    spotify_playlists INTEGER DEFAULT 0,
                    artists_limit INTEGER DEFAULT 20,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    UNIQUE(user_id)
                )
            """)

            # Tabla para almacenar artistas pendientes de Spotify
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pending_spotify_artists (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    artists_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    UNIQUE(user_id)
                )
            """)

            # Índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_chat_id ON users(chat_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_artists_mbid ON artists(mbid)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_followed_user_id ON user_followed_artists(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_followed_artist_id ON user_followed_artists(artist_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_chat_id ON pending_artist_selections(chat_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concerts_hash ON concerts(concert_hash)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_concerts_artist ON concerts(artist_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_notifications_user ON notifications_sent(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_notifications_concert ON notifications_sent(concert_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_search_cache_user ON user_search_cache(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_search_cache_created ON user_search_cache(created_at)")

            # Índices para Last.fm
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_lastfm_user_id ON user_lastfm(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_lastfm_user_id ON pending_lastfm_sync(user_id)")

            # Índices para Spotify
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_spotify_user_id ON user_spotify(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pending_spotify_user_id ON pending_spotify_artists(user_id)")

            conn.commit()
            logger.info("Base de datos inicializada correctamente")

        except sqlite3.Error as e:
            logger.error(f"Error al inicializar la base de datos: {e}")
            conn.rollback()
        finally:
            conn.close()

    def add_user(self, username: str, chat_id: int) -> bool:
        """
        Añade un nuevo usuario

        Args:
            username: Nombre de usuario
            chat_id: ID del chat de Telegram

        Returns:
            True si se añadió correctamente, False en caso contrario
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO users (username, chat_id, last_activity)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (username, chat_id))

            conn.commit()
            logger.info(f"Usuario {username} añadido/actualizado con chat_id {chat_id}")
            return True

        except sqlite3.Error as e:
            logger.error(f"Error al añadir usuario {username}: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_by_chat_id(self, chat_id: int) -> Optional[Dict]:
        """
        Obtiene un usuario por su chat_id

        Args:
            chat_id: ID del chat de Telegram

        Returns:
            Diccionario con datos del usuario o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            logger.error(f"Error al obtener usuario por chat_id {chat_id}: {e}")
            return None
        finally:
            conn.close()

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        """
        Obtiene un usuario por su nombre de usuario

        Args:
            username: Nombre de usuario

        Returns:
            Diccionario con datos del usuario o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            logger.error(f"Error al obtener usuario {username}: {e}")
            return None
        finally:
            conn.close()

    def get_artist_by_mbid(self, mbid: str) -> Optional[int]:
        """
        Busca un artista por su MBID

        Args:
            mbid: MusicBrainz ID del artista

        Returns:
            ID del artista o None si no existe
        """
        if not mbid:
            return None

        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT id FROM artists WHERE mbid = ?", (mbid,))
            row = cursor.fetchone()

            if row:
                return row[0]
            return None

        except sqlite3.Error as e:
            logger.error(f"Error buscando artista por MBID {mbid}: {e}")
            return None
        finally:
            conn.close()

    def search_artist_candidates(self, artist_name: str) -> List[Dict]:
        """
        Busca candidatos de artistas en MusicBrainz con estrategias mejoradas

        Args:
            artist_name: Nombre del artista a buscar

        Returns:
            Lista de candidatos encontrados, ordenados por relevancia
        """
        # Esta función necesitará acceso a las APIs de MusicBrainz
        # Se mantendrá aquí pero se adaptará para recibir las dependencias
        logger.info(f"Buscando candidatos para '{artist_name}' en MusicBrainz...")

        try:
            # Importar funciones de MusicBrainz
            from apis.mb_artist_info import search_artist_in_musicbrainz, get_artist_from_musicbrainz

            candidates = []

            # Estrategia 1: Búsqueda exacta con comillas
            exact_results = self._search_exact_artist(artist_name)
            if exact_results:
                candidates.extend(exact_results)
                logger.info(f"Búsqueda exacta: {len(exact_results)} resultados")

            # Estrategia 2: Búsqueda por campo artist específico
            if len(candidates) < 5:
                field_results = self._search_artist_field(artist_name)
                candidates.extend(field_results)
                logger.info(f"Búsqueda por campo: {len(field_results)} resultados adicionales")

            # Estrategia 3: Búsqueda básica solo si es necesario
            if len(candidates) < 3:
                basic_results = self._search_basic_artist(artist_name)
                candidates.extend(basic_results)
                logger.info(f"Búsqueda básica: {len(basic_results)} resultados adicionales")

            # Eliminar duplicados basándose en MBID
            seen_mbids = set()
            unique_candidates = []
            for candidate in candidates:
                mbid = candidate.get('mbid')
                if mbid and mbid not in seen_mbids:
                    seen_mbids.add(mbid)
                    unique_candidates.append(candidate)
                elif not mbid:
                    unique_candidates.append(candidate)

            # Aplicar filtros de relevancia
            filtered_candidates = self._filter_candidates_by_relevance(unique_candidates, artist_name)

            # Ordenar por score y relevancia
            final_candidates = self._rank_candidates(filtered_candidates, artist_name)

            logger.info(f"Candidatos finales para '{artist_name}': {len(final_candidates)}")
            return final_candidates[:10]

        except Exception as e:
            logger.error(f"Error al buscar candidatos para '{artist_name}': {e}")
            return self._fallback_search(artist_name)

    def _search_exact_artist(self, artist_name: str) -> List[Dict]:
        """Búsqueda exacta usando comillas"""
        try:
            from apis.mb_artist_info import search_artist_in_musicbrainz

            exact_query = f'"{artist_name}"'
            search_results = search_artist_in_musicbrainz(exact_query)

            if not search_results:
                return []

            candidates = []
            for result in search_results[:5]:
                candidate = self._parse_search_result(result, boost_score=20)
                candidates.append(candidate)

            return candidates

        except Exception as e:
            logger.error(f"Error en búsqueda exacta: {e}")
            return []

    def _search_basic_artist(self, artist_name: str) -> List[Dict]:
        """Búsqueda básica como último recurso"""
        try:
            from apis.mb_artist_info import search_artist_in_musicbrainz

            search_results = search_artist_in_musicbrainz(artist_name)

            if not search_results:
                return []

            candidates = []
            for result in search_results[:8]:
                candidate = self._parse_search_result(result, boost_score=0)
                candidates.append(candidate)

            return candidates

        except Exception as e:
            logger.error(f"Error en búsqueda básica: {e}")
            return []

    def _search_artist_field(self, artist_name: str) -> List[Dict]:
        """Búsqueda específica en el campo artist"""
        try:
            from apis.mb_artist_info import search_artist_in_musicbrainz

            field_query = f'artist:"{artist_name}"'
            search_results = search_artist_in_musicbrainz(field_query)

            if not search_results:
                field_query = f'artist:{artist_name}'
                search_results = search_artist_in_musicbrainz(field_query)

            if not search_results:
                return []

            candidates = []
            for result in search_results[:5]:
                candidate = self._parse_search_result(result, boost_score=10)
                candidates.append(candidate)

            return candidates

        except Exception as e:
            logger.error(f"Error en búsqueda por campo: {e}")
            return []

    def _fallback_search(self, artist_name: str) -> List[Dict]:
        """Búsqueda de fallback usando el método original"""
        try:
            from apis.mb_artist_info import search_artist_in_musicbrainz

            logger.info(f"Usando búsqueda de fallback para '{artist_name}'")
            search_results = search_artist_in_musicbrainz(artist_name)

            if not search_results:
                return []

            candidates = []
            for result in search_results[:10]:
                score = 0
                try:
                    score_value = result.get('ext:score', result.get('score', 0))
                    if isinstance(score_value, str):
                        score = int(float(score_value))
                    elif isinstance(score_value, (int, float)):
                        score = int(score_value)
                except (ValueError, TypeError):
                    score = 0

                candidate = {
                    'mbid': result.get('id'),
                    'name': result.get('name', artist_name),
                    'type': result.get('type', ''),
                    'country': result.get('country', ''),
                    'disambiguation': result.get('disambiguation', ''),
                    'score': score
                }

                if 'life-span' in result:
                    life_span = result['life-span']
                    if 'begin' in life_span and life_span['begin']:
                        candidate['formed_year'] = life_span['begin'][:4]
                    if 'end' in life_span and life_span['end']:
                        candidate['ended_year'] = life_span['end'][:4]

                candidates.append(candidate)

            candidates.sort(key=lambda x: x['score'], reverse=True)
            return candidates

        except Exception as e:
            logger.error(f"Error en búsqueda de fallback: {e}")
            return []

    def _parse_search_result(self, result: Dict, boost_score: int = 0) -> Dict:
        """Convierte un resultado de MusicBrainz en un candidato"""
        score = 0
        try:
            score_value = result.get('ext:score', result.get('score', 0))
            if isinstance(score_value, str):
                score = int(float(score_value))
            elif isinstance(score_value, (int, float)):
                score = int(score_value)
        except (ValueError, TypeError):
            score = 0

        score += boost_score

        candidate = {
            'mbid': result.get('id'),
            'name': result.get('name', ''),
            'type': result.get('type', ''),
            'country': result.get('country', ''),
            'disambiguation': result.get('disambiguation', ''),
            'score': score
        }

        if 'life-span' in result:
            life_span = result['life-span']
            if 'begin' in life_span and life_span['begin']:
                candidate['formed_year'] = life_span['begin'][:4]
            if 'end' in life_span and life_span['end']:
                candidate['ended_year'] = life_span['end'][:4]

        return candidate

    def _filter_candidates_by_relevance(self, candidates: List[Dict], original_query: str) -> List[Dict]:
        """Filtra candidatos por relevancia usando múltiples criterios"""
        if not candidates:
            return []

        filtered = []
        query_lower = original_query.lower()
        query_words = set(query_lower.split())

        for candidate in candidates:
            name_lower = candidate['name'].lower()
            name_words = set(name_lower.split())

            relevance_score = 0

            if name_lower == query_lower:
                relevance_score += 100
            elif query_words.issubset(name_words):
                relevance_score += 80
            else:
                word_matches = len(query_words.intersection(name_words))
                if word_matches > 0:
                    match_ratio = word_matches / len(query_words)
                    relevance_score += match_ratio * 60

                    if query_words and name_words:
                        first_query_word = list(query_words)[0]
                        if first_query_word in name_words:
                            relevance_score += 10
                else:
                    if any(word in name_lower for word in query_words):
                        relevance_score += 20
                    else:
                        continue

            extra_words = len(name_words) - len(query_words)
            if extra_words > 2:
                relevance_score -= extra_words * 3

            artist_type = candidate.get('type', '').lower()
            if artist_type in ['person', 'group', 'band']:
                relevance_score += 5

            formed_year = candidate.get('formed_year')
            if formed_year:
                try:
                    year = int(formed_year)
                    if year < 1700:
                        relevance_score -= 15
                    elif year < 1900 and 'composer' in candidate.get('type', '').lower():
                        relevance_score -= 10
                except (ValueError, TypeError):
                    pass

            candidate['relevance_score'] = max(0, relevance_score)

            min_threshold = 15 if len(candidates) < 5 else 25
            if relevance_score >= min_threshold:
                filtered.append(candidate)

        if not filtered and candidates:
            logger.info(f"Aplicando umbral más permisivo para '{original_query}'")
            for candidate in candidates:
                if candidate.get('relevance_score', 0) >= 10:
                    filtered.append(candidate)

        if not filtered and candidates:
            logger.info(f"Usando fallback de score original para '{original_query}'")
            filtered = sorted(candidates, key=lambda x: x.get('score', 0), reverse=True)[:3]

        return filtered

    def _rank_candidates(self, candidates: List[Dict], original_query: str) -> List[Dict]:
        """Ordena candidatos por relevancia combinada"""
        if not candidates:
            return []

        def combined_score(candidate):
            mb_score = candidate.get('score', 0)
            relevance_score = candidate.get('relevance_score', 0)
            return (relevance_score * 1.5) + (mb_score * 0.5)

        sorted_candidates = sorted(candidates, key=combined_score, reverse=True)
        return sorted_candidates

    def create_artist_from_candidate(self, candidate: Dict) -> Optional[int]:
        """
        Crea un artista en la base de datos a partir de un candidato seleccionado

        Args:
            candidate: Diccionario con datos del candidato

        Returns:
            ID del artista creado o None si hay error
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            mbid = candidate['mbid']

            # Verificar si ya existe
            cursor.execute("SELECT id FROM artists WHERE mbid = ?", (mbid,))
            row = cursor.fetchone()
            if row:
                return row[0]

            # Obtener datos detallados del artista
            from apis.mb_artist_info import get_artist_from_musicbrainz
            artist_data = get_artist_from_musicbrainz(mbid) if mbid else None

            # Extraer información relevante
            name = candidate['name']
            country = candidate.get('country')
            artist_type = candidate.get('type')
            disambiguation = candidate.get('disambiguation')
            formed_year = None
            ended_year = None
            total_works = None
            musicbrainz_url = f"https://musicbrainz.org/artist/{mbid}" if mbid else None

            if artist_data:
                country = artist_data.get('country') or country
                artist_type = artist_data.get('type') or artist_type
                disambiguation = artist_data.get('disambiguation') or disambiguation

                if 'life-span' in artist_data:
                    life_span = artist_data['life-span']
                    if 'begin' in life_span and life_span['begin']:
                        try:
                            formed_year = int(life_span['begin'][:4])
                        except (ValueError, TypeError):
                            pass
                    if 'end' in life_span and life_span['end']:
                        try:
                            ended_year = int(life_span['end'][:4])
                        except (ValueError, TypeError):
                            pass

                if 'release-group-count' in artist_data:
                    try:
                        total_works = int(artist_data['release-group-count'])
                    except (ValueError, TypeError):
                        pass
                elif 'work-count' in artist_data:
                    try:
                        total_works = int(artist_data['work-count'])
                    except (ValueError, TypeError):
                        pass

            # Insertar artista
            cursor.execute("""
                INSERT INTO artists (name, mbid, country, formed_year, ended_year, total_works,
                                   musicbrainz_url, artist_type, disambiguation)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (name, mbid, country, formed_year, ended_year, total_works,
                  musicbrainz_url, artist_type, disambiguation))

            artist_id = cursor.lastrowid
            conn.commit()

            logger.info(f"Artista '{name}' creado con datos de MusicBrainz (MBID: {mbid})")
            return artist_id

        except sqlite3.Error as e:
            logger.error(f"Error al crear artista: {e}")
            conn.rollback()
            return None
        finally:
            conn.close()

    def add_followed_artist(self, user_id: int, artist_id: int) -> bool:
        """
        Añade un artista a la lista de seguimiento de un usuario

        Args:
            user_id: ID del usuario
            artist_id: ID del artista

        Returns:
            True si se añadió correctamente (era nuevo), False si ya existía
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO user_followed_artists (user_id, artist_id)
                VALUES (?, ?)
            """, (user_id, artist_id))

            was_new = cursor.rowcount > 0
            conn.commit()
            return was_new

        except sqlite3.Error as e:
            logger.error(f"Error al añadir artista seguido: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_followed_artists(self, user_id: int) -> List[Dict]:
        """
        Obtiene la lista de artistas seguidos por un usuario

        Args:
            user_id: ID del usuario

        Returns:
            Lista de diccionarios con información de los artistas
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT a.*, ufa.followed_at
                FROM artists a
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                WHERE ufa.user_id = ?
                ORDER BY ufa.followed_at DESC
            """, (user_id,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error al obtener artistas seguidos para usuario {user_id}: {e}")
            return []
        finally:
            conn.close()

    def remove_followed_artist(self, user_id: int, artist_name: str) -> bool:
        """
        Elimina un artista de la lista de seguimiento de un usuario

        Args:
            user_id: ID del usuario
            artist_name: Nombre del artista

        Returns:
            True si se eliminó correctamente, False en caso contrario
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                DELETE FROM user_followed_artists
                WHERE user_id = ? AND artist_id = (
                    SELECT id FROM artists WHERE LOWER(name) = LOWER(?)
                )
            """, (user_id, artist_name))

            was_removed = cursor.rowcount > 0
            conn.commit()
            return was_removed

        except sqlite3.Error as e:
            logger.error(f"Error al eliminar artista seguido: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def save_pending_selection(self, chat_id: int, candidates: List[Dict], original_query: str) -> bool:
        """
        Guarda una selección pendiente para un usuario

        Args:
            chat_id: ID del chat
            candidates: Lista de candidatos
            original_query: Consulta original del usuario

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar selecciones anteriores del mismo chat
            cursor.execute("DELETE FROM pending_artist_selections WHERE chat_id = ?", (chat_id,))

            # Guardar nueva selección
            cursor.execute("""
                INSERT INTO pending_artist_selections (chat_id, search_results, original_query)
                VALUES (?, ?, ?)
            """, (chat_id, json.dumps(candidates), original_query))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error al guardar selección pendiente: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_pending_selection(self, chat_id: int) -> Optional[Tuple[List[Dict], str]]:
        """
        Obtiene una selección pendiente para un usuario

        Args:
            chat_id: ID del chat

        Returns:
            Tupla con (candidatos, consulta_original) o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT search_results, original_query
                FROM pending_artist_selections
                WHERE chat_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (chat_id,))

            row = cursor.fetchone()
            if row:
                candidates = json.loads(row[0])
                original_query = row[1]
                return candidates, original_query

            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error al obtener selección pendiente: {e}")
            return None
        finally:
            conn.close()

    def clear_pending_selection(self, chat_id: int):
        """Limpia la selección pendiente de un usuario"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("DELETE FROM pending_artist_selections WHERE chat_id = ?", (chat_id,))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error al limpiar selección pendiente: {e}")
        finally:
            conn.close()

    def save_concert(self, concert_data: Dict) -> Optional[int]:
        """
        Guarda un concierto en la base de datos

        Args:
            concert_data: Diccionario con datos del concierto

        Returns:
            ID del concierto guardado o None si ya existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Crear hash único para el concierto
            concert_hash = self._create_concert_hash(concert_data)

            # Verificar si ya existe
            cursor.execute("SELECT id FROM concerts WHERE concert_hash = ?", (concert_hash,))
            existing = cursor.fetchone()
            if existing:
                return existing[0]

            # Insertar nuevo concierto
            cursor.execute("""
                INSERT INTO concerts (
                    artist_name, concert_name, venue, city, country,
                    date, time, url, source, concert_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                concert_data.get('artist', ''),
                concert_data.get('name', ''),
                concert_data.get('venue', ''),
                concert_data.get('city', ''),
                concert_data.get('country', ''),
                concert_data.get('date', ''),
                concert_data.get('time', ''),
                concert_data.get('url', ''),
                concert_data.get('source', ''),
                concert_hash
            ))

            concert_id = cursor.lastrowid
            conn.commit()
            return concert_id

        except sqlite3.Error as e:
            logger.error(f"Error al guardar concierto: {e}")
            return None
        finally:
            conn.close()

    def _create_concert_hash(self, concert_data: Dict) -> str:
        """Crea un hash único para un concierto"""
        key_data = f"{concert_data.get('artist', '')}-{concert_data.get('venue', '')}-{concert_data.get('date', '')}-{concert_data.get('source', '')}"
        return hashlib.md5(key_data.encode()).hexdigest()

    def mark_concert_notified(self, user_id: int, concert_id: int) -> bool:
        """
        Marca un concierto como notificado para un usuario

        Args:
            user_id: ID del usuario
            concert_id: ID del concierto

        Returns:
            True si se marcó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO notifications_sent (user_id, concert_id)
                VALUES (?, ?)
            """, (user_id, concert_id))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error al marcar concierto como notificado: {e}")
            return False
        finally:
            conn.close()

    def get_unnotified_concerts_for_user(self, user_id: int) -> List[Dict]:
        """
        Obtiene conciertos no notificados para un usuario

        Args:
            user_id: ID del usuario

        Returns:
            Lista de conciertos no notificados
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT DISTINCT c.*
                FROM concerts c
                JOIN artists a ON LOWER(c.artist_name) = LOWER(a.name)
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                WHERE ufa.user_id = ?
                AND NOT EXISTS (
                    SELECT 1 FROM notifications_sent ns
                    WHERE ns.user_id = ? AND ns.concert_id = c.id
                )
                ORDER BY c.date DESC
            """, (user_id, user_id))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error al obtener conciertos no notificados: {e}")
            return []
        finally:
            conn.close()

    def get_all_concerts_for_user(self, user_id: int) -> List[Dict]:
        """
        Obtiene todos los conciertos para un usuario (notificados y no notificados)

        Args:
            user_id: ID del usuario

        Returns:
            Lista de todos los conciertos
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT DISTINCT c.*,
                       CASE WHEN ns.id IS NOT NULL THEN 1 ELSE 0 END as notified
                FROM concerts c
                JOIN artists a ON LOWER(c.artist_name) = LOWER(a.name)
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                LEFT JOIN notifications_sent ns ON ns.user_id = ? AND ns.concert_id = c.id
                WHERE ufa.user_id = ?
                ORDER BY c.date DESC
            """, (user_id, user_id))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error al obtener todos los conciertos: {e}")
            return []
        finally:
            conn.close()

    def get_users_for_notifications(self) -> List[Dict]:
        """
        Obtiene usuarios que tienen notificaciones habilitadas

        Returns:
            Lista de usuarios con notificaciones habilitadas
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT * FROM users
                WHERE notification_enabled = 1
            """)

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error al obtener usuarios para notificaciones: {e}")
            return []
        finally:
            conn.close()

    def save_user_search_cache(self, user_id: int, search_type: str, data: List[Dict]):
        """Guarda datos de búsqueda en caché temporal"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché anterior del usuario
            cursor.execute("DELETE FROM user_search_cache WHERE user_id = ?", (user_id,))

            # Guardar nuevos datos
            cursor.execute("""
                INSERT INTO user_search_cache (user_id, search_type, search_data)
                VALUES (?, ?, ?)
            """, (user_id, search_type, json.dumps(data)))

            conn.commit()
            logger.info(f"Caché guardado para usuario {user_id}: {search_type}")

        except sqlite3.Error as e:
            logger.error(f"Error guardando caché: {e}")
        finally:
            conn.close()

    def save_user_search_cache(user_id: int, search_type: str, data: List[Dict]):
        """Guarda datos de búsqueda en caché temporal"""
        conn = db.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché anterior del usuario
            cursor.execute("DELETE FROM user_search_cache WHERE user_id = ?", (user_id,))

            # Guardar nuevos datos
            cursor.execute("""
                INSERT INTO user_search_cache (user_id, search_type, search_data)
                VALUES (?, ?, ?)
            """, (user_id, search_type, json.dumps(data)))

            conn.commit()
            logger.info(f"Caché guardado para usuario {user_id}: {search_type}")

        except sqlite3.Error as e:
            logger.error(f"Error guardando caché: {e}")
        finally:
            conn.close()


    def get_user_search_cache(self, user_id: int) -> Optional[Tuple[str, List[Dict]]]:
        """Obtiene datos de búsqueda del caché"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché antiguo (más de 1 hora)
            cursor.execute("""
                DELETE FROM user_search_cache
                WHERE created_at < datetime('now', '-1 hour')
            """)

            # Obtener datos del usuario
            cursor.execute("""
                SELECT search_type, search_data
                FROM user_search_cache
                WHERE user_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))

            row = cursor.fetchone()
            if row:
                search_type = row[0]
                data = json.loads(row[1])
                return search_type, data

            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo caché: {e}")
            return None
        finally:
            conn.close()

    def save_list_pagination_data(self, user_id: int, artists_data: List[Dict], display_name: str):
        """Guarda datos de artistas para paginación temporal"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar datos anteriores del usuario
            cursor.execute("DELETE FROM user_search_cache WHERE user_id = ? AND search_type LIKE 'list_%'", (user_id,))

            # Guardar nuevos datos
            data_to_save = {
                'artists': artists_data,
                'display_name': display_name
            }

            cursor.execute("""
                INSERT INTO user_search_cache (user_id, search_type, search_data)
                VALUES (?, ?, ?)
            """, (user_id, "list_pagination", json.dumps(data_to_save)))

            conn.commit()
            logger.info(f"Datos de lista guardados para usuario {user_id}")

        except sqlite3.Error as e:
            logger.error(f"Error guardando datos de lista: {e}")
        finally:
            conn.close()

    def get_list_pagination_data(self, user_id: int) -> Optional[Tuple[List[Dict], str]]:
        """Obtiene datos de artistas del caché temporal"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché antiguo (más de 1 hora)
            cursor.execute("""
                DELETE FROM user_search_cache
                WHERE created_at < datetime('now', '-1 hour')
            """)

            # Obtener datos del usuario
            cursor.execute("""
                SELECT search_data
                FROM user_search_cache
                WHERE user_id = ? AND search_type = 'list_pagination'
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id,))

            row = cursor.fetchone()
            if row:
                data = json.loads(row[0])
                return data['artists'], data['display_name']

            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo datos de lista: {e}")
            return None
        finally:
            conn.close()

    def save_artist_concerts_cache(self, user_id: int, artist_name: str, concerts: List[Dict]):
        """Guarda conciertos de un artista en caché temporal para botones"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché anterior del usuario para este artista
            cursor.execute("""
                DELETE FROM user_search_cache
                WHERE user_id = ? AND search_type LIKE ?
            """, (user_id, f"showartist_{artist_name}%"))

            # Guardar nuevos datos
            cursor.execute("""
                INSERT INTO user_search_cache (user_id, search_type, search_data)
                VALUES (?, ?, ?)
            """, (user_id, f"showartist_{artist_name}", json.dumps(concerts)))

            conn.commit()
            logger.info(f"Caché de conciertos guardado para {artist_name}")

        except sqlite3.Error as e:
            logger.error(f"Error guardando caché de artista: {e}")
        finally:
            conn.close()

    def get_artist_concerts_cache(self, user_id: int, artist_name: str) -> Optional[List[Dict]]:
        """Obtiene conciertos de un artista del caché"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché antiguo (más de 1 hora)
            cursor.execute("""
                DELETE FROM user_search_cache
                WHERE created_at < datetime('now', '-1 hour')
            """)

            # Obtener datos del artista
            cursor.execute("""
                SELECT search_data
                FROM user_search_cache
                WHERE user_id = ? AND search_type = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (user_id, f"showartist_{artist_name}"))

            row = cursor.fetchone()
            if row:
                data = json.loads(row[0])
                return data

            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo caché de artista: {e}")
            return None
        finally:
            conn.close()

    # ======================
    # FUNCIONES DE LAST.FM
    # ======================

    def set_user_lastfm(self, user_id: int, lastfm_username: str, user_info: dict = None) -> bool:
        """
        Establece el usuario de Last.fm para un usuario

        Args:
            user_id: ID del usuario
            lastfm_username: Nombre de usuario de Last.fm
            user_info: Información adicional del usuario (opcional)

        Returns:
            True si se estableció correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            playcount = 0
            registered = ''

            if user_info:
                playcount = user_info.get('playcount', 0)
                registered = user_info.get('registered', '')

            cursor.execute("""
                INSERT OR REPLACE INTO user_lastfm
                (user_id, lastfm_username, lastfm_playcount, lastfm_registered, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, lastfm_username, playcount, registered))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error estableciendo usuario Last.fm: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_lastfm(self, user_id: int) -> Optional[Dict]:
        """
        Obtiene el usuario de Last.fm asociado

        Args:
            user_id: ID del usuario

        Returns:
            Diccionario con datos de Last.fm o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM user_lastfm WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo usuario Last.fm: {e}")
            return None
        finally:
            conn.close()

    def set_lastfm_sync_limit(self, user_id: int, limit: int) -> bool:
        """
        Establece el límite de sincronización para Last.fm

        Args:
            user_id: ID del usuario
            limit: Número de artistas a sincronizar

        Returns:
            True si se estableció correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE user_lastfm SET sync_limit = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (limit, user_id))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error estableciendo límite de sincronización: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def save_pending_lastfm_sync(self, user_id: int, period: str, artists_data: List[Dict]) -> bool:
        """
        Guarda una sincronización pendiente de Last.fm

        Args:
            user_id: ID del usuario
            period: Período de Last.fm
            artists_data: Lista de artistas a sincronizar

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO pending_lastfm_sync
                (user_id, period, artists_data, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, period, json.dumps(artists_data)))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error guardando sincronización pendiente: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_pending_lastfm_sync(self, user_id: int, period: str) -> Optional[List[Dict]]:
        """
        Obtiene una sincronización pendiente de Last.fm

        Args:
            user_id: ID del usuario
            period: Período de Last.fm

        Returns:
            Lista de artistas o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT artists_data FROM pending_lastfm_sync
                WHERE user_id = ? AND period = ?
            """, (user_id, period))

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo sincronización pendiente: {e}")
            return None
        finally:
            conn.close()

    def clear_pending_lastfm_sync(self, user_id: int, period: str = None):
        """
        Limpia sincronizaciones pendientes de Last.fm

        Args:
            user_id: ID del usuario
            period: Período específico (opcional, si no se especifica limpia todos)
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            if period:
                cursor.execute("DELETE FROM pending_lastfm_sync WHERE user_id = ? AND period = ?", (user_id, period))
            else:
                cursor.execute("DELETE FROM pending_lastfm_sync WHERE user_id = ?", (user_id,))

            conn.commit()

        except sqlite3.Error as e:
            logger.error(f"Error limpiando sincronización pendiente: {e}")
        finally:
            conn.close()

    # ======================
    # FUNCIONES DE SPOTIFY
    # ======================

    def set_user_spotify(self, user_id: int, spotify_username: str, user_info: dict = None) -> bool:
        """
        Establece el usuario de Spotify para un usuario

        Args:
            user_id: ID del usuario
            spotify_username: Nombre de usuario de Spotify
            user_info: Información adicional del usuario (opcional)

        Returns:
            True si se estableció correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            followers = 0
            display_name = spotify_username
            public_playlists = 0

            if user_info:
                followers = user_info.get('followers', 0)
                display_name = user_info.get('display_name', spotify_username)
                public_playlists = user_info.get('public_playlists', 0)

            cursor.execute("""
                INSERT OR REPLACE INTO user_spotify
                (user_id, spotify_username, spotify_display_name, spotify_followers,
                 spotify_playlists, artists_limit, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, spotify_username, display_name, followers, public_playlists, 20))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error estableciendo usuario Spotify: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_spotify(self, user_id: int) -> Optional[Dict]:
        """
        Obtiene el usuario de Spotify asociado

        Args:
            user_id: ID del usuario

        Returns:
            Diccionario con datos de Spotify o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT * FROM user_spotify WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()

            if row:
                return dict(row)
            return None

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo usuario Spotify: {e}")
            return None
        finally:
            conn.close()

    def set_spotify_artists_limit(self, user_id: int, limit: int) -> bool:
        """
        Establece el límite de artistas para Spotify

        Args:
            user_id: ID del usuario
            limit: Número de artistas a mostrar

        Returns:
            True si se estableció correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE user_spotify SET artists_limit = ?, updated_at = CURRENT_TIMESTAMP
                WHERE user_id = ?
            """, (limit, user_id))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error estableciendo límite de artistas Spotify: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def save_pending_spotify_artists(self, user_id: int, artists_data: List[Dict]) -> bool:
        """
        Guarda artistas pendientes de Spotify

        Args:
            user_id: ID del usuario
            artists_data: Lista de artistas

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO pending_spotify_artists
                (user_id, artists_data, created_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (user_id, json.dumps(artists_data)))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error guardando artistas pendientes Spotify: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_pending_spotify_artists(self, user_id: int) -> Optional[List[Dict]]:
        """
        Obtiene artistas pendientes de Spotify

        Args:
            user_id: ID del usuario

        Returns:
            Lista de artistas o None si no existe
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT artists_data FROM pending_spotify_artists
                WHERE user_id = ?
            """, (user_id,))

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo artistas pendientes Spotify: {e}")
            return None
        finally:
            conn.close()

    def clear_pending_spotify_artists(self, user_id: int):
        """
        Limpia artistas pendientes de Spotify

        Args:
            user_id: ID del usuario
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("DELETE FROM pending_spotify_artists WHERE user_id = ?", (user_id,))
            conn.commit()

        except sqlite3.Error as e:
            logger.error(f"Error limpiando artistas pendientes Spotify: {e}")
        finally:
            conn.close()

    def format_artists_preview(self, artists: List[Dict], limit: int = 10) -> str:
        """
        Formatea una vista previa de artistas con información de MBID

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
        mbid_count = sum(1 for artist in display_artists if artist.get("mbid"))

        for i, artist in enumerate(display_artists, 1):
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
                line += " 🎵"  # Emoji para indicar que tiene MBID

            # Añadir géneros si están disponibles
            genres = artist.get("genres", [])
            if genres:
                genre_text = ", ".join(genres[:2])  # Mostrar hasta 2 géneros
                line += f" _{genre_text}_"

            lines.append(line)

        if len(artists) > limit:
            lines.append(f"_...y {len(artists) - limit} más_")

        # Añadir estadísticas de MBID
        lines.append("")
        lines.append(f"🎵 {mbid_count}/{len(display_artists)} artistas con MBID para sincronización precisa")

        return "\n".join(lines)


    def save_pending_playlist_artists(self, user_id: int, playlist_id: str, playlist_name: str, artists_data: List[Dict]) -> bool:
        """
        Guarda artistas pendientes de una playlist de Spotify

        Args:
            user_id: ID del usuario
            playlist_id: ID de la playlist
            playlist_name: Nombre de la playlist
            artists_data: Lista de artistas

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            data_to_save = {
                'playlist_id': playlist_id,
                'playlist_name': playlist_name,
                'artists': artists_data
            }

            cursor.execute("""
                INSERT OR REPLACE INTO user_search_cache
                (user_id, search_type, search_data, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, f"playlist_{playlist_id}", json.dumps(data_to_save)))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error guardando artistas de playlist: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_pending_playlist_artists(self, user_id: int, playlist_id: str) -> Optional[Dict]:
        """
        Obtiene artistas pendientes de una playlist

        Args:
            user_id: ID del usuario
            playlist_id: ID de la playlist

        Returns:
            Diccionario con datos de la playlist o None
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT search_data FROM user_search_cache
                WHERE user_id = ? AND search_type = ?
            """, (user_id, f"playlist_{playlist_id}"))

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo artistas de playlist: {e}")
            return None
        finally:
            conn.close()

    def save_pending_playlists(self, user_id: int, playlists_data: List[Dict]) -> bool:
        """
        Guarda playlists pendientes del usuario

        Args:
            user_id: ID del usuario
            playlists_data: Lista de playlists

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR REPLACE INTO user_search_cache
                (user_id, search_type, search_data, created_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            """, (user_id, "user_playlists", json.dumps(playlists_data)))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error guardando playlists: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_pending_playlists(self, user_id: int) -> Optional[List[Dict]]:
        """
        Obtiene playlists pendientes del usuario

        Args:
            user_id: ID del usuario

        Returns:
            Lista de playlists o None
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT search_data FROM user_search_cache
                WHERE user_id = ? AND search_type = 'user_playlists'
            """, (user_id,))

            row = cursor.fetchone()
            if row:
                return json.loads(row[0])
            return None

        except (sqlite3.Error, json.JSONDecodeError) as e:
            logger.error(f"Error obteniendo playlists: {e}")
            return None
        finally:
            conn.close()



    def init_muspy_tables(self):
        """Inicializa las tablas específicas de Muspy"""
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Verificar si las nuevas columnas de Muspy existen en users
            cursor.execute("PRAGMA table_info(users)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'muspy_email' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN muspy_email TEXT")
                logger.info("Columna muspy_email añadida a users")

            if 'muspy_password' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN muspy_password TEXT")
                logger.info("Columna muspy_password añadida a users")

            if 'muspy_userid' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN muspy_userid TEXT")
                logger.info("Columna muspy_userid añadida a users")

            # Verificar si la columna muspy existe en user_followed_artists
            cursor.execute("PRAGMA table_info(user_followed_artists)")
            columns = [col[1] for col in cursor.fetchall()]

            if 'muspy' not in columns:
                cursor.execute("ALTER TABLE user_followed_artists ADD COLUMN muspy BOOLEAN DEFAULT 0")
                logger.info("Columna muspy añadida a user_followed_artists")

            # Crear tabla para artistas de Muspy si no existe
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS muspy_artists_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    artist_mbid TEXT NOT NULL,
                    artist_name TEXT NOT NULL,
                    disambiguation TEXT DEFAULT '',
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    UNIQUE(user_id, artist_mbid)
                )
            """)

            # Índices para optimización
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_muspy_cache_user ON muspy_artists_cache(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_muspy_cache_mbid ON muspy_artists_cache(artist_mbid)")

            conn.commit()
            logger.info("Tablas de Muspy inicializadas correctamente")

        except sqlite3.Error as e:
            logger.error(f"Error al inicializar tablas de Muspy: {e}")
            conn.rollback()
        finally:
            conn.close()

    # ======================
    # FUNCIONES DE MUSPY
    # ======================

    def save_muspy_credentials(self, user_id: int, email: str, password: str, userid: str) -> bool:
        """
        Guarda las credenciales de Muspy para un usuario

        Args:
            user_id: ID del usuario
            email: Email de Muspy
            password: Contraseña de Muspy
            userid: User ID de Muspy

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE users
                SET muspy_email = ?, muspy_password = ?, muspy_userid = ?
                WHERE id = ?
            """, (email, password, userid, user_id))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error guardando credenciales Muspy: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_muspy_credentials(self, user_id: int) -> Optional[Tuple[str, str, str]]:
        """
        Obtiene las credenciales de Muspy de un usuario

        Args:
            user_id: ID del usuario

        Returns:
            Tupla (email, password, userid) o None si no existen
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT muspy_email, muspy_password, muspy_userid
                FROM users WHERE id = ?
            """, (user_id,))

            result = cursor.fetchone()

            if result and all(result):
                return result
            return None

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo credenciales Muspy: {e}")
            return None
        finally:
            conn.close()

    def update_muspy_status_for_artists(self, user_id: int, artist_ids: List[int], muspy_status: bool) -> bool:
        """
        Actualiza el estado de muspy para una lista de artistas

        Args:
            user_id: ID del usuario
            artist_ids: Lista de IDs de artistas
            muspy_status: True/False para estado de Muspy

        Returns:
            True si se actualizó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            for artist_id in artist_ids:
                cursor.execute("""
                    UPDATE user_followed_artists
                    SET muspy = ?
                    WHERE user_id = ? AND artist_id = ?
                """, (muspy_status, user_id, artist_id))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error actualizando estado Muspy: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_followed_artists_muspy_status(self, user_id: int, muspy_only: bool = False) -> List[Dict]:
        """
        Obtiene artistas seguidos con información de estado Muspy

        Args:
            user_id: ID del usuario
            muspy_only: Si True, solo artistas marcados como de Muspy

        Returns:
            Lista de artistas con estado Muspy
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            query = """
                SELECT a.*, ufa.followed_at, ufa.muspy
                FROM artists a
                JOIN user_followed_artists ufa ON a.id = ufa.artist_id
                WHERE ufa.user_id = ?
            """

            if muspy_only:
                query += " AND ufa.muspy = 1"

            query += " ORDER BY ufa.followed_at DESC"

            cursor.execute(query, (user_id,))
            rows = cursor.fetchall()

            artists = []
            for row in rows:
                artist_dict = dict(row)
                artist_dict['muspy'] = bool(artist_dict['muspy'])
                artists.append(artist_dict)

            return artists

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo artistas con estado Muspy: {e}")
            return []
        finally:
            conn.close()

    def add_user_followed_artist_muspy(self, user_id: int, artist_id: int, muspy: bool = False) -> bool:
        """
        Añade un artista seguido con estado de Muspy

        Args:
            user_id: ID del usuario
            artist_id: ID del artista
            muspy: Estado de Muspy

        Returns:
            True si se añadió o ya existía
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Verificar si ya existe la relación
            cursor.execute("""
                SELECT id FROM user_followed_artists
                WHERE user_id = ? AND artist_id = ?
            """, (user_id, artist_id))

            if cursor.fetchone():
                # Actualizar el estado de muspy si es necesario
                if muspy:
                    cursor.execute("""
                        UPDATE user_followed_artists SET muspy = 1
                        WHERE user_id = ? AND artist_id = ?
                    """, (user_id, artist_id))
            else:
                # Crear nueva relación
                cursor.execute("""
                    INSERT INTO user_followed_artists (user_id, artist_id, muspy)
                    VALUES (?, ?, ?)
                """, (user_id, artist_id, muspy))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error añadiendo artista con estado Muspy: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def cache_muspy_artists(self, user_id: int, muspy_artists: List[Dict]) -> bool:
        """
        Almacena en caché los artistas de Muspy para un usuario

        Args:
            user_id: ID del usuario
            muspy_artists: Lista de artistas de Muspy

        Returns:
            True si se guardó correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            # Limpiar caché anterior
            cursor.execute("DELETE FROM muspy_artists_cache WHERE user_id = ?", (user_id,))

            # Insertar nuevos artistas
            for artist in muspy_artists:
                cursor.execute("""
                    INSERT INTO muspy_artists_cache
                    (user_id, artist_mbid, artist_name, disambiguation)
                    VALUES (?, ?, ?, ?)
                """, (
                    user_id,
                    artist.get('mbid', ''),
                    artist.get('name', ''),
                    artist.get('disambiguation', '')
                ))

            conn.commit()
            return True

        except sqlite3.Error as e:
            logger.error(f"Error guardando caché de Muspy: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_cached_muspy_artists(self, user_id: int) -> List[Dict]:
        """
        Obtiene artistas de Muspy desde el caché

        Args:
            user_id: ID del usuario

        Returns:
            Lista de artistas desde caché
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT artist_mbid, artist_name, disambiguation, last_updated
                FROM muspy_artists_cache
                WHERE user_id = ?
                ORDER BY artist_name
            """, (user_id,))

            rows = cursor.fetchall()
            return [dict(row) for row in rows]

        except sqlite3.Error as e:
            logger.error(f"Error obteniendo caché de Muspy: {e}")
            return []
        finally:
            conn.close()

    def clear_muspy_credentials(self, user_id: int) -> bool:
        """
        Limpia las credenciales de Muspy de un usuario

        Args:
            user_id: ID del usuario

        Returns:
            True si se limpió correctamente
        """
        conn = self.get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                UPDATE users
                SET muspy_email = NULL, muspy_password = NULL, muspy_userid = NULL
                WHERE id = ?
            """, (user_id,))

            # También limpiar caché
            cursor.execute("DELETE FROM muspy_artists_cache WHERE user_id = ?", (user_id,))

            conn.commit()
            return cursor.rowcount > 0

        except sqlite3.Error as e:
            logger.error(f"Error limpiando credenciales Muspy: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

# clase para multihilos


class DatabaseConcurrentWrapper:
    """
    Wrapper que proporciona conexiones de base de datos thread-safe
    COMPATIBLE con código existente que espera get_connection() directo
    """

    def __init__(self, db_instance):
        self.db_instance = db_instance
        self.db_path = db_instance.db_path
        self._local = threading.local()
        self._lock = threading.Lock()

    def _get_thread_connection(self):
        """Obtiene una conexión thread-local"""
        if not hasattr(self._local, 'connection') or self._local.connection is None:
            self._local.connection = sqlite3.connect(
                self.db_path,
                timeout=30.0,  # Timeout más largo
                check_same_thread=False  # Permitir uso entre threads
            )
            self._local.connection.row_factory = sqlite3.Row
            # Configurar conexión para mejor concurrencia
            self._local.connection.execute("PRAGMA journal_mode=WAL")
            self._local.connection.execute("PRAGMA synchronous=NORMAL")
            self._local.connection.execute("PRAGMA cache_size=10000")
            self._local.connection.execute("PRAGMA temp_store=MEMORY")
        return self._local.connection

    def get_connection(self):
        """
        MÉTODO COMPATIBLE: Devuelve una conexión thread-safe
        que se comporta como la original
        """
        return ThreadSafeConnection(self._get_thread_connection())

    @contextmanager
    def get_connection_context(self):
        """Context manager para operaciones más complejas"""
        conn = self._get_thread_connection()
        try:
            yield conn
        finally:
            # NO cerrar la conexión aquí, solo commitear
            try:
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

    def save_concert(self, concert_data):
        """Guarda un concierto de forma thread-safe"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()

                # Verificar si ya existe
                cursor.execute("""
                    SELECT id FROM concerts
                    WHERE artist_name = ? AND venue = ? AND city = ? AND date = ?
                """, (
                    concert_data.get('artist_name', ''),
                    concert_data.get('venue', ''),
                    concert_data.get('city', ''),
                    concert_data.get('date', '')
                ))

                if cursor.fetchone():
                    return  # Ya existe

                # Insertar nuevo concierto
                cursor.execute("""
                    INSERT INTO concerts (
                        artist_name, name, venue, city, country, country_code,
                        date, time, url, source, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """, (
                    concert_data.get('artist_name', ''),
                    concert_data.get('name', ''),
                    concert_data.get('venue', ''),
                    concert_data.get('city', ''),
                    concert_data.get('country', ''),
                    concert_data.get('country_code', ''),
                    concert_data.get('date', ''),
                    concert_data.get('time', ''),
                    concert_data.get('url', ''),
                    concert_data.get('source', '')
                ))

        except Exception as e:
            logger.error(f"Error guardando concierto thread-safe: {e}")

    def close_thread_connections(self):
        """Cierra las conexiones thread-local"""
        if hasattr(self._local, 'connection') and self._local.connection:
            try:
                self._local.connection.close()
                self._local.connection = None
            except Exception as e:
                logger.error(f"Error cerrando conexión thread-local: {e}")

    def close_pool(self):
        """Cierra todas las conexiones del pool"""
        self.close_thread_connections()

    # Delegar otros métodos al objeto original
    def __getattr__(self, name):
        return getattr(self.db_instance, name)


class ThreadSafeConnection:
    """
    Wrapper para conexiones SQLite que proporciona auto-commit y manejo de errores
    COMPATIBLE con código existente
    """

    def __init__(self, connection):
        self._connection = connection
        self._closed = False

    def cursor(self):
        """Devuelve un cursor thread-safe"""
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
        return ThreadSafeCursor(self._connection.cursor(), self._connection)

    def execute(self, sql, parameters=None):
        """Ejecuta SQL directamente"""
        if self._closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")

        if parameters:
            return self._connection.execute(sql, parameters)
        else:
            return self._connection.execute(sql)

    def commit(self):
        """Commit de transacción"""
        if not self._closed:
            self._connection.commit()

    def rollback(self):
        """Rollback de transacción"""
        if not self._closed:
            self._connection.rollback()

    def close(self):
        """Marca como cerrada pero NO cierra la conexión real (thread-local se mantiene)"""
        self._closed = True
        # NO cerrar self._connection porque es compartida por el thread
        # Solo hacer commit de cambios pendientes
        try:
            if not self._closed:
                self._connection.commit()
        except Exception as e:
            logger.debug(f"Error en commit al cerrar: {e}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        self.close()

    # Delegar otros métodos a la conexión real
    def __getattr__(self, name):
        return getattr(self._connection, name)


class ThreadSafeCursor:
    """
    Wrapper para cursors SQLite con auto-commit
    """

    def __init__(self, cursor, connection):
        self._cursor = cursor
        self._connection = connection

    def execute(self, sql, parameters=None):
        """Ejecuta SQL y hace auto-commit para operaciones de escritura"""
        try:
            if parameters:
                result = self._cursor.execute(sql, parameters)
            else:
                result = self._cursor.execute(sql)

            # Auto-commit para operaciones de escritura
            sql_upper = sql.strip().upper()
            if any(sql_upper.startswith(op) for op in ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'ALTER', 'DROP']):
                self._connection.commit()

            return result
        except Exception as e:
            self._connection.rollback()
            raise e

    def executemany(self, sql, parameters):
        """Ejecuta SQL múltiple con auto-commit"""
        try:
            result = self._cursor.executemany(sql, parameters)
            self._connection.commit()
            return result
        except Exception as e:
            self._connection.rollback()
            raise e

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def fetchmany(self, size=None):
        if size is None:
            return self._cursor.fetchmany()
        return self._cursor.fetchmany(size)

    # Delegar otros métodos al cursor real
    def __getattr__(self, name):
        return getattr(self._cursor, name)



# Función para actualizar la BD global en main()
def upgrade_database_for_concurrency(db_instance):
    """Wrappea la instancia de BD existente para concurrencia"""
    return DatabaseConcurrentWrapper(db_instance)
