import requests
import json
import sqlite3
import logging
from typing import List, Dict, Optional, Set
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class CountryCityService:
    """Servicio para gestionar países y ciudades usando la API countrystatecity.in"""

    def __init__(self, api_key: str, db_path: str):
        self.api_key = api_key
        self.db_path = db_path
        self.base_url = "https://api.countrystatecity.in/v1"
        self.headers = {
            "X-CSCAPI-KEY": api_key,
            "Content-Type": "application/json"
        }

        # Inicializar tablas de la base de datos
        self._init_database()

    def _init_database(self):
        """Inicializa las tablas necesarias para países y ciudades"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Tabla de países disponibles
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS countries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    phone_code TEXT,
                    currency TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Tabla de ciudades por país
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cities (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code TEXT NOT NULL,
                    state_code TEXT,
                    state_name TEXT,
                    name TEXT NOT NULL,
                    latitude REAL,
                    longitude REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (country_code) REFERENCES countries (code),
                    UNIQUE(country_code, name)
                )
            """)

            # Tabla de países configurados por usuario (múltiples países)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_countries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    country_code TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
                    FOREIGN KEY (country_code) REFERENCES countries (code),
                    UNIQUE(user_id, country_code)
                )
            """)

            # Índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cities_country ON cities(country_code)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_cities_name ON cities(name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_countries_user ON user_countries(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_countries_country ON user_countries(country_code)")

            conn.commit()
            logger.info("✅ Tablas de países y ciudades inicializadas")

        except sqlite3.Error as e:
            logger.error(f"❌ Error inicializando tablas: {e}")
            conn.rollback()
        finally:
            conn.close()

    def get_available_countries(self, force_refresh: bool = False) -> List[Dict]:
        """
        Obtiene lista de países disponibles desde la API

        Args:
            force_refresh: Si True, fuerza actualización desde API

        Returns:
            Lista de países con código, nombre, etc.
        """
        # Verificar caché primero
        if not force_refresh:
            cached_countries = self._get_cached_countries()
            if cached_countries:
                logger.info(f"🔄 Usando {len(cached_countries)} países del caché")
                return cached_countries

        logger.info("🌍 Obteniendo países desde API countrystatecity.in...")

        try:
            response = requests.get(
                f"{self.base_url}/countries",
                headers=self.headers,
                timeout=10
            )
            response.raise_for_status()

            countries = response.json()

            if countries:
                # Guardar en base de datos
                self._save_countries_to_db(countries)
                logger.info(f"✅ {len(countries)} países obtenidos y guardados")
                return countries
            else:
                logger.warning("⚠️ No se obtuvieron países de la API")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error obteniendo países: {e}")
            # Retornar caché como fallback
            return self._get_cached_countries()

    def get_country_cities(self, country_code: str, force_refresh: bool = False) -> List[Dict]:
        """
        Obtiene ciudades de un país específico

        Args:
            country_code: Código ISO del país (ej: ES, US, FR)
            force_refresh: Si True, fuerza actualización desde API

        Returns:
            Lista de ciudades del país
        """
        country_code = country_code.upper()

        # Verificar caché primero
        if not force_refresh:
            cached_cities = self._get_cached_cities(country_code)
            if cached_cities:
                logger.info(f"🔄 Usando {len(cached_cities)} ciudades del caché para {country_code}")
                return cached_cities

        logger.info(f"🏙️ Obteniendo ciudades de {country_code} desde API...")

        try:
            response = requests.get(
                f"{self.base_url}/countries/{country_code}/cities",
                headers=self.headers,
                timeout=30  # Timeout más largo para ciudades
            )
            response.raise_for_status()

            cities = response.json()

            if cities:
                # Guardar en base de datos
                self._save_cities_to_db(country_code, cities)
                logger.info(f"✅ {len(cities)} ciudades obtenidas y guardadas para {country_code}")
                return cities
            else:
                logger.warning(f"⚠️ No se obtuvieron ciudades para {country_code}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error obteniendo ciudades de {country_code}: {e}")
            # Retornar caché como fallback
            return self._get_cached_cities(country_code)

    def add_user_country(self, user_id: int, country_code: str) -> bool:
        """
        Añade un país a la configuración del usuario

        Args:
            user_id: ID del usuario
            country_code: Código del país a añadir

        Returns:
            True si se añadió correctamente
        """
        country_code = country_code.upper()

        # Verificar que el país existe
        if not self._country_exists(country_code):
            # Intentar obtener países actualizados
            countries = self.get_available_countries(force_refresh=True)
            if not any(c.get('iso2') == country_code for c in countries):
                logger.error(f"❌ País {country_code} no encontrado")
                return False

        # Obtener ciudades del país
        cities = self.get_country_cities(country_code)
        if not cities:
            logger.warning(f"⚠️ No se encontraron ciudades para {country_code}")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT OR IGNORE INTO user_countries (user_id, country_code)
                VALUES (?, ?)
            """, (user_id, country_code))

            was_added = cursor.rowcount > 0
            conn.commit()

            if was_added:
                logger.info(f"✅ País {country_code} añadido para usuario {user_id}")
            else:
                logger.info(f"ℹ️ Usuario {user_id} ya tenía el país {country_code}")

            return True

        except sqlite3.Error as e:
            logger.error(f"❌ Error añadiendo país: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def remove_user_country(self, user_id: int, country_code: str) -> bool:
        """
        Elimina un país de la configuración del usuario

        Args:
            user_id: ID del usuario
            country_code: Código del país a eliminar

        Returns:
            True si se eliminó correctamente
        """
        country_code = country_code.upper()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                DELETE FROM user_countries
                WHERE user_id = ? AND country_code = ?
            """, (user_id, country_code))

            was_removed = cursor.rowcount > 0
            conn.commit()

            if was_removed:
                logger.info(f"✅ País {country_code} eliminado para usuario {user_id}")
            else:
                logger.info(f"ℹ️ Usuario {user_id} no tenía el país {country_code}")

            return was_removed

        except sqlite3.Error as e:
            logger.error(f"❌ Error eliminando país: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_user_countries(self, user_id: int) -> List[Dict]:
        """
        Obtiene países configurados para un usuario

        Args:
            user_id: ID del usuario

        Returns:
            Lista de países del usuario con información completa
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT c.code, c.name, c.phone_code, c.currency, uc.added_at
                FROM user_countries uc
                JOIN countries c ON uc.country_code = c.code
                WHERE uc.user_id = ?
                ORDER BY uc.added_at ASC
            """, (user_id,))

            rows = cursor.fetchall()
            countries = []

            for row in rows:
                countries.append({
                    'code': row[0],
                    'name': row[1],
                    'phone_code': row[2],
                    'currency': row[3],
                    'added_at': row[4]
                })

            return countries

        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo países del usuario: {e}")
            return []
        finally:
            conn.close()

    def get_user_country_codes(self, user_id: int) -> Set[str]:
        """
        Obtiene códigos de países del usuario (para filtros rápidos)

        Args:
            user_id: ID del usuario

        Returns:
            Set con códigos de países
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT country_code FROM user_countries WHERE user_id = ?
            """, (user_id,))

            rows = cursor.fetchall()
            return {row[0] for row in rows}

        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo códigos de países: {e}")
            return set()
        finally:
            conn.close()

    def find_city_country(self, city_name: str, user_countries: Set[str] = None) -> Optional[str]:
        """
        Encuentra el país de una ciudad, priorizando países del usuario
        VERSIÓN MEJORADA: Filtro más estricto para evitar falsos positivos

        Args:
            city_name: Nombre de la ciudad a buscar
            user_countries: Set de códigos de países del usuario

        Returns:
            Código del país si se encuentra, None en caso contrario
        """
        if not city_name:
            return None

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            city_clean = city_name.strip()

            # 1. BÚSQUEDA EXACTA (más estricta)
            cursor.execute("""
                SELECT country_code FROM cities
                WHERE LOWER(name) = LOWER(?)
                ORDER BY country_code
            """, (city_clean,))

            exact_matches = [row[0] for row in cursor.fetchall()]

            if exact_matches:
                # Si hay coincidencia exacta, priorizar países del usuario
                if user_countries:
                    for country in exact_matches:
                        if country in user_countries:
                            logger.info(f"🎯 Ciudad '{city_name}' encontrada exacta en país preferido: {country}")
                            return country

                # Si no hay match con países del usuario, retornar el primero
                logger.info(f"🌍 Ciudad '{city_name}' encontrada exacta en: {exact_matches[0]}")
                return exact_matches[0]

            # 2. BÚSQUEDA CON VARIACIONES COMUNES (más controlada)
            # Solo si no hay coincidencia exacta y la ciudad tiene más de 3 caracteres
            if len(city_clean) > 3:
                variations = self._generate_city_variations(city_clean)

                for variation in variations:
                    cursor.execute("""
                        SELECT country_code FROM cities
                        WHERE LOWER(name) = LOWER(?)
                        ORDER BY country_code
                    """, (variation,))

                    var_matches = [row[0] for row in cursor.fetchall()]

                    if var_matches:
                        # Priorizar países del usuario
                        if user_countries:
                            for country in var_matches:
                                if country in user_countries:
                                    logger.info(f"🎯 Ciudad '{city_name}' (variación '{variation}') encontrada en país preferido: {country}")
                                    return country

                        logger.info(f"🌍 Ciudad '{city_name}' (variación '{variation}') encontrada en: {var_matches[0]}")
                        return var_matches[0]

            # 3. BÚSQUEDA PARCIAL MUY RESTRICTIVA (solo como último recurso)
            # Solo para ciudades largas y con condiciones muy estrictas
            if len(city_clean) >= 6:
                # Buscar solo si la ciudad consultada es substancialmente similar
                cursor.execute("""
                    SELECT country_code, name FROM cities
                    WHERE LOWER(name) LIKE LOWER(?)
                    AND LENGTH(name) BETWEEN ? AND ?
                    ORDER BY LENGTH(name), country_code
                """, (f"{city_clean}%", len(city_clean), len(city_clean) + 3))

                partial_matches = cursor.fetchall()

                # Filtrar para evitar casos como "Rome" -> "Romeral"
                filtered_matches = []
                for country_code, db_city_name in partial_matches:
                    # Solo aceptar si:
                    # 1. La ciudad de BD empieza con la ciudad consultada
                    # 2. La diferencia de longitud no es excesiva
                    # 3. No hay caracteres raros en la diferencia
                    if (db_city_name.lower().startswith(city_clean.lower()) and
                        len(db_city_name) - len(city_clean) <= 3 and
                        self._is_valid_city_extension(city_clean, db_city_name)):
                        filtered_matches.append((country_code, db_city_name))

                if filtered_matches:
                    # Priorizar países del usuario
                    if user_countries:
                        for country_code, db_city_name in filtered_matches:
                            if country_code in user_countries:
                                logger.info(f"🎯 Ciudad '{city_name}' (parcial '{db_city_name}') encontrada en país preferido: {country_code}")
                                return country_code

                    # Si no hay match con países del usuario, retornar el primero
                    country_code, db_city_name = filtered_matches[0]
                    logger.info(f"🌍 Ciudad '{city_name}' (parcial '{db_city_name}') encontrada en: {country_code}")
                    return country_code

            logger.info(f"❓ Ciudad '{city_name}' no encontrada en base de datos")
            return None

        except sqlite3.Error as e:
            logger.error(f"❌ Error buscando ciudad: {e}")
            return None
        finally:
            conn.close()

    def _generate_city_variations(self, city_name: str) -> List[str]:
        """
        Genera variaciones comunes de nombres de ciudades

        Args:
            city_name: Nombre original de la ciudad

        Returns:
            Lista de variaciones posibles
        """
        variations = []
        city_lower = city_name.lower()

        # Variaciones comunes de acentos y caracteres especiales
        accent_replacements = {
            'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ã': 'a',
            'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e',
            'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i',
            'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'õ': 'o',
            'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u',
            'ñ': 'n', 'ç': 'c'
        }

        # Crear versión sin acentos
        no_accents = city_lower
        for accented, plain in accent_replacements.items():
            no_accents = no_accents.replace(accented, plain)

        if no_accents != city_lower:
            variations.append(no_accents)

        # Crear versión con acentos (reverso)
        reverse_replacements = {v: k for k, v in accent_replacements.items()}
        with_accents = city_lower
        for plain, accented in reverse_replacements.items():
            with_accents = with_accents.replace(plain, accented)

        if with_accents != city_lower and with_accents not in variations:
            variations.append(with_accents)

        # Variaciones específicas comunes
        common_variations = {
            'saint': ['st', 'san', 'santa'],
            'st': ['saint', 'san', 'santa'],
            'san': ['saint', 'st', 'santa'],
            'santa': ['saint', 'st', 'san'],
            'mount': ['mt', 'monte'],
            'mt': ['mount', 'monte'],
            'monte': ['mount', 'mt']
        }

        for original, alternatives in common_variations.items():
            if original in city_lower:
                for alt in alternatives:
                    variation = city_lower.replace(original, alt)
                    if variation not in variations:
                        variations.append(variation)

        return variations[:5]  # Limitar a 5 variaciones máximo


    def _is_valid_city_extension(self, query_city: str, db_city: str) -> bool:
        """
        Verifica si la extensión de una ciudad es válida
        Evita casos como "Rome" -> "Romeral"

        Args:
            query_city: Ciudad consultada
            db_city: Ciudad en base de datos

        Returns:
            True si la extensión es válida
        """
        if len(db_city) <= len(query_city):
            return True

        extension = db_city[len(query_city):].lower()

        # Extensiones válidas (sufijos comunes de ciudades)
        valid_extensions = [
            ' city', ' town', ' beach', ' hill', ' park', ' valley',
            ' springs', ' falls', ' lake', ' river', ' bay', ' port',
            'ville', 'burg', 'ton', 'ham', 'ford', 'field', 'wood',
            'land', 'stead', 'worth', 'borough', 'wich', 'thorpe',
            'by', 'stad', 'borg', 'havn', 'heim', 'dal', 'vik',
            'a', 'o', 'i', 'e', 'u',  # Vocales sueltas
            's', 'n', 't', 'r', 'l',  # Consonantes comunes al final
            'es', 'os', 'as', 'is',   # Plurales simples
            'ino', 'ina', 'ito', 'ita'  # Diminutivos
        ]

        # Si la extensión es muy corta (1-3 caracteres), probablemente es válida
        if len(extension) <= 3:
            return True

        # Verificar si la extensión contiene algún sufijo válido
        for valid_ext in valid_extensions:
            if extension.startswith(valid_ext) or extension.endswith(valid_ext):
                return True

        # Si la extensión es completamente diferente (como "al" en "Romeral"), rechazar
        if len(extension) > 3 and not any(c in 'aeiou' for c in extension):
            return False

        return True


    def get_country_info(self, country_code: str) -> Optional[Dict]:
        """
        Obtiene información completa de un país

        Args:
            country_code: Código del país

        Returns:
            Diccionario con información del país o None
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT code, name, phone_code, currency
                FROM countries WHERE code = ?
            """, (country_code.upper(),))

            row = cursor.fetchone()
            if row:
                return {
                    'code': row[0],
                    'name': row[1],
                    'phone_code': row[2],
                    'currency': row[3]
                }
            return None

        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo info del país: {e}")
            return None
        finally:
            conn.close()

    def search_countries(self, query: str) -> List[Dict]:
        """
        Busca países por nombre o código

        Args:
            query: Texto a buscar

        Returns:
            Lista de países que coinciden
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT code, name, phone_code, currency
                FROM countries
                WHERE LOWER(name) LIKE LOWER(?) OR LOWER(code) LIKE LOWER(?)
                ORDER BY name
            """, (f"%{query}%", f"%{query}%"))

            rows = cursor.fetchall()
            countries = []

            for row in rows:
                countries.append({
                    'code': row[0],
                    'name': row[1],
                    'phone_code': row[2],
                    'currency': row[3]
                })

            return countries

        except sqlite3.Error as e:
            logger.error(f"❌ Error buscando países: {e}")
            return []
        finally:
            conn.close()

    def _get_cached_countries(self) -> List[Dict]:
        """Obtiene países del caché de la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            # Verificar si los datos son recientes (menos de 7 días)
            cursor.execute("""
                SELECT COUNT(*) FROM countries
                WHERE updated_at > datetime('now', '-7 days')
            """)

            if cursor.fetchone()[0] == 0:
                return []

            cursor.execute("""
                SELECT code, name, phone_code, currency
                FROM countries ORDER BY name
            """)

            rows = cursor.fetchall()
            countries = []

            for row in rows:
                countries.append({
                    'iso2': row[0],  # Formato compatible con API
                    'name': row[1],
                    'phone_code': row[2],
                    'currency': row[3]
                })

            return countries

        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo países del caché: {e}")
            return []
        finally:
            conn.close()

    def _get_cached_cities(self, country_code: str) -> List[Dict]:
        """Obtiene ciudades del caché de la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("""
                SELECT name, state_name, latitude, longitude
                FROM cities
                WHERE country_code = ?
                ORDER BY name
            """, (country_code,))

            rows = cursor.fetchall()
            cities = []

            for row in rows:
                cities.append({
                    'name': row[0],
                    'state_name': row[1],
                    'latitude': row[2],
                    'longitude': row[3]
                })

            return cities

        except sqlite3.Error as e:
            logger.error(f"❌ Error obteniendo ciudades del caché: {e}")
            return []
        finally:
            conn.close()

    def _save_countries_to_db(self, countries: List[Dict]):
        """Guarda países en la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for country in countries:
                cursor.execute("""
                    INSERT OR REPLACE INTO countries
                    (code, name, phone_code, currency, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (
                    country.get('iso2', ''),
                    country.get('name', ''),
                    country.get('phonecode', ''),
                    country.get('currency', '')
                ))

            conn.commit()
            logger.info(f"✅ {len(countries)} países guardados en BD")

        except sqlite3.Error as e:
            logger.error(f"❌ Error guardando países: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _save_cities_to_db(self, country_code: str, cities: List[Dict]):
        """Guarda ciudades en la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            for city in cities:
                cursor.execute("""
                    INSERT OR REPLACE INTO cities
                    (country_code, state_code, state_name, name, latitude, longitude)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    country_code,
                    city.get('state_code', ''),
                    city.get('state_name', ''),
                    city.get('name', ''),
                    city.get('latitude'),
                    city.get('longitude')
                ))

            conn.commit()
            logger.info(f"✅ {len(cities)} ciudades guardadas para {country_code}")

        except sqlite3.Error as e:
            logger.error(f"❌ Error guardando ciudades: {e}")
            conn.rollback()
        finally:
            conn.close()

    def _country_exists(self, country_code: str) -> bool:
        """Verifica si un país existe en la base de datos"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT 1 FROM countries WHERE code = ?", (country_code,))
            return cursor.fetchone() is not None
        except sqlite3.Error:
            return False
        finally:
            conn.close()


# Integración con ArtistTrackerDatabase
class ArtistTrackerDatabaseExtended:
    """Extensión de ArtistTrackerDatabase con funcionalidad de países múltiples"""

    def __init__(self, db_path: str, country_city_service: CountryCityService = None):
        self.db_path = db_path
        self.country_city_service = country_city_service

    def get_user_countries_legacy(self, user_id: int) -> str:
        """
        Obtiene el país legacy del usuario (compatibilidad)

        Returns:
            Código del primer país configurado o 'ES' por defecto
        """
        if not self.country_city_service:
            return 'ES'

        countries = self.country_city_service.get_user_country_codes(user_id)
        return list(countries)[0] if countries else 'ES'

    def get_user_countries_set(self, user_id: int) -> Set[str]:
        """
        Obtiene set de países del usuario para filtros

        Returns:
            Set con códigos de países del usuario
        """
        if not self.country_city_service:
            return {'ES'}

        return self.country_city_service.get_user_country_codes(user_id) or {'ES'}

    def filter_concerts_by_countries(self, concerts: List[Dict], user_countries: Set[str]) -> List[Dict]:
        """
        Filtra conciertos según países del usuario
        VERSIÓN MEJORADA: Mejor manejo de países en conciertos de Ticketmaster
        """
        if not user_countries or not self.country_city_service:
            return concerts

        filtered_concerts = []
        user_countries_upper = {c.upper() for c in user_countries}

        for concert in concerts:
            concert_country = (concert.get('country') or '').upper()
            concert_country_code = (concert.get('country_code') or '').upper()

            # Coincidencia directa: nombre de país o código ISO
            if (concert_country and concert_country in user_countries_upper) or \
               (concert_country_code and concert_country_code in user_countries_upper):
                filtered_concerts.append(concert)
                continue

            # Intentar detectar país por ciudad si no hay coincidencia directa
            city = concert.get('city', '')
            if city:
                detected_country = self.country_city_service.find_city_country(city, user_countries_upper)
                if detected_country:
                    concert['country'] = detected_country
                    if detected_country.upper() in user_countries_upper:
                        filtered_concerts.append(concert)
                        continue

            # Sin país identificable: incluir (mejor mostrar de más que de menos)
            if not concert_country and not concert_country_code:
                filtered_concerts.append(concert)

        logger.info(f"Filtrado de conciertos: {len(concerts)} -> {len(filtered_concerts)} para países {user_countries}")
        return filtered_concerts
