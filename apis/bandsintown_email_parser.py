"""Extrae los datos de un concierto a partir del HTML de un correo de
notificación de Bandsintown ("sigues a {artista}... hay una fecha nueva").

El layout de Bandsintown ancla la fecha en un <img alt="Cal-Icon"> seguido
de dos <p> (fecha, hora) y, si el evento tiene recinto físico, un
<img alt="Pin-Icon"> seguido de otros dos <p> (recinto, ciudad+país). Los
correos de "Just Announced"/livestream no llevan Pin-Icon — en ese caso se
usa la etiqueta "Event:" como recinto y no hay ciudad/país.

Los correos que incluyen una sección "You might also like"/"Promoted" con
conciertos de OTROS artistas (mismo markup de Cal-Icon) se recortan antes de
parsear, para no confundirlos con el evento principal.
"""

import re

from bs4 import BeautifulSoup

_TICKET_LINK_PRIORITY = ["Get Tickets", "Tickets", "More Info", "RSVP", "Remind Me"]


def parse_bandsintown_email(html: str) -> dict | None:
    cut = html.find("You might also like")
    if cut == -1:
        cut = html.find("Promoted")
    if cut != -1:
        html = html[:cut]

    soup = BeautifulSoup(html, "html.parser")

    artist = None
    footer = soup.find(string=re.compile(r"you.re following", re.IGNORECASE))
    if footer:
        m = re.search(r"following\s+(.+?)\s*$", footer.strip())
        if m:
            artist = m.group(1).strip()
    if not artist:
        h1 = soup.find("h1")
        if h1:
            artist = h1.get_text(strip=True)

    cal = soup.find("img", alt="Cal-Icon")
    if cal is None:
        return None

    # Los <p> de fecha/hora (o recinto/ciudad) viven en el <td> que sigue al
    # icono, NUNCA se buscan con find_next("p") suelto: si a un evento le
    # falta la hora (o la ciudad), ese find_next cruzaría al siguiente
    # bloque del correo (el recinto, o un concierto "Promoted" ajeno) y
    # devolvería basura en vez de None.
    date_td = cal.find_next("td")
    date_ps = [p.get_text(strip=True) for p in date_td.find_all("p")] if date_td else []
    date_str = date_ps[0] if date_ps else None
    time_str = None
    if len(date_ps) > 1:
        candidate = date_ps[1]
        if re.match(r"^\d{1,2}:\d{2}\s*[AP]M$", candidate, re.IGNORECASE):
            time_str = candidate
        else:
            # No es una hora real (p.ej. año suelto "2026" en eventos de
            # varios días tipo "Jun 12-13") -- se pliega en la fecha.
            date_str = f"{date_str} {candidate}"

    pin = soup.find("img", alt="Pin-Icon")
    venue = city_country = None
    is_livestream = pin is None
    if pin is not None:
        loc_td = pin.find_next("td")
        loc_ps = [p.get_text(strip=True) for p in loc_td.find_all("p")] if loc_td else []
        venue = loc_ps[0] if loc_ps else None
        city_country = loc_ps[1] if len(loc_ps) > 1 else None
    else:
        strings = list(soup.stripped_strings)
        if "Event:" in strings:
            i = strings.index("Event:")
            if i + 1 < len(strings):
                venue = strings[i + 1]

    ticket_url = None
    for label in _TICKET_LINK_PRIORITY:
        a = soup.find("a", string=lambda s: s and s.strip() == label, href=True)
        if a:
            ticket_url = a["href"]
            break

    if not artist or not date_str:
        return None

    city, country = (None, None)
    if city_country and "," in city_country:
        city, country = (p.strip() for p in city_country.split(",", 1))
    elif city_country:
        city = city_country.strip()

    return {
        "artist": artist,
        "raw_date": date_str,
        "raw_time": time_str,
        "venue": venue,
        "city": city,
        "country": country,
        "is_livestream": is_livestream,
        "url": ticket_url,
        "source": "bandsintown",
    }
