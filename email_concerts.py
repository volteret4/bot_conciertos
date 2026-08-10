#!/usr/bin/env python3
"""Revisa el correo de disroot en busca de correos de notificación de
conciertos (por ahora, solo Bandsintown — Dice.fm y Resident Advisor no
envían correos a este buzón todavía, ver CLAUDE.md) y por cada uno:

  1. Envía un aviso por Telegram (bot admin del propio bot_conciertos).
  2. Lo sube al calendario "conciertos" de Radicale (mismo mecanismo que
     los conciertos de Ticketmaster: RadicaleClient.push_events_bulk),
     salvo que sea un livestream/anuncio sin recinto físico.
  3. Marca el correo como leído — única deduplicación, igual que el
     notificador de wantlist de Discogs.

Pensado para ejecutarse periódicamente vía Ofelia.
"""

import argparse
import logging
import os
import re
import sys
from datetime import date, datetime
from email import message_from_bytes
from email.header import decode_header
from imaplib import IMAP4_SSL

from dotenv import load_dotenv

import admin_notify
from apis.bandsintown_email_parser import parse_bandsintown_email
from apis.radicale import RadicaleClient

load_dotenv()

IMAP_SERVER = os.environ["IMAP_SERVER"]
IMAP_PORT = int(os.environ.get("IMAP_PORT") or 993)
IMAP_EMAIL = os.environ["IMAP_EMAIL"]
IMAP_PASSWORD = os.environ["IMAP_PASSWORD"]

RADICALE_URL = os.environ.get("RADICALE_URL", "")
RADICALE_USERNAME = os.environ.get("RADICALE_USERNAME", "")
RADICALE_PASSWORD = os.environ.get("RADICALE_PASSWORD", "")
RADICALE_CALENDAR = os.environ.get("RADICALE_CALENDAR", "")

# sender_kw -> parser(html) -> dict de concierto (ver bandsintown_email_parser)
SOURCE_PARSERS = {
    "bandsintown": parse_bandsintown_email,
}

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)


def decode_subject(raw):
    parts = decode_header(raw or "")
    out = ""
    for text, enc in parts:
        out += text.decode(enc or "utf-8", errors="replace") if isinstance(text, bytes) else text
    return out


def get_html_body(msg):
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                return part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", errors="replace")
        return None
    if msg.get_content_type() == "text/html":
        return msg.get_payload(decode=True).decode(msg.get_content_charset() or "utf-8", errors="replace")
    return None


def normalize_date_time(raw_date, raw_time, reference=None):
    """'Sat, Aug 08' + '07:00PM' -> ('2026-08-08', '19:00'). Sin año en el
    correo: se elige, entre año actual/anterior/siguiente, la fecha más
    cercana a `reference` que no quede más de 30 días en el pasado."""
    reference = reference or date.today()
    m = re.search(r"([A-Za-z]{3})\w*\s+(\d{1,2})", raw_date or "")
    if not m:
        return None, None
    try:
        month = datetime.strptime(m.group(1)[:3], "%b").month
    except ValueError:
        return None, None
    day = int(m.group(2))

    # Algunos formatos (rango de varios días, fechas lejanas) sí traen año
    # explícito en el propio texto -- usarlo directo en vez de adivinar.
    ym = re.search(r"\b(20\d{2})\b", raw_date or "")
    if ym:
        try:
            chosen = date(int(ym.group(1)), month, day)
        except ValueError:
            return None, None
        date_str = chosen.isoformat()
    else:
        candidates = []
        for year in (reference.year - 1, reference.year, reference.year + 1):
            try:
                candidates.append(date(year, month, day))
            except ValueError:
                continue
        if not candidates:
            return None, None
        future_or_recent = [d for d in candidates if (d - reference).days >= -30]
        chosen = min(future_or_recent or candidates, key=lambda d: abs((d - reference).days))
        date_str = chosen.isoformat()

    time_str = None
    tm = re.search(r"(\d{1,2}):(\d{2})\s*(AM|PM)?", raw_time or "", re.IGNORECASE)
    if tm:
        h, mi, ampm = int(tm.group(1)), tm.group(2), (tm.group(3) or "").upper()
        if ampm == "PM" and h != 12:
            h += 12
        elif ampm == "AM" and h == 12:
            h = 0
        time_str = f"{h:02d}:{mi}"

    return date_str, time_str


def _escape_md(text):
    """admin_notify.notify() envía con parse_mode=Markdown (legacy) — un
    '_'/'*'/'`'/'[' suelto (frecuentísimo en las URL de tracking de
    Sendgrid, o en nombres de artista/recinto) revienta el parseo de
    entidades con HTTP 400 y el correo se queda sin marcar como leído."""
    return re.sub(r"([_*`\[])", r"\\\1", text or "")


def format_telegram_message(concert):
    kind = "📡 Livestream/anuncio" if concert["is_livestream"] else "🎫 Nuevo concierto"
    lines = [f"{kind}: {_escape_md(concert['artist'])}"]
    lines.append(f"📅 {_escape_md(concert['raw_date'])}" + (f" {_escape_md(concert['raw_time'])}" if concert["raw_time"] else ""))
    if concert["venue"]:
        loc = _escape_md(concert["venue"])
        if concert["city"]:
            loc += f" — {_escape_md(concert['city'])}"
            if concert["country"]:
                loc += f", {_escape_md(concert['country'])}"
        lines.append(f"📍 {loc}")
    if concert["url"]:
        lines.append(_escape_md(concert["url"]))
    return "\n".join(lines)


def push_to_radicale(concert):
    if not (RADICALE_URL and RADICALE_USERNAME and RADICALE_PASSWORD and RADICALE_CALENDAR):
        log.warning("Radicale no configurado (falta RADICALE_PASSWORD u otra variable) — no se sube el evento")
        return
    if concert["is_livestream"]:
        log.info("Livestream/anuncio sin recinto — no se sube a Radicale: %s", concert["artist"])
        return

    date_str, time_str = normalize_date_time(concert["raw_date"], concert["raw_time"])
    if not date_str:
        log.warning("No se pudo normalizar la fecha '%s' — no se sube a Radicale", concert["raw_date"])
        return

    event = {
        "artist_name": concert["artist"],
        "venue": concert["venue"] or "",
        "city": concert["city"] or "",
        "country": concert["country"] or "",
        "date": date_str,
        "time": time_str or "",
        "url": concert["url"] or "",
    }
    client = RadicaleClient(RADICALE_URL, RADICALE_USERNAME, RADICALE_PASSWORD, RADICALE_CALENDAR)
    pushed, errors, error_msgs = client.push_events_bulk([event], event_type="concert")
    if errors:
        log.error("Error subiendo a Radicale (%s): %s", concert["artist"], "; ".join(error_msgs))
    else:
        log.info("Subido a Radicale: %s (%s)", concert["artist"], date_str)


def process_email(msg, dry_run):
    subject = decode_subject(msg.get("Subject", ""))
    from_addr = (msg.get("From") or "").lower()

    parser = None
    for kw, fn in SOURCE_PARSERS.items():
        if kw in from_addr:
            parser = fn
            break
    if parser is None:
        log.warning("Remitente no reconocido, se ignora: %s", from_addr)
        return False

    html = get_html_body(msg)
    if not html:
        log.warning("Mensaje sin cuerpo HTML: %s", subject)
        return False

    concert = parser(html)
    if not concert:
        log.warning("No se pudo extraer el concierto de: %s", subject)
        return False

    text = format_telegram_message(concert)
    if dry_run:
        print(text)
        print("---")
        return True

    ok = admin_notify.notify("concierto_email", details=text)
    if not ok:
        log.error("Fallo enviando por Telegram: %s", subject)
        return False

    push_to_radicale(concert)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="No envía a Telegram/Radicale ni marca como leído; imprime por stdout")
    args = parser.parse_args()

    m = IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
    m.login(IMAP_EMAIL, IMAP_PASSWORD)

    total = 0
    try:
        m.select("INBOX")
        for kw in SOURCE_PARSERS:
            typ, data = m.search(None, "UNSEEN", f'FROM "{kw}"')
            if typ != "OK" or not data[0]:
                log.info("%s: sin correos nuevos", kw)
                continue
            ids = data[0].split()
            log.info("%s: %d correo(s) nuevo(s)", kw, len(ids))
            for msg_id in ids:
                typ, msgdata = m.fetch(msg_id, "(RFC822)")
                if typ != "OK" or not msgdata or not msgdata[0]:
                    log.warning("No se pudo leer el mensaje %s", msg_id)
                    continue
                msg = message_from_bytes(msgdata[0][1])
                if process_email(msg, args.dry_run):
                    total += 1
                    if not args.dry_run:
                        m.store(msg_id, "+FLAGS", "\\Seen")
    finally:
        m.logout()

    log.info("Total conciertos procesados: %d", total)


if __name__ == "__main__":
    main()
