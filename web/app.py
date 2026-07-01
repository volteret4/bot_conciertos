#!/usr/bin/env python3
"""
Interfaz web de bot_conciertos — en construcción.
Sustituye este stub por la app real cuando esté lista;
el Dockerfile.web y el servicio bot-conciertos-web ya están preparados.
"""
import os
from flask import Flask

app = Flask(__name__)


@app.route("/")
def index():
    return (
        "<h1>bot_conciertos</h1>"
        "<p>La interfaz web está en construcción.</p>"
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8747))
    app.run(host="0.0.0.0", port=port)
