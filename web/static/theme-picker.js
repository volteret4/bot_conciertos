/*
 * Selector de tema compartido de tumtumpa. Archivo IDÉNTICO en las 8 apps.
 * Inyecta un <select> fijo en la esquina si la página no trae ya un
 * contenedor #theme-picker, aplica data-theme al <html> y persiste la
 * elección en localStorage (clave "tumtumpa-theme", default "og").
 */
(function () {
  var THEMES = [
    ["og", "OG"],
    ["nord", "Nord"],
    ["catppuccin", "Catppuccin"],
    ["kanagawa", "Kanagawa"],
    ["dracula", "Dracula"],
    ["solarized", "Solarized"],
    ["gruvbox", "Gruvbox"],
  ];
  var STORAGE_KEY = "tumtumpa-theme";

  function applyTheme(name) {
    document.documentElement.setAttribute("data-theme", name);
  }

  function ensureContainer() {
    var el = document.getElementById("theme-picker");
    if (el) return el;

    el = document.createElement("div");
    el.id = "theme-picker";
    el.style.position = "fixed";
    el.style.bottom = ".6rem";
    el.style.left = ".8rem";
    el.style.zIndex = "9999";
    el.style.fontFamily = "monospace";
    el.style.fontSize = ".7rem";
    document.body.appendChild(el);
    return el;
  }

  function buildSelect() {
    var select = document.createElement("select");
    select.id = "theme-picker-select";
    select.style.background = "var(--surface, #1e2028)";
    select.style.color = "var(--text, #e8e6e3)";
    select.style.border = "1px solid var(--border, #2e3340)";
    select.style.borderRadius = "4px";
    select.style.padding = ".25rem .4rem";
    select.style.fontFamily = "monospace";
    select.style.fontSize = ".7rem";
    select.style.cursor = "pointer";

    THEMES.forEach(function (t) {
      var opt = document.createElement("option");
      opt.value = t[0];
      opt.textContent = t[1];
      select.appendChild(opt);
    });
    return select;
  }

  function init() {
    var saved = localStorage.getItem(STORAGE_KEY) || "og";
    applyTheme(saved);

    var container = ensureContainer();
    var select = buildSelect();
    select.value = saved;
    select.addEventListener("change", function () {
      localStorage.setItem(STORAGE_KEY, select.value);
      applyTheme(select.value);
    });
    container.appendChild(select);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
