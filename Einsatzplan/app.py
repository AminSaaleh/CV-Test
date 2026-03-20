# app.py
# Flask App – PostgreSQL/Supabase Version (Aufbau wie APP 9), Logik unverändert übernommen aus der SQLite-Version.
#
# Start:
#   export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
#   export SECRET_KEY="."
#   python app.py
#
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g, send_file
import os, uuid, re, json, io
from datetime import datetime


def normalize_role(role: str) -> str:
    r = (role or "").strip().lower()
    # akzeptiere Anzeigenamen mit Leerzeichen
    if r in ["planner bbs", "planner_bbs"]:
        return "planner_bbs"
    if r in ["vorgesetzter cp", "vorgesetzter_cp"]:
        return "vorgesetzter_cp"
    return r




# --- Mail (Gmail App Password / SMTP) ---
import smtplib
from email.message import EmailMessage

# ---------------- SMTP Config ----------------
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
MAIL_FROM = os.environ.get("MAIL_FROM", f"REMINDER – CV Planung <{SMTP_USER}>")

def send_mail(to_addr: str, subject: str, body: str) -> None:
    """Send a plain text email via SMTP. No-op if config is missing."""
    to_addr = (to_addr or "").strip()
    if not to_addr:
        return
    if not (SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS):
        return

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        s.ehlo()
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

def build_welcome_mail(employee_name: str, username: str, password: str) -> str:
    lines = [
        f"Hallo {employee_name},",
        "",
        "herzlich willkommen beim Casutt Veranstaltungsservice!",
        "",
        "Deine Zugangsdaten:",
        f"Benutzername: {username}",
        f"Passwort: {password}",
        "",
        "Hier geht es zur CV-Planung:",
        "https://cv-planung.onrender.com",
        "",
        "Wir freuen uns auf die zusammenarbeit!",
        "",
        "Viele Grüße",
        "CV Planung"
    ]
    return "\n".join(lines)


def build_change_mail(employee_name: str,
                      event_title: str,
                      event_start_dt: str,
                      ort: str,
                      dienstkleidung: str,
                      new_start_time: str,
                      new_remark: str = "") -> str:
    # Datum immer europäisch: TT.MM.JJJJ
    date_de = "TT.MM.JJJJ"
    try:
        if isinstance(event_start_dt, str) and event_start_dt.strip():
            d = datetime.fromisoformat(event_start_dt.replace("Z", "").strip())
            date_de = d.strftime("%d.%m.%Y")
    except Exception:
        pass

    # Inhalt dynamisch: nur geänderte Felder in die Mail
    lines = [
        f"Hallo {employee_name},",
        "",
        f"es gibt eine Aktualisierung zu deinem Einsatz am {date_de}.",
        ""
    ]

    start_time = (new_start_time or "").strip()
    remark_line = (new_remark or "").strip()

    if start_time:
        lines.append(f"Neue Startzeit: {start_time} ✅")
    if remark_line:
        lines.append(f"Neue Bemerkung: {remark_line} ✅")

    # Basisinfos immer mitgeben
    title = (event_title or "").strip() or "-"
    dienst = (dienstkleidung or "").strip() or "-"
    location = (ort or "").strip() or "-"

    lines.extend([
        "",
        f"Einsatz:  {title}",
        f"Dienstkleidung: {dienst}",
        f"Ort: {location}",
        "",
        "Viele Grüße",
        "CV Planung"
    ])

    return "\n".join(lines)

import psycopg2
import psycopg2.extras
from psycopg2 import IntegrityError
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, template_folder=os.path.join(BASE_DIR, 'templates'), static_folder=os.path.join(BASE_DIR, 'static'))
app.secret_key = os.environ.get("SECRET_KEY", "geheimes_passwort")

# Supabase/PostgreSQL connection string
DATABASE_URL = os.environ.get("DATABASE_URL")


# ---------------- DB helpers (PostgreSQL / Supabase) ----------------
class DBWrapper:
    def __init__(self, conn):
        self.conn = conn

    def execute(self, sql, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def get_db():
    db = getattr(g, "_db", None)
    if db is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL ist nicht gesetzt (Supabase/PostgreSQL Verbindung fehlt).")

        connect_kwargs = {
            "dsn": DATABASE_URL,
            "cursor_factory": psycopg2.extras.RealDictCursor,
        }
        # Supabase verlangt i.d.R. SSL. Wenn sslmode nicht im URL steht, erzwingen wir require.
        if "sslmode=" not in (DATABASE_URL or ""):
            connect_kwargs["sslmode"] = "require"

        conn = psycopg2.connect(**connect_kwargs)
        db = g._db = DBWrapper(conn)
    return db


@app.teardown_appcontext
def close_db(exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


def col_exists(db, table, col):
    cur = db.execute(
        '''
        SELECT 1
        FROM information_schema.columns
        WHERE table_name=%s AND column_name=%s
        ''',
        (table, col),
    )
    return cur.fetchone() is not None


def row_to_dict(row):
    return dict(row)


def to_int(v, default=0):
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return default



def normalize_s34a_art(value):
    if not value:
        return value

    value = value.strip().lower()

    if value == "unterrichtung":
        return "Unterrichtung"
    if value == "sachkunde":
        return "Sachkunde"

    return value


def status_to_css_token(value: str) -> str:
    """Normalize status strings for safe CSS class tokens (e.g. 'bestätigt' -> 'bestaetigt')."""
    s = (value or "").strip().lower()
    if not s:
        return ""
    # German umlauts
    s = (s.replace("ä", "ae")
           .replace("ö", "oe")
           .replace("ü", "ue")
           .replace("ß", "ss"))
    # allow only [a-z0-9_-], replace other runs with '-'
    s = re.sub(r"[^a-z0-9_-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s




def get_user_consent(db, username: str) -> dict:
    """Return consent info for a user: {given: bool, name: str, date: str, full_name: str}."""
    u = db.execute(
        "SELECT vorname, nachname, consent_given, consent_name, consent_date FROM users WHERE username=%s",
        (username,),
    ).fetchone()
    if not u:
        return {"given": False, "name": "", "date": "", "full_name": ""}

    full_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip()
    given = bool(u.get("consent_given") or False)
    name = (u.get("consent_name") or "").strip()
    date = (u.get("consent_date") or "").strip()
    return {"given": given, "name": name, "date": date, "full_name": full_name}


def employee_requires_consent() -> bool:
    """True if current session is a 'mitarbeiter' and consent is missing."""
    if session.get("role") != "mitarbeiter":
        return False
    try:
        info = get_user_consent(get_db(), session.get("username"))
        return not bool(info.get("given"))
    except Exception:
        # Im Zweifel sperren wir
        return True

def init_db():
    db = get_db()

    # NOTE: In Postgres ist "user" ein reserviertes Wort -> wir nutzen "users".
    db.execute(
        '''
        
CREATE TABLE IF NOT EXISTS users (
    username TEXT PRIMARY KEY,
    password TEXT NOT NULL,
    role TEXT DEFAULT 'mitarbeiter',
    vorname TEXT,
    nachname TEXT,
    email TEXT,
    telefon TEXT,
    geburtsdatum TEXT,
    geburtsort TEXT,
    staatsangehoerigkeit TEXT,
    staatsnummer TEXT,
    bkv_rv TEXT,
    sv5n TEXT,
    image_data TEXT,
    image_name TEXT,
    amtliches_dokument TEXT,
    dokumentennummer TEXT,
    ausstellungsdatum TEXT,
    ausstellende_behoerde TEXT,
    s34a TEXT,
    s34a_art TEXT,
    amt TEXT,
    bewerber_id TEXT,
    steuernummer TEXT,
    bewach_id TEXT,
    bsw TEXT,
    fschein TEXT,
    kati TEXT,
    sanitaeter TEXT,
    pschein TEXT,
    stundensatz DOUBLE PRECISION,
    bemerkung TEXT,
    sonstige TEXT,
    qualifications_json TEXT,
    license_classes_json TEXT,
    sprachen_json TEXT,
    weitere_sprachen_json TEXT,
    consent_given BOOLEAN DEFAULT FALSE,
    consent_name TEXT,
    consent_date TEXT,
    is_locked BOOLEAN DEFAULT FALSE
);
'''
    )

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS event (
            id TEXT PRIMARY KEY,
            title TEXT,
            ort TEXT,
            dienstkleidung TEXT,
            auftraggeber TEXT,
            start TEXT,
            planned_end_time TEXT,      -- 'HH:MM'
            frist TEXT,                 -- 'YYYY-MM-DDTHH:MM' (Annahmefrist)
            status TEXT,                -- 'geplant' | 'offen'
            category TEXT DEFAULT 'CP', -- 'CP' | 'CV'
            required_staff INTEGER DEFAULT 0,
            use_event_rate INTEGER DEFAULT 1, -- 1=Einsatz-Stundensatz, 0=User-Profil
            stundensatz DOUBLE PRECISION
        );
        '''
    )

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS response (
            id SERIAL PRIMARY KEY,
            event_id TEXT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
            username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
            status TEXT,
            remark TEXT,
            start_time TEXT,
            end_time TEXT,
            rate_override DOUBLE PRECISION,
            UNIQUE(event_id, username)
        );
        '''
    )

    # Indizes
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_event ON response(event_id);")
    db.execute("CREATE INDEX IF NOT EXISTS idx_response_user  ON response(username);")

    # ---- Migrationen (falls Tabellen schon existieren, aber Spalten fehlen) ----
    
# users
for c, ddl in [
    ("email", "ALTER TABLE users ADD COLUMN email TEXT"),
    ("telefon", "ALTER TABLE users ADD COLUMN telefon TEXT"),
    ("geburtsdatum", "ALTER TABLE users ADD COLUMN geburtsdatum TEXT"),
    ("geburtsort", "ALTER TABLE users ADD COLUMN geburtsort TEXT"),
    ("staatsangehoerigkeit", "ALTER TABLE users ADD COLUMN staatsangehoerigkeit TEXT"),
    ("staatsnummer", "ALTER TABLE users ADD COLUMN staatsnummer TEXT"),
    ("bkv_rv", "ALTER TABLE users ADD COLUMN bkv_rv TEXT"),
    ("sv5n", "ALTER TABLE users ADD COLUMN sv5n TEXT"),
    ("image_data", "ALTER TABLE users ADD COLUMN image_data TEXT"),
    ("image_name", "ALTER TABLE users ADD COLUMN image_name TEXT"),
    ("amtliches_dokument", "ALTER TABLE users ADD COLUMN amtliches_dokument TEXT"),
    ("dokumentennummer", "ALTER TABLE users ADD COLUMN dokumentennummer TEXT"),
    ("ausstellungsdatum", "ALTER TABLE users ADD COLUMN ausstellungsdatum TEXT"),
    ("ausstellende_behoerde", "ALTER TABLE users ADD COLUMN ausstellende_behoerde TEXT"),
    ("bewach_id", "ALTER TABLE users ADD COLUMN bewach_id TEXT"),
    ("steuernummer", "ALTER TABLE users ADD COLUMN steuernummer TEXT"),
    ("amt", "ALTER TABLE users ADD COLUMN amt TEXT"),
    ("bewerber_id", "ALTER TABLE users ADD COLUMN bewerber_id TEXT"),
    ("bsw", "ALTER TABLE users ADD COLUMN bsw TEXT"),
    ("fschein", "ALTER TABLE users ADD COLUMN fschein TEXT"),
    ("kati", "ALTER TABLE users ADD COLUMN kati TEXT"),
    ("sanitaeter", "ALTER TABLE users ADD COLUMN sanitaeter TEXT"),
    ("pschein", "ALTER TABLE users ADD COLUMN pschein TEXT"),
    ("stundensatz", "ALTER TABLE users ADD COLUMN stundensatz DOUBLE PRECISION"),
    ("bemerkung", "ALTER TABLE users ADD COLUMN bemerkung TEXT"),
    ("sonstige", "ALTER TABLE users ADD COLUMN sonstige TEXT"),
    ("qualifications_json", "ALTER TABLE users ADD COLUMN qualifications_json TEXT"),
    ("license_classes_json", "ALTER TABLE users ADD COLUMN license_classes_json TEXT"),
    ("sprachen_json", "ALTER TABLE users ADD COLUMN sprachen_json TEXT"),
    ("weitere_sprachen_json", "ALTER TABLE users ADD COLUMN weitere_sprachen_json TEXT"),
    ("consent_given", "ALTER TABLE users ADD COLUMN consent_given BOOLEAN DEFAULT FALSE"),
    ("consent_name", "ALTER TABLE users ADD COLUMN consent_name TEXT"),
    ("consent_date", "ALTER TABLE users ADD COLUMN consent_date TEXT"),
    ("s34a", "ALTER TABLE users ADD COLUMN s34a TEXT"),
    ("s34a_art", "ALTER TABLE users ADD COLUMN s34a_art TEXT"),
    ("vorname", "ALTER TABLE users ADD COLUMN vorname TEXT"),
    ("nachname", "ALTER TABLE users ADD COLUMN nachname TEXT"),
    ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'mitarbeiter'"),
    ("password", "ALTER TABLE users ADD COLUMN password TEXT"),
    ("is_locked", "ALTER TABLE users ADD COLUMN is_locked BOOLEAN DEFAULT FALSE"),
]:
    if not col_exists(db, "users", c):
        db.execute(ddl)

    # event
    for c, ddl in [
        ("planned_end_time", "ALTER TABLE event ADD COLUMN planned_end_time TEXT"),
        ("frist", "ALTER TABLE event ADD COLUMN frist TEXT"),
        ("status", "ALTER TABLE event ADD COLUMN status TEXT"),
        ("category", "ALTER TABLE event ADD COLUMN category TEXT DEFAULT 'CP'"),
        ("required_staff", "ALTER TABLE event ADD COLUMN required_staff INTEGER DEFAULT 0"),
        ("use_event_rate", "ALTER TABLE event ADD COLUMN use_event_rate INTEGER DEFAULT 1"),
        ("stundensatz", "ALTER TABLE event ADD COLUMN stundensatz DOUBLE PRECISION"),
    ]:
        if not col_exists(db, "event", c):
            db.execute(ddl)

    # response
    for c, ddl in [
        ("status", "ALTER TABLE response ADD COLUMN status TEXT"),
        ("remark", "ALTER TABLE response ADD COLUMN remark TEXT"),
        ("start_time", "ALTER TABLE response ADD COLUMN start_time TEXT"),
        ("end_time", "ALTER TABLE response ADD COLUMN end_time TEXT"),
        ("rate_override", "ALTER TABLE response ADD COLUMN rate_override DOUBLE PRECISION"),
    ]:
        if not col_exists(db, "response", c):
            db.execute(ddl)

    db.commit()

    # ---- AdminTest ----
    exists = db.execute("SELECT 1 FROM users WHERE username=%s", ("AdminTest",)).fetchone()
    if not exists:
        db.execute(
            '''
            INSERT INTO users
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ''',
            (
                "AdminTest", "Test1234", "vorgesetzter",
                "Admin", "Test",
                "",          # email
                "ja",        # s34a
                "Sachkunde", # s34a_art
                "ja",        # pschein
                "A-000",     # bewach_id
                "ST-000",    # steuernummer
                "nein",      # bsw
                "nein",      # sanitaeter
                0.0,
            ),
        )
        db.commit()


USER_LIST_FIELDS = [
    "username", "password", "role", "vorname", "nachname", "email", "telefon",
    "geburtsdatum", "geburtsort", "staatsangehoerigkeit", "staatsnummer",
    "bkv_rv", "sv5n", "image_data", "image_name",
    "amtliches_dokument", "dokumentennummer", "ausstellungsdatum", "ausstellende_behoerde",
    "s34a", "s34a_art", "amt", "bewerber_id", "steuernummer", "bewach_id",
    "bsw", "fschein", "kati", "sanitaeter", "pschein", "stundensatz",
    "bemerkung", "sonstige", "qualifications_json", "license_classes_json",
    "sprachen_json", "weitere_sprachen_json", "consent_given", "consent_name",
    "consent_date", "is_locked"
]

YES_NO_FIELDS = ["s34a", "bsw", "fschein", "kati", "sanitaeter", "pschein"]
JSON_LIST_FIELDS = ["qualifications_json", "license_classes_json", "sprachen_json", "weitere_sprachen_json"]


def json_dumps_safe(value):
    try:
        return json.dumps(value or [], ensure_ascii=False)
    except Exception:
        return "[]"


def json_loads_safe(value):
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def normalize_yes_no(value, default="nein"):
    v = str(value or "").strip().lower()
    if v in ("1", "true", "yes", "ja", "y"):
        return "ja"
    if v in ("0", "false", "no", "nein", "n"):
        return "nein"
    return default


def normalize_bool(value):
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "yes", "ja", "y")


def clean_str(value):
    return str(value or "").strip()


def enrich_user_record(row):
    u = dict(row)
    for f in JSON_LIST_FIELDS:
        u[f] = json_loads_safe(u.get(f))
    u["qualifications"] = u.get("qualifications_json", [])
    u["license_classes"] = u.get("license_classes_json", [])
    u["sprachen"] = u.get("sprachen_json", [])
    u["weitere_sprachen"] = u.get("weitere_sprachen_json", [])
    u["is_locked"] = bool(u.get("is_locked") or False)
    u["consent_given"] = bool(u.get("consent_given") or False)
    u["full_name"] = f"{clean_str(u.get('vorname'))} {clean_str(u.get('nachname'))}".strip()
    return u


def normalize_user_payload(d, existing=None):
    existing = dict(existing or {})
    payload = {k: existing.get(k) for k in USER_LIST_FIELDS}

    scalar_fields = [
        "username", "password", "role", "vorname", "nachname", "email", "telefon",
        "geburtsdatum", "geburtsort", "staatsangehoerigkeit", "staatsnummer", "bkv_rv", "sv5n",
        "image_data", "image_name", "amtliches_dokument", "dokumentennummer", "ausstellungsdatum",
        "ausstellende_behoerde", "s34a_art", "amt", "bewerber_id", "steuernummer", "bewach_id",
        "bemerkung", "sonstige"
    ]
    for f in scalar_fields:
        if f in d:
            payload[f] = clean_str(d.get(f))

    if "stundensatz" in d:
        payload["stundensatz"] = None if d.get("stundensatz") in (None, "") else float(d.get("stundensatz"))
    elif existing and "stundensatz" in existing:
        payload["stundensatz"] = existing.get("stundensatz")
    else:
        payload["stundensatz"] = None

    payload["role"] = clean_str(payload.get("role") or "mitarbeiter") or "mitarbeiter"
    payload["s34a_art"] = normalize_s34a_art(payload.get("s34a_art") or "")

    for f in YES_NO_FIELDS:
        if f in d:
            payload[f] = normalize_yes_no(d.get(f))
        elif existing:
            payload[f] = normalize_yes_no(existing.get(f), default=existing.get(f) or "nein")
        else:
            payload[f] = "nein"

    list_map = {
        "qualifications_json": d.get("qualifications", existing.get("qualifications_json") if existing else []),
        "license_classes_json": d.get("license_classes", existing.get("license_classes_json") if existing else []),
        "sprachen_json": d.get("sprachen", existing.get("sprachen_json") if existing else []),
        "weitere_sprachen_json": d.get("weitere_sprachen", existing.get("weitere_sprachen_json") if existing else []),
    }
    for field, value in list_map.items():
        payload[field] = json_dumps_safe(value if isinstance(value, list) else json_loads_safe(value))

    if "consent_given" in d:
        payload["consent_given"] = normalize_bool(d.get("consent_given"))
    else:
        payload["consent_given"] = bool(existing.get("consent_given") or False)
    payload["consent_name"] = clean_str(d.get("consent_name")) if "consent_name" in d else clean_str(existing.get("consent_name"))
    payload["consent_date"] = clean_str(d.get("consent_date")) if "consent_date" in d else clean_str(existing.get("consent_date"))
    payload["is_locked"] = normalize_bool(d.get("is_locked")) if "is_locked" in d else bool(existing.get("is_locked") or False)

    return payload


def validate_user_payload(payload, is_new=False):
    required = ["vorname", "nachname", "username", "role"]
    if is_new:
        required.append("password")
    for field in required:
        if not clean_str(payload.get(field)):
            return f"{field} ist erforderlich"
    email = clean_str(payload.get("email"))
    if email and not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", email):
        return "Bitte eine gültige E-Mail-Adresse eingeben."
    return None


def build_user_pdf(user):
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    y = height - 40

    def line(text="", gap=16, bold=False):
        nonlocal y
        if y < 60:
            c.showPage()
            y = height - 40
        c.setFont("Helvetica-Bold" if bold else "Helvetica", 10 if not bold else 12)
        c.drawString(40, y, str(text)[:120])
        y -= gap

    c.setTitle(f"Personal_{user.get('username') or 'profil'}")
    line("Personaldaten", bold=True, gap=22)
    line(f"Name: {user.get('full_name') or '-'}")
    line(f"Benutzername: {user.get('username') or '-'}")
    line(f"Rolle: {user.get('role') or '-'}")
    line(f"E-Mail: {user.get('email') or '-'}")
    line(f"Telefon: {user.get('telefon') or '-'}")
    line(f"Geburtsdatum / -ort: {user.get('geburtsdatum') or '-'} / {user.get('geburtsort') or '-'}")
    line(f"Staatsangehörigkeit / Staatsnummer: {user.get('staatsangehoerigkeit') or '-'} / {user.get('staatsnummer') or '-'}")
    line(f"BKV/RV: {user.get('bkv_rv') or '-'}")
    line(f"SV-5/N: {user.get('sv5n') or '-'}")
    line(f"Amtliches Dokument: {user.get('amtliches_dokument') or '-'}")
    line(f"Dokumentennummer: {user.get('dokumentennummer') or '-'}")
    line(f"Ausstellungsdatum / Behörde: {user.get('ausstellungsdatum') or '-'} / {user.get('ausstellende_behoerde') or '-'}")
    line(f"§34a / Nachweis: {user.get('s34a') or '-'} {('(' + user.get('s34a_art') + ')') if user.get('s34a_art') else ''}")
    line(f"Amt / Bewerber-ID: {user.get('amt') or '-'} / {user.get('bewerber_id') or '-'}")
    line(f"BSW / F-Schein / KATI: {user.get('bsw') or '-'} / {user.get('fschein') or '-'} / {user.get('kati') or '-'}")
    line(f"Sanitäter / P-Schein / SVG: {user.get('sanitaeter') or '-'} / {user.get('pschein') or '-'} / {user.get('sv5n') or '-'}")
    line(f"Qualifikationen: {', '.join(user.get('qualifications') or []) or '-'}")
    line(f"Führerscheinklassen: {', '.join(user.get('license_classes') or []) or '-'}")
    line(f"Sprachen: {', '.join(user.get('sprachen') or []) or '-'}")
    line(f"Weitere Sprachen: {', '.join(user.get('weitere_sprachen') or []) or '-'}")
    line(f"Sonstige: {user.get('sonstige') or '-'}")
    line(f"Bemerkung: {user.get('bemerkung') or '-'}")
    line(f"Datenschutzerklärung: {'Ja' if user.get('consent_given') else 'Nein'}")
    line(f"Status: {'Gesperrt' if user.get('is_locked') else 'Aktiv'}")
    c.save()
    buf.seek(0)
    return buf


def safe_init_db():
    try:
        with app.app_context():
            init_db()
        print("DB-Initialisierung erfolgreich.")
    except Exception as e:
        # Wichtig: nicht crashen, nur Fehler loggen
        print("FEHLER bei init_db():", repr(e))


# Wird beim Import einmal ausgeführt
safe_init_db()


# ---------------- Routes ----------------
@app.route("/health")
def health():
    return "ok", 200


@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"].strip()
        password = request.form["password"]

        db = get_db()
        u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()

        if u and u.get("password") == password:
            session["username"] = username
            session["role"] = u.get("role") or "mitarbeiter"
            return redirect(url_for("dashboard"))

        return render_template("login.html", error="Login fehlgeschlagen")
    return render_template("login.html")


@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect(url_for("login"))

    role = normalize_role(session.get("role") or "mitarbeiter")

    # Chef-Dashboard auch für Planer (UI beschränkt Planer auf den Planung-Reiter)
    if role in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        return render_template("dashboard_chef.html", user=session["username"], role=role)

    return render_template("dashboard_mitarbeiter.html", user=session["username"], role=role)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ---------------- Consent (DSGVO) ----------------
@app.route("/consent_status", methods=["GET"])
def consent_status():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403
    db = get_db()
    info = get_user_consent(db, session.get("username"))
    return jsonify(info)


@app.route("/consent", methods=["POST"])
def consent_set():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # Nur Mitarbeiter müssen hier zustimmen
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    yes = bool(d.get("yes") is True or str(d.get("yes")).lower() in ("1", "true", "ja", "yes"))
    name = (d.get("name") or "").strip()
    date = (d.get("date") or "").strip()

    if not yes:
        return jsonify({"error": "Bitte bestätige die Einwilligung."}), 400
    if not name:
        return jsonify({"error": "Name ist erforderlich."}), 400
    if not date:
        # Fallback: heute
        date = datetime.now().strftime("%Y-%m-%d")

    db = get_db()
    db.execute(
        "UPDATE users SET consent_given=TRUE, consent_name=%s, consent_date=%s WHERE username=%s",
        (name, date, session.get("username")),
    )
    db.commit()
    return jsonify({"status": "ok"})



# ---------------- Users API ----------------
@app.route("/users", methods=["GET"])
def get_users():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        "SELECT * FROM users WHERE username NOT IN (%s,%s) ORDER BY nachname, vorname",
        ("AdminTest", "TestAdmin")
    )
    users = [enrich_user_record(r) for r in cur.fetchall()]
    for u in users:
        if u.get("stundensatz") is None:
            u["stundensatz"] = ""
    return jsonify(users)


@app.route("/users_public", methods=["GET"])
def users_public():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    if session.get("role") not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        "SELECT username, vorname, nachname FROM users WHERE username NOT IN (%s,%s) ORDER BY nachname, vorname",
        ("AdminTest", "TestAdmin")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    return jsonify(users)


@app.route("/users", methods=["POST"])
def add_user():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    payload = normalize_user_payload(d)
    err = validate_user_payload(payload, is_new=True)
    if err:
        return jsonify({"error": err}), 400

    db = get_db()
    if db.execute("SELECT 1 FROM users WHERE username=%s", (payload["username"],)).fetchone():
        return jsonify({"error": "Benutzername existiert bereits."}), 400

    columns = USER_LIST_FIELDS
    values = [payload.get(c) for c in columns]
    placeholders = ",".join(["%s"] * len(columns))
    sql = f"INSERT INTO users ({','.join(columns)}) VALUES ({placeholders})"

    try:
        db.execute(sql, tuple(values))
        db.commit()
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    email = clean_str(payload.get("email"))
    employee_name = f"{clean_str(payload.get('vorname'))} {clean_str(payload.get('nachname'))}".strip() or payload["username"]
    mail_sent = False
    mail_error = ""
    invite_link = "https://cv-planung.onrender.com"
    if email:
        subject = "Deine Zugangsdaten zum Portal"
        body = build_welcome_mail(employee_name, payload["username"], payload["password"]) + f"\n\nEinladungslink: {invite_link}"
        try:
            send_mail(email, subject, body)
            mail_sent = True
        except Exception as e:
            mail_error = str(e)
    else:
        mail_error = "Keine E-Mail-Adresse hinterlegt."

    return jsonify({"status": "ok", "mail_sent": mail_sent, "mail_error": mail_error})


@app.route("/users/rename", methods=["POST"])
def rename_user():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    old_username = clean_str(d.get("old_username"))
    new_username = clean_str(d.get("new_username"))
    if not old_username or not new_username:
        return jsonify({"error": "old_username und new_username erforderlich"}), 400

    db = get_db()
    try:
        old = db.execute("SELECT * FROM users WHERE username=%s", (old_username,)).fetchone()
        if not old:
            return jsonify({"error": "Alter Benutzer nicht gefunden"}), 404
        if db.execute("SELECT 1 FROM users WHERE username=%s", (new_username,)).fetchone():
            return jsonify({"error": "Neuer Benutzername existiert schon"}), 400

        payload = normalize_user_payload({}, old)
        payload["username"] = new_username
        columns = USER_LIST_FIELDS
        values = [payload.get(c) for c in columns]
        placeholders = ",".join(["%s"] * len(columns))
        sql = f"INSERT INTO users ({','.join(columns)}) VALUES ({placeholders})"
        db.execute(sql, tuple(values))
        db.execute("UPDATE response SET username=%s WHERE username=%s", (new_username, old_username))
        db.execute("DELETE FROM users WHERE username=%s", (old_username,))
        db.commit()
        return jsonify({"status": "ok"})
    except IntegrityError as e:
        db.rollback()
        return jsonify({"error": f"Datenbankfehler: {str(e)}"}), 400
    except Exception as e:
        db.rollback()
        return jsonify({"error": f"Serverfehler: {str(e)}"}), 500


@app.route("/users/<username>", methods=["PUT"])
def edit_user(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    db = get_db()
    current = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not current:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    payload = normalize_user_payload(d, current)
    err = validate_user_payload(payload, is_new=False)
    if err:
        return jsonify({"error": err}), 400

    update_fields = [c for c in USER_LIST_FIELDS if c != "username"]
    set_clause = ", ".join([f"{c}=%s" for c in update_fields])
    values = [payload.get(c) for c in update_fields] + [username]

    db.execute(f"UPDATE users SET {set_clause} WHERE username=%s", tuple(values))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>/lock", methods=["POST"])
def lock_user(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    db = get_db()
    current = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not current:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404
    locked = normalize_bool(d.get("locked")) if "locked" in d else (not bool(current.get("is_locked") or False))
    db.execute("UPDATE users SET is_locked=%s WHERE username=%s", (locked, username))
    db.commit()
    return jsonify({"status": "ok", "is_locked": locked})


@app.route("/users/<username>/pdf", methods=["GET"])
def user_pdf(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404
    pdf = build_user_pdf(enrich_user_record(u))
    return send_file(pdf, mimetype="application/pdf", as_attachment=False, download_name=f"personal_{username}.pdf")

# ---------------- Events API ----------------
@app.route("/events", methods=["GET"])
def events_list():
    # ✅ Login erforderlich (damit Planer/Mitarbeiter nicht anonym zugreifen)
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # ✅ DSGVO: Mitarbeiter ohne Einwilligung dürfen keine Einsätze laden
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst auf der Startseite in die Datenverarbeitung einwilligen."}), 403

    db = get_db()
    role = normalize_role(session.get("role") or "mitarbeiter")

    ecur = db.execute("SELECT * FROM event")
    events = [row_to_dict(e) for e in ecur.fetchall()]

    # ✅ Rollen-Restriktionen (serverseitig)
    role_lc = normalize_role(role)
    if role_lc == "planner_bbs":
        events = [e for e in events if (e.get("category") or "CP").strip().upper() == "CV"]
    # Mitarbeiter: Profil-Stundensatz holen (für my_rate)
    my_profile_rate = 0.0
    if role not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
        me = db.execute("SELECT * FROM users WHERE username=%s", (session.get("username"),)).fetchone()
        if me:
            my_profile_rate = float(me.get("stundensatz") or 0.0)

    result = []
    for e in events:
        rcur = db.execute(
            "SELECT username,status,remark,start_time,end_time,rate_override FROM response WHERE event_id=%s",
            (e["id"],)
        )
        rmap = {
            r["username"]: {
                "status": r["status"] or "",
                "remark": r["remark"] or "",
                "start_time": r["start_time"] or "",
                "end_time": r.get("end_time") or "",
                "rate_override": r["rate_override"]
            } for r in rcur.fetchall()
        }
        e["responses"] = rmap

        # ---- UI helpers: CSS Klassen für FullCalendar (Dot/Block Färbung) ----
        # Diese Erweiterung entfernt/ändert keine bestehende Logik; sie ergänzt nur Metadaten fürs Frontend.
        cls = []
        # Kategorie (CP/CV)
        cat = (e.get("category") or "CP").strip().upper()
        if cat not in ("CP","CV"):
            cat = "CP"
        cls.append("cat-" + cat.lower())

        # Event-Status (geplant/offen/...)
        ev_status_token = status_to_css_token(e.get("status", ""))
        if ev_status_token:
            cls.append(f"status-event-{ev_status_token}")

        # Zusatz-Status für Chef-Ansicht (nur bei status 'offen'):
        # - 'voll'  => benötigte Mitarbeiter erreicht (grün)
        # - 'bewerbung' => es gibt Bewerbungen/Zusagen, aber noch nicht voll (blau)
        # Diese Logik ergänzt nur CSS-Klassen und ändert keine Daten in der DB.
        try:
            req = int(e.get("required_staff") or 0)
        except Exception:
            req = 0

        # Bewerbungen/Zusagen zählen (alles, was nicht leer ist und nicht explizit entfernt wurde)
        has_applications = any(
            (rv.get("status") or "").strip() in ("zugesagt", "bestätigt")
            for rv in (rmap or {}).values()
        )

        confirmed_count = sum(
            1 for rv in (rmap or {}).values()
            if (rv.get("status") or "").strip() == "bestätigt"
        )

        if (e.get("status") or "").strip().lower() == "offen":
            if req > 0 and confirmed_count >= req:
                cls.append("status-event-voll")
            elif has_applications:
                cls.append("status-event-bewerbung")

        # Für Mitarbeiter: eigener Response-Status als Klasse (zugesagt/bestätigt/abgelehnt/...)
        if role not in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
            my = rmap.get(session.get("username"), {}) or {}
            my_status_token = status_to_css_token(my.get("status", ""))
            if my_status_token:
                cls.append(f"status-{my_status_token}")

        # An FullCalendar übergeben (wird als classNames akzeptiert)
        e["classNames"] = cls

        # ✅ BUGFIX: 0 darf NICHT zu 1 werden
        raw_u = e.get("use_event_rate")
        use_event_rate = 1 if raw_u is None else int(raw_u)

        # Chef/Vorgesetzter/Planer: keine eigenen Raten berechnen
        if role in ["chef", "vorgesetzter", "planer", "planner_bbs", "vorgesetzter_cp"]:
            e["my_rate"] = 0
        else:
            if use_event_rate == 1:
                e["my_rate"] = float(e.get("stundensatz") or 0.0)
            else:
                e["my_rate"] = my_profile_rate

        result.append(e)

    return jsonify(result)


@app.route("/events", methods=["POST"])
def add_event():
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    ev_id = str(uuid.uuid4())

    start = d.get("start") or ""
    planned_end_time = (d.get("planned_end_time") or "").strip()
    frist = (d.get("frist") or "").strip()

    status = d.get("status", "geplant")
    category = (d.get("category") or "CP").strip().upper()
    if category not in ("CP","CV"):
        category = "CP"
    required_staff = to_int(d.get("required_staff", 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    db.execute(
        """INSERT INTO event
           (id,title,ort,dienstkleidung,auftraggeber,start,planned_end_time,frist,status,category,required_staff,use_event_rate,stundensatz)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            ev_id,
            d.get("title") or "",
            d.get("ort") or "",
            d.get("dienstkleidung") or "",
            d.get("auftraggeber") or "",
            start,
            planned_end_time,
            frist,
            status,
            category,
            required_staff,
            use_event_rate,
            stundensatz
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/assign_user", methods=["POST"])
def assign_user():
    """Chef: Mitarbeiter als bestätigt zuweisen."""
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
    if not db.execute("SELECT 1 FROM event WHERE id=%s", (event_id,)).fetchone():
        return jsonify({"error": "Event nicht gefunden"}), 404

    if not db.execute("SELECT 1 FROM users WHERE username=%s", (username,)).fetchone():
        return jsonify({"error": "User nicht gefunden"}), 404

    if db.execute("SELECT 1 FROM response WHERE event_id=%s AND username=%s", (event_id, username)).fetchone():
        db.execute(
            "UPDATE response SET status='bestätigt' WHERE event_id=%s AND username=%s",
            (event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, "bestätigt", "", "", "")
        )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/remove_user", methods=["POST"])
def remove_user_from_event():
    """Chef: Mitarbeiter komplett aus Einsatz entfernen."""
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    username = d.get("username")

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()
        # Statt Löschen: auf "entfernt_chef" setzen, damit der Mitarbeiter den Einsatz nicht mehr sieht
    # und es nicht wieder als "offen" erscheint.
    cur = db.execute(
        "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
        ("entfernt_chef", event_id, username)
    )

    # Falls es noch keinen Response-Eintrag gab, legen wir einen entfernt_chefen an
    if cur.rowcount == 0:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, "entfernt_chef", "", "", "")
        )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM event WHERE id=%s", (event_id,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/release", methods=["POST"])
def release_event():
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    event_id = d.get("event_id")

    db = get_db()
    cur = db.execute("UPDATE event SET status='offen' WHERE id=%s", (event_id,))
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/update", methods=["POST"])
def update_event():
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = d.get("event_id")
    if not event_id:
        return jsonify({"error": "event_id fehlt"}), 400

    title = d.get("title") or ""
    ort = d.get("ort") or ""
    dienstkleidung = d.get("dienstkleidung") or ""
    auftraggeber = d.get("auftraggeber") or ""
    start = d.get("start") or ""
    planned_end_time = (d.get("planned_end_time") or "").strip()
    frist = (d.get("frist") or "").strip()
    status = d.get("status") or "geplant"
    category = (d.get("category") or "CP").strip().upper()
    if category not in ("CP","CV"):
        category = "CP"
    required_staff = to_int(d.get("required_staff", 0), 0)

    use_event_rate = to_int(d.get("use_event_rate", 1), 1)
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in ("", None) else float(stundensatz)
    if use_event_rate == 0:
        stundensatz = None

    db = get_db()
    cur = db.execute(
        """UPDATE event SET
           title=%s, ort=%s, dienstkleidung=%s, auftraggeber=%s,
           start=%s, planned_end_time=%s, frist=%s, status=%s, category=%s, required_staff=%s,
           use_event_rate=%s, stundensatz=%s
           WHERE id=%s""",
        (
            title, ort, dienstkleidung, auftraggeber,
            start, planned_end_time, frist, status, category, required_staff,
            use_event_rate, stundensatz,
            event_id
        )
    )
    if cur.rowcount == 0:
        return jsonify({"error": "Event nicht gefunden"}), 404

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/respond", methods=["POST"])
def respond_event():
    """
    Mitarbeiter: auf offenen Einsatz reagieren.
    - response: 'zugesagt' | 'abgelehnt' | '' (zurückziehen)
    - remark: optional (wird für Chef sichtbar gespeichert)
    Regel: Änderungen sind nur bis zur Frist möglich (falls gesetzt).
    """
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    # ✅ DSGVO: erst Einwilligung, dann Aktionen
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst auf der Startseite in die Datenverarbeitung einwilligen."}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    response_val = (d.get("response") or "").strip()
    remark = (d.get("remark") or "").strip()

    if not event_id:
        return jsonify({"error": "event_id fehlt"}), 400

    if response_val not in ("zugesagt", "abgelehnt", ""):
        return jsonify({"error": "Ungültige Antwort"}), 400

    db = get_db()

    ev = db.execute("SELECT id, frist FROM event WHERE id=%s", (event_id,)).fetchone()
    if not ev:
        return jsonify({"error": "Event nicht gefunden"}), 404

    # Frist prüfen (falls gesetzt)
    frist_raw = (ev["frist"] or "").strip() if "frist" in ev.keys() else ""
    if frist_raw:
        try:
            frist_dt = datetime.fromisoformat(frist_raw)
            if datetime.now() > frist_dt:
                return jsonify({"error": "Die Frist ist abgelaufen. Änderungen sind nicht mehr möglich."}), 400
        except Exception:
            # Wenn das Datum in der DB kaputt ist, sperren wir lieber nicht
            pass

    me = db.execute("SELECT username FROM users WHERE username=%s", (session["username"],)).fetchone()
    if not me:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    # Bestehenden Eintrag prüfen
    existing = db.execute(
        "SELECT status, end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, me["username"])
    ).fetchone()

    # Wenn bereits bestätigt oder Endzeit gesetzt -> nicht über /respond ändern
    if existing:
        if (existing["status"] or "") == "bestätigt" or (existing["end_time"] or "").strip():
            return jsonify({"error": "Dieser Einsatz ist bereits bestätigt/abgerechnet und kann hier nicht mehr geändert werden."}), 400

    # Zurückziehen: Status/Bemerkung wirklich entfernen (NULL), damit im Chef-Dashboard
    # keine "leere Karte" mit Rahmen stehen bleibt.
    if response_val == "":
        if existing:
            db.execute(
                "UPDATE response SET status=NULL, remark=NULL WHERE event_id=%s AND username=%s",
                (event_id, me["username"])
            )
        else:
            # Wenn es noch keinen Eintrag gab, müssen wir nichts anlegen.
            pass
    else:
        if existing:
            db.execute(
                "UPDATE response SET status=%s, remark=%s WHERE event_id=%s AND username=%s",
                (response_val, remark, event_id, me["username"])
            )
        else:
            db.execute(
                "INSERT INTO response (event_id, username, status, remark) VALUES (%s,%s,%s,%s)",
                (event_id, me["username"], response_val, remark)
            )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/confirm", methods=["POST"])
def confirm_event():
    """Chef: Zusage bestätigen oder ablehnen.
    - decision: 'bestätigt' | 'abgelehnt'
    Hinweis: Chef-Ablehnung wird als 'abgelehnt_chef' gespeichert, damit das UI die Fälle unterscheiden kann.
    """
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    username = (d.get("username") or "").strip()
    decision = (d.get("decision") or "").strip()

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    if decision == "bestätigt":
        decision_db = "bestätigt"
    elif decision == "abgelehnt":
        decision_db = "abgelehnt_chef"
    else:
        return jsonify({"error": "Ungültige Entscheidung"}), 400

    db = get_db()
    exists = db.execute(
        "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
        (event_id, username)
    ).fetchone()

    if exists:
        db.execute(
            "UPDATE response SET status=%s WHERE event_id=%s AND username=%s",
            (decision_db, event_id, username)
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, status, remark, start_time, end_time) VALUES (%s,%s,%s,%s,%s,%s)",
            (event_id, username, decision_db, "", "", "")
        )

    db.commit()
    return jsonify({"status": "ok"})


@app.route("/events/endtime", methods=["POST"])
def set_endtime():
    """Mitarbeiter: Endzeit EINMALIG speichern."""
    if session.get("role") != "mitarbeiter":
        return jsonify({"error": "Nicht erlaubt"}), 403

    # ✅ DSGVO: erst Einwilligung, dann Aktionen
    if employee_requires_consent():
        return jsonify({"error":"Bitte zuerst auf der Startseite in die Datenverarbeitung einwilligen."}), 403

    # ✅ DSGVO: Endzeit erst nach Einwilligung
    info = get_user_consent(get_db(), session.get("username"))
    if not bool(info.get("given")):
        return jsonify({"error": "Einwilligung zur Datenverarbeitung ist erforderlich."}), 403


    d = request.json or {}
    event_id = d.get("event_id")
    end_time = (d.get("end_time") or "").strip()

    if not event_id or not end_time:
        return jsonify({"error": "event_id und end_time erforderlich"}), 400

    db = get_db()

    r = db.execute(
        "SELECT end_time FROM response WHERE event_id=%s AND username=%s",
        (event_id, session["username"])
    ).fetchone()

    if r and (r.get("end_time") or "").strip():
        return jsonify({"error": "Endzeit bereits gespeichert"}), 400

    if r:
        db.execute(
            "UPDATE response SET end_time=%s WHERE event_id=%s AND username=%s",
            (end_time, event_id, session["username"])
        )
    else:
        db.execute(
            "INSERT INTO response (event_id, username, end_time) VALUES (%s,%s,%s)",
            (event_id, session["username"], end_time)
        )

    db.commit()
    return jsonify({"success": True})


@app.route("/events/edit_entry", methods=["POST"])
def edit_entry():
    """
    Chef: Zeiten/Bemerkung/Stundensatz-Override pro Mitarbeiter setzen.
    WICHTIG: Wenn Chef start_time oder remark ändert -> Email an den Mitarbeiter.
    """
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    event_id = (d.get("event_id") or "").strip()
    username = (d.get("username") or "").strip()
    start_time = (d.get("start_time") or "").strip()
    end_time = (d.get("end_time") or "").strip()
    remark = (d.get("remark") or "").strip()

    rate_override = d.get("rate_override", None)
    if rate_override in ("", None):
        rate_override = None
    else:
        try:
            rate_override = float(rate_override)
        except Exception:
            return jsonify({"error": "rate_override ungültig"}), 400

    if not event_id:
        return jsonify({"error": "event_id erforderlich"}), 400

    db = get_db()

    old_start = ""
    old_remark = ""

    if username:
        old_row = db.execute(
            "SELECT start_time, remark FROM response WHERE event_id=%s AND username=%s",
            (event_id, username)
        ).fetchone()
        old_start = (old_row.get("start_time") if old_row else "") or ""
        old_remark = (old_row.get("remark") if old_row else "") or ""

        exists = db.execute(
            "SELECT 1 FROM response WHERE event_id=%s AND username=%s",
            (event_id, username)
        ).fetchone()

        if exists:
            db.execute(
                """
                UPDATE response SET
                  start_time    = COALESCE(NULLIF(%s,''), start_time),
                  end_time      = COALESCE(NULLIF(%s,''), end_time),
                  remark        = %s,
                  rate_override = %s
                WHERE event_id=%s AND username=%s
                """,
                (start_time, end_time, remark, rate_override, event_id, username)
            )
        else:
            db.execute(
                """
                INSERT INTO response (event_id, username, status, remark, start_time, end_time, rate_override)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
                (event_id, username, "bestätigt", remark, start_time or "", end_time or "", rate_override)
            )
    else:
        db.execute(
            """
            UPDATE response SET
              end_time      = COALESCE(NULLIF(%s,''), end_time),
              remark        = %s,
              rate_override = %s
            WHERE event_id=%s
            """,
            (end_time, remark, rate_override, event_id)
        )

    db.commit()

    changed_start = bool(start_time) and (start_time != old_start)
    changed_remark = (remark != old_remark)

    if username and (changed_start or changed_remark):
        u = db.execute(
            "SELECT vorname, nachname, email FROM users WHERE username=%s",
            (username,)
        ).fetchone()
        e = db.execute(
            "SELECT title, start, ort, dienstkleidung FROM event WHERE id=%s",
            (event_id,)
        ).fetchone()

        if u and e and (u.get("email") or "").strip():
            employee_name = (f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}").strip() or username
            event_start_dt = ((e.get("start") or "").strip().replace("T", " ")) or "-"
            subject = f"Änderung zu deinem Einsatz: {(e.get('title') or 'Einsatz')}"
            body = build_change_mail(
                employee_name=employee_name,
                event_title=(e.get("title") or "Einsatz"),
                event_start_dt=event_start_dt,
                ort=(e.get("ort") or ""),
                dienstkleidung=(e.get("dienstkleidung") or ""),
                new_start_time=(start_time or old_start),
                new_remark=(remark if changed_remark else ""),
            )
            try:
                send_mail((u.get("email") or "").strip(), subject, body)
            except Exception:
                pass

    return jsonify({"status": "ok"})





@app.route("/events/duplicate", methods=["POST"])
def duplicate_event():
    """Chef/Vorgesetzter: Einsatz duplizieren (stabil & fehlertolerant)."""
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    try:
        d = request.json or {}
        source_id = (d.get("event_id") or "").strip()
        if not source_id:
            return jsonify({"error": "event_id fehlt"}), 400

        dates = d.get("dates") or []
        single_start = (d.get("start") or "").strip()

        db = get_db()
        src = db.execute("SELECT * FROM event WHERE id=%s", (source_id,)).fetchone()
        if not src:
            return jsonify({"error": "Event nicht gefunden"}), 404

        # --- Kategorie sauber normalisieren ---
        src_cat = (src.get("category") or "CP").strip().upper()
        if src_cat not in ("CP", "CV"):
            src_cat = "CP"

        # --- Uhrzeit aus Quelle holen ---
        src_start = (src.get("start") or "").strip()
        src_time = "09:00"
        m = re.match(r"^\d{4}-\d{2}-\d{2}T(\d{2}:\d{2})", src_start)
        if m:
            src_time = m.group(1)

        def insert_new(start_val: str) -> str:
            new_id = str(uuid.uuid4())
            db.execute(
                """
                INSERT INTO event
                  (id,title,ort,dienstkleidung,auftraggeber,start,
                   planned_end_time,frist,status,category,
                   required_staff,use_event_rate,stundensatz)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    new_id,
                    src.get("title") or "",
                    src.get("ort") or "",
                    src.get("dienstkleidung") or "",
                    src.get("auftraggeber") or "",
                    start_val,
                    src.get("planned_end_time") or "",
                    src.get("frist") or "",
                    src.get("status") or "geplant",
                    src_cat,
                    int(src.get("required_staff") or 0),
                    int(src.get("use_event_rate") if src.get("use_event_rate") is not None else 1),
                    src.get("stundensatz"),
                ),
            )
            return new_id

        created_ids = []

        # --- Mehrere Daten ---
        if isinstance(dates, list) and dates:
            for ds in dates:
                ds = (ds or "").strip()
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", ds):
                    continue
                created_ids.append(insert_new(f"{ds}T{src_time}"))

            if not created_ids:
                db.rollback()
                return jsonify({"error": "Keine gültigen Datumswerte"}), 400

            db.commit()
            return jsonify({"status": "ok", "new_event_ids": created_ids}), 200

        # --- Einzeltermin ---
        start_val = single_start or src_start
        if not start_val:
            return jsonify({"error": "start fehlt"}), 400

        new_id = insert_new(start_val)
        db.commit()
        return jsonify({"status": "ok", "new_event_id": new_id}), 200

    except Exception as e:
        try:
            db.rollback()
        except Exception:
            pass
        print("DUPLICATE ERROR:", repr(e))
        return jsonify({"error": "Duplizieren fehlgeschlagen", "detail": str(e)}), 500



@app.route("/events/send_mail_all", methods=["POST"])
def send_mail_all():
    """Chef/Vorgesetzter: Sammel-Mail an alle Mitarbeiter senden.
    Text ist fest vorgegeben (wie in der Anforderung).
    Rückgabe: {"status":"ok","sent":<anzahl>}
    """
    if session.get("role") not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    # "alle Mitarbeiter" = Rolle mitarbeiter (und nur mit gültiger E-Mail)
    cur = db.execute("SELECT vorname, nachname, email FROM users WHERE role=%s", ("mitarbeiter",))
    rows = cur.fetchall() or []

    subject = "Neue Einsätze zum Einbuchen"
    body = (
        "Hallo,\n\n"
        "es wurden neue Einsätze zum Einbuchen im Online-Portal eingestellt.\n\n"
        "Bitte die Rückmeldefrist beachten.\n\n"
        "Viele Grüße\n"
        "CV Planung\n"
    )

    sent = 0
    for u in rows:
        to_addr = (u.get("email") or "").strip()
        if not to_addr:
            continue
        try:
            send_mail(to_addr, subject, body)
            sent += 1
        except Exception:
            # Mail-Fehler sollen die API nicht kaputt machen
            pass

    return jsonify({"status": "ok", "sent": sent})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)






