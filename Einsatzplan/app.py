# app.py
# Flask App – PostgreSQL/Supabase Version (Aufbau wie APP 9), Logik unverändert übernommen aus der SQLite-Version.
#
# Start:
#   export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
#   export SECRET_KEY="."
#   python app.py
#
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g, send_file, abort
import os, uuid, re, json
from datetime import datetime
import io
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from PIL import Image
from urllib.parse import urljoin
from urllib.request import urlopen



def normalize_role(role: str) -> str:
    r = (role or "").strip().lower()
    # akzeptiere Anzeigenamen mit Leerzeichen
    if r in ["planner bbs", "planner_bbs","planer bbs", "planer_bbs"]:
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

def build_welcome_mail(employee_name: str, username: str, password: str, portal_url: str) -> str:
    display_name = (employee_name or "").strip() or username
    lines = [
        f"Hallo {display_name},",
        "",
        "herzlich willkommen beim Casutt Veranstaltungsservice!",
        "",
        "Deine Zugangsdaten:",
        f"Benutzername: {username}",
        f"Passwort: {password}",
        "",
        "Hier geht es zur CV-Planung:",
        portal_url,
        "",
        "Wir freuen uns auf die Zusammenarbeit!",
        "",
        "Viele Grüße",
        "Casutt Veranstaltungsservice",
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

app = Flask(__name__)
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



def normalize_s34a_art(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    # Einheitliche Schreibweise
    if s.lower() == "sachkunde":
        return "Sachkunde"
    return s


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
            s34a TEXT,
            s34a_art TEXT,
            pschein TEXT,
            bewach_id TEXT,
            steuernummer TEXT,
            bsw TEXT,
            sanitaeter TEXT,
            stundensatz DOUBLE PRECISION,

            -- Erweiterung: Personalbogen (intern)
            photo_url TEXT,
            geburtsdatum TEXT,                -- ISO: YYYY-MM-DD
            staatsangehoerigkeit TEXT,
            amtliches_dokument TEXT,          -- 'Personalausweis' | 'Reisepass'
            dokumentennr TEXT,
            ausstellende_behoerde TEXT,
            ausstellungsdatum TEXT,           -- ISO: YYYY-MM-DD

            -- Qualifikationen / Fähigkeiten
            sanitaeter_art TEXT,              -- 'Rettungssanitäter' | 'Rettungsassistent'
            brandschutzhelfer TEXT,           -- ja/nein
            deeskalation TEXT,                -- ja/nein
            gssk TEXT,                        -- ja/nein
            fachkraft TEXT,                   -- ja/nein
            personenschutz TEXT,              -- ja/nein
            behoerdlich TEXT,                 -- ja/nein (behördliche Ausbildung/Studium)
            waffensachkunde TEXT,             -- ja/nein
            fuehrerschein_klasse TEXT,        -- 'B' | 'C' | 'CE' | ''
            sonstige TEXT,
            sprachen TEXT,                    -- CSV oder JSON (Frontend sendet CSV)

            consent_given BOOLEAN DEFAULT FALSE,
            consent_name TEXT,
            consent_date TEXT
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

    db.execute(
        '''
        CREATE TABLE IF NOT EXISTS custom_language (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
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
        ("bewach_id", "ALTER TABLE users ADD COLUMN bewach_id TEXT"),
        ("steuernummer", "ALTER TABLE users ADD COLUMN steuernummer TEXT"),
        ("bsw", "ALTER TABLE users ADD COLUMN bsw TEXT"),
        ("sanitaeter", "ALTER TABLE users ADD COLUMN sanitaeter TEXT"),
        ("stundensatz", "ALTER TABLE users ADD COLUMN stundensatz DOUBLE PRECISION"),
        ("photo_url", "ALTER TABLE users ADD COLUMN photo_url TEXT"),
        ("geburtsdatum", "ALTER TABLE users ADD COLUMN geburtsdatum TEXT"),
        ("staatsangehoerigkeit", "ALTER TABLE users ADD COLUMN staatsangehoerigkeit TEXT"),
        ("amtliches_dokument", "ALTER TABLE users ADD COLUMN amtliches_dokument TEXT"),
        ("dokumentennr", "ALTER TABLE users ADD COLUMN dokumentennr TEXT"),
        ("ausstellende_behoerde", "ALTER TABLE users ADD COLUMN ausstellende_behoerde TEXT"),
        ("ausstellungsdatum", "ALTER TABLE users ADD COLUMN ausstellungsdatum TEXT"),
        ("sanitaeter_art", "ALTER TABLE users ADD COLUMN sanitaeter_art TEXT"),
        ("brandschutzhelfer", "ALTER TABLE users ADD COLUMN brandschutzhelfer TEXT"),
        ("deeskalation", "ALTER TABLE users ADD COLUMN deeskalation TEXT"),
        ("gssk", "ALTER TABLE users ADD COLUMN gssk TEXT"),
        ("fachkraft", "ALTER TABLE users ADD COLUMN fachkraft TEXT"),
        ("personenschutz", "ALTER TABLE users ADD COLUMN personenschutz TEXT"),
        ("behoerdlich", "ALTER TABLE users ADD COLUMN behoerdlich TEXT"),
        ("waffensachkunde", "ALTER TABLE users ADD COLUMN waffensachkunde TEXT"),
        ("fuehrerschein_klasse", "ALTER TABLE users ADD COLUMN fuehrerschein_klasse TEXT"),
        ("sonstige", "ALTER TABLE users ADD COLUMN sonstige TEXT"),
        ("sprachen", "ALTER TABLE users ADD COLUMN sprachen TEXT"),
        ("consent_given", "ALTER TABLE users ADD COLUMN consent_given BOOLEAN DEFAULT FALSE"),
        ("consent_name", "ALTER TABLE users ADD COLUMN consent_name TEXT"),
        ("consent_date", "ALTER TABLE users ADD COLUMN consent_date TEXT"),
        ("acc_locked", "ALTER TABLE users ADD COLUMN acc_locked BOOLEAN DEFAULT FALSE"),
        ("s34a", "ALTER TABLE users ADD COLUMN s34a TEXT"),
        ("s34a_art", "ALTER TABLE users ADD COLUMN s34a_art TEXT"),
        ("pschein", "ALTER TABLE users ADD COLUMN pschein TEXT"),
        ("vorname", "ALTER TABLE users ADD COLUMN vorname TEXT"),
        ("nachname", "ALTER TABLE users ADD COLUMN nachname TEXT"),
        ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'mitarbeiter'"),
        ("password", "ALTER TABLE users ADD COLUMN password TEXT"),
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
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz,photo_url,geburtsdatum,staatsangehoerigkeit,amtliches_dokument,dokumentennr,ausstellende_behoerde,ausstellungsdatum,sanitaeter_art,brandschutzhelfer,deeskalation,gssk,fachkraft,personenschutz,behoerdlich,waffensachkunde,fuehrerschein_klasse,sonstige,sprachen)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
            if bool(u.get("acc_locked") or False):
                session.clear()
                return render_template("login.html", error="Dieser Account ist gesperrt. Bitte wende dich an die Verwaltung.")
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

    me = get_db().execute("SELECT username, acc_locked FROM users WHERE username=%s", (session.get("username"),)).fetchone()
    if me and bool(me.get("acc_locked") or False):
        session.clear()
        return redirect(url_for("login"))

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
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    cur = get_db().execute(
        "SELECT * FROM users WHERE username NOT IN (%s,%s) ORDER BY nachname, vorname",
        ("AdminTest","TestAdmin")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    for u in users:
        if u.get("stundensatz") is None:
            u["stundensatz"] = ""
        u["acc_locked"] = bool(u.get("acc_locked") or False)
        u["sprachen_levels"] = _parse_language_entries(u.get("sprachen") or "")
        u["fuehrerschein_klassen"] = _parse_multi_values(u.get("fuehrerschein_klasse") or "")
    return jsonify(users)


@app.route("/users_public", methods=["GET"])
def users_public():
    """
    Minimaler User-Export (nur Name) für Planung.
    Erlaubt für eingeloggte Rollen inkl. Planer – ohne sensible Felder/Passwörter.
    """
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
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    username = (d.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username ist erforderlich"}), 400

    db = get_db()

    # stundensatz darf leer sein
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    email = (d.get("email") or "").strip()
    password_plain = d.get("password") or ""
    first_name = (d.get("vorname") or "").strip()
    last_name = (d.get("nachname") or "").strip()
    employee_name = f"{first_name} {last_name}".strip() or username

    try:
        db.execute(
            """INSERT INTO users
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz,photo_url,geburtsdatum,staatsangehoerigkeit,amtliches_dokument,dokumentennr,ausstellende_behoerde,ausstellungsdatum,sanitaeter_art,brandschutzhelfer,deeskalation,gssk,fachkraft,personenschutz,behoerdlich,waffensachkunde,fuehrerschein_klasse,sonstige,sprachen,acc_locked)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                username,
                password_plain,
                d.get("role") or "mitarbeiter",
                first_name,
                last_name,
                email,
                d.get("s34a") or "nein",
                normalize_s34a_art(d.get("s34a_art") or ""),
                d.get("pschein") or "nein",
                d.get("bewach_id") or "",
                d.get("steuernummer") or "",
                d.get("bsw") or "nein",
                d.get("sanitaeter") or "nein",
                stundensatz,
                d.get("photo_url") or "",
                (d.get("geburtsdatum") or "").strip(),
                (d.get("staatsangehoerigkeit") or "").strip(),
                (d.get("amtliches_dokument") or "").strip(),
                (d.get("dokumentennr") or "").strip(),
                (d.get("ausstellende_behoerde") or "").strip(),
                (d.get("ausstellungsdatum") or "").strip(),
                (d.get("sanitaeter_art") or "").strip(),
                d.get("brandschutzhelfer") or "nein",
                d.get("deeskalation") or "nein",
                d.get("gssk") or "nein",
                d.get("fachkraft") or "nein",
                d.get("personenschutz") or "nein",
                d.get("behoerdlich") or "nein",
                d.get("waffensachkunde") or "nein",
                (_normalize_multi_text(d.get("fuehrerschein_klasse") or "")).strip(),
                (d.get("sonstige") or "").strip(),
                _serialize_language_entries(d.get("sprachen_levels")) or (d.get("sprachen") or "").strip(),
                bool(d.get("acc_locked") or False),
            ),
        )
        _sync_custom_languages(db, d.get("sprachen") or "")
        db.commit()
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    mail_sent = False
    mail_error = ""
    if email:
        try:
            portal_url = request.host_url.rstrip("/")
            subject = "Willkommen bei Casutt Veranstaltungsservice – deine Zugangsdaten"
            body = build_welcome_mail(employee_name, username, password_plain, portal_url)
            send_mail(email, subject, body)
            mail_sent = True
        except Exception as e:
            mail_error = str(e)

    return jsonify({"status": "ok", "mail_sent": mail_sent, "mail_error": mail_error})
@app.route("/users/rename", methods=["POST"])
def rename_user():
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    old_username = (d.get("old_username") or "").strip()
    new_username = (d.get("new_username") or "").strip()

    if not old_username or not new_username:
        return jsonify({"error": "old_username und new_username erforderlich"}), 400

    db = get_db()

    try:
        old = db.execute("SELECT * FROM users WHERE username=%s", (old_username,)).fetchone()
        if not old:
            return jsonify({"error": "Alter Benutzer nicht gefunden"}), 404

        if db.execute("SELECT 1 FROM users WHERE username=%s", (new_username,)).fetchone():
            return jsonify({"error": "Neuer Benutzername existiert schon"}), 400

        # Wichtig: In SQLite kann ein UPDATE des PK (username) scheitern,
        # wenn es Foreign-Key-Referenzen gibt (response.username -> user.username),
        # da im Schema kein ON UPDATE CASCADE definiert ist.
        # Lösung: neuen User anlegen, Referenzen umhängen, alten User löschen.
        db.execute(
            """INSERT INTO users
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,stundensatz,photo_url,geburtsdatum,staatsangehoerigkeit,amtliches_dokument,dokumentennr,ausstellende_behoerde,ausstellungsdatum,sanitaeter_art,brandschutzhelfer,deeskalation,gssk,fachkraft,personenschutz,behoerdlich,waffensachkunde,fuehrerschein_klasse,sonstige,sprachen,acc_locked)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                new_username,
                old["password"],
                old["role"] or "mitarbeiter",
                old["vorname"] or "",
                old["nachname"] or "",
                (old.get("email") or "").strip(),
                old["s34a"] or "nein",
                normalize_s34a_art(old["s34a_art"] or ""),
                old["pschein"] or "nein",
                old["bewach_id"] or "",
                old["steuernummer"] or "",
                old["bsw"] or "nein",
                old["sanitaeter"] or "nein",
                old["stundensatz"],
                old.get("photo_url") or "",
                (old.get("geburtsdatum") or "").strip(),
                (old.get("staatsangehoerigkeit") or "").strip(),
                (old.get("amtliches_dokument") or "").strip(),
                (old.get("dokumentennr") or "").strip(),
                (old.get("ausstellende_behoerde") or "").strip(),
                (old.get("ausstellungsdatum") or "").strip(),
                (old.get("sanitaeter_art") or "").strip(),
                old.get("brandschutzhelfer") or "nein",
                old.get("deeskalation") or "nein",
                old.get("gssk") or "nein",
                old.get("fachkraft") or "nein",
                old.get("personenschutz") or "nein",
                old.get("behoerdlich") or "nein",
                old.get("waffensachkunde") or "nein",
                (old.get("fuehrerschein_klasse") or "").strip(),
                (old.get("sonstige") or "").strip(),
                (old.get("sprachen") or "").strip(),
                bool(old.get("acc_locked") or False)
            )
        )

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
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    db = get_db()

    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    updates = dict(u)
    for k in ["vorname", "nachname", "email", "role", "s34a", "s34a_art", "pschein",
              "bewach_id", "steuernummer", "bsw", "sanitaeter",
              "photo_url", "geburtsdatum", "staatsangehoerigkeit", "amtliches_dokument", "dokumentennr",
              "ausstellende_behoerde", "ausstellungsdatum",
              "sanitaeter_art", "brandschutzhelfer", "deeskalation", "gssk", "fachkraft", "personenschutz",
              "behoerdlich", "waffensachkunde", "fuehrerschein_klasse", "sonstige", "sprachen", "sprachen_levels", "acc_locked"]:
        if k in d:
            # ✅ Bugfix: Sachkunde darf beim Speichern der E-Mail nicht verschwinden.
            # Wenn Frontend ein leeres Feld sendet, behalten wir den bisherigen Wert.
            if k == "s34a_art":
                newv = normalize_s34a_art(d.get(k))
                if str(newv or "").strip() == "":
                    continue
                updates[k] = newv
            else:
                updates[k] = d[k]

    if "password" in d and d["password"] is not None:
        updates["password"] = d["password"]

    if "stundensatz" in d:
        updates["stundensatz"] = None if d["stundensatz"] in ("", None) else float(d["stundensatz"])

    db.execute(
        """UPDATE users SET
           password=%s, role=%s, vorname=%s, nachname=%s, email=%s, s34a=%s, s34a_art=%s, pschein=%s,
           bewach_id=%s, steuernummer=%s, bsw=%s, sanitaeter=%s, stundensatz=%s,
           photo_url=%s, geburtsdatum=%s, staatsangehoerigkeit=%s, amtliches_dokument=%s, dokumentennr=%s,
           ausstellende_behoerde=%s, ausstellungsdatum=%s,
           sanitaeter_art=%s, brandschutzhelfer=%s, deeskalation=%s, gssk=%s, fachkraft=%s, personenschutz=%s,
           behoerdlich=%s, waffensachkunde=%s, fuehrerschein_klasse=%s, sonstige=%s, sprachen=%s, acc_locked=%s
           WHERE username=%s""",
        (
            updates["password"], updates["role"], updates["vorname"], updates["nachname"], updates.get("email") or "",
            updates["s34a"], updates["s34a_art"], updates["pschein"],
            updates["bewach_id"], updates["steuernummer"], updates["bsw"], updates["sanitaeter"],
            updates["stundensatz"],
            updates.get("photo_url") or "",
            updates.get("geburtsdatum") or "",
            updates.get("staatsangehoerigkeit") or "",
            updates.get("amtliches_dokument") or "",
            updates.get("dokumentennr") or "",
            updates.get("ausstellende_behoerde") or "",
            updates.get("ausstellungsdatum") or "",
            updates.get("sanitaeter_art") or "",
            updates.get("brandschutzhelfer") or "nein",
            updates.get("deeskalation") or "nein",
            updates.get("gssk") or "nein",
            updates.get("fachkraft") or "nein",
            updates.get("personenschutz") or "nein",
            updates.get("behoerdlich") or "nein",
            updates.get("waffensachkunde") or "nein",
            _normalize_multi_text(updates.get("fuehrerschein_klasse") or ""),
            updates.get("sonstige") or "",
            _serialize_language_entries(updates.get("sprachen_levels")) or ", ".join(_parse_languages(updates.get("sprachen") or "")),
            bool(updates.get("acc_locked") or False),
            username
        )
    )
    _sync_custom_languages(db, updates.get("sprachen") or "")
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/custom_languages", methods=["GET"])
def get_custom_languages():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    rows = get_db().execute("SELECT name FROM custom_language ORDER BY LOWER(name), name").fetchall()
    return jsonify([str(r.get("name") or "").strip() for r in rows if str(r.get("name") or "").strip()])


@app.route("/custom_languages", methods=["POST"])
def add_custom_language():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    name = _normalize_language_label(d.get("name") or "")
    if not name:
        return jsonify({"error": "Bitte eine Sprache eingeben."}), 400
    db = get_db()
    db.execute("INSERT INTO custom_language (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (name,))
    db.commit()
    return jsonify({"status": "ok", "name": name})


@app.route("/custom_languages/<path:name>", methods=["DELETE"])
def delete_custom_language(name):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    lang = _normalize_language_label(name)
    db = get_db()
    db.execute("DELETE FROM custom_language WHERE name=%s", (lang,))
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>/lock", methods=["POST"])
def lock_user(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    d = request.json or {}
    locked = bool(d.get("locked") or False)
    db = get_db()
    u = db.execute("SELECT username FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404
    db.execute("UPDATE users SET acc_locked=%s WHERE username=%s", (locked, username))
    db.commit()
    return jsonify({"status": "ok", "acc_locked": locked})


# ---------------- Mitarbeiter: Foto Upload & PDF-Auszug ----------------
UPLOAD_DIR = os.path.join(app.root_path, "static", "uploads", "users")
os.makedirs(UPLOAD_DIR, exist_ok=True)

ALLOWED_PHOTO_EXT = {".jpg", ".jpeg", ".png", ".webp"}


def _safe_photo_ext(filename: str) -> str:
    ext = os.path.splitext((filename or "").lower())[1]
    return ext if ext in ALLOWED_PHOTO_EXT else ".jpg"


@app.route("/users/<username>/photo", methods=["POST"])
def upload_user_photo(username):
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    if "photo" not in request.files:
        return jsonify({"error": "Datei-Feld 'photo' fehlt"}), 400

    f = request.files["photo"]
    if not f or not (f.filename or "").strip():
        return jsonify({"error": "Keine Datei gewählt"}), 400

    db = get_db()
    u = db.execute("SELECT username FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    ext = _safe_photo_ext(f.filename)

    # webp speichern wir als png, damit Pillow/PDF stabil bleiben
    final_ext = ".png" if ext == ".webp" else ext
    fn = f"{username}_{uuid.uuid4().hex}{final_ext}"
    abs_path = os.path.join(UPLOAD_DIR, fn)

    try:
        with Image.open(f.stream) as img:
            fmt = "PNG" if final_ext == ".png" else "JPEG"
            save_img = img.convert("RGBA") if fmt == "PNG" else img.convert("RGB")
            save_img.save(abs_path, format=fmt)
    except Exception:
        f.stream.seek(0)
        f.save(abs_path)

    rel_url = f"/static/uploads/users/{fn}"
    db.execute("UPDATE users SET photo_url=%s WHERE username=%s", (rel_url, username))
    db.commit()
    return jsonify({"status": "ok", "photo_url": rel_url})


def _yesno_label(v: str) -> str:
    return "Ja" if str(v or "").strip().lower() == "ja" else "Nein"


LANGUAGE_LEVEL_OPTIONS = [
    "Grundkenntnisse",
    "Fortgeschritten",
    "Verhandlungssicher in Wort und Schrift",
    "Muttersprache",
]


def _normalize_language_label(value: str) -> str:
    return re.sub(r"\s{2,}", " ", str(value or "").strip())


def _normalize_multi_text(raw_value):
    if raw_value is None:
        return ""
    if isinstance(raw_value, list):
        parts = raw_value
    else:
        s = str(raw_value or "").strip()
        if not s:
            return ""
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                parts = parsed
            else:
                parts = re.split(r"[,;\n]+", s)
        except Exception:
            parts = re.split(r"[,;\n]+", s)

    items = []
    seen = set()
    for part in parts:
        label = re.sub(r"\s{2,}", " ", str(part or "").strip())
        key = label.casefold()
        if label and key not in seen:
            seen.add(key)
            items.append(label)
    return ", ".join(items)


def _parse_multi_values(raw_value):
    return [v for v in _normalize_multi_text(raw_value).split(", ") if v]


def _parse_language_entries(raw):
    entries = []
    seen = set()

    if raw is None:
        return entries

    if isinstance(raw, (list, tuple)):
        payload = raw
    else:
        text = str(raw or "").strip()
        if not text:
            return entries
        payload = None
        if text.startswith("[") or text.startswith("{"):
            try:
                payload = json.loads(text)
            except Exception:
                payload = None
        if payload is None:
            payload = [part.strip() for part in text.split(",") if part.strip()]

    if isinstance(payload, dict):
        if isinstance(payload.get("items"), list):
            payload = payload.get("items")
        else:
            payload = [{"name": k, "level": v} for k, v in payload.items()]

    for item in payload if isinstance(payload, list) else []:
        if isinstance(item, dict):
            name = _normalize_language_label(item.get("name") or item.get("language") or item.get("label") or "")
            level = str(item.get("level") or "").strip()
        else:
            name = _normalize_language_label(item)
            level = ""
        key = name.casefold()
        if name and key not in seen:
            seen.add(key)
            entries.append({"name": name, "level": level if level in LANGUAGE_LEVEL_OPTIONS else ""})
    return entries


def _serialize_language_entries(entries):
    normalized = []
    seen = set()
    for item in entries or []:
        if isinstance(item, dict):
            name = _normalize_language_label(item.get("name") or item.get("language") or item.get("label") or "")
            level = str(item.get("level") or "").strip()
        else:
            name = _normalize_language_label(item)
            level = ""
        key = name.casefold()
        if name and key not in seen:
            seen.add(key)
            normalized.append({"name": name, "level": level if level in LANGUAGE_LEVEL_OPTIONS else ""})
    if not normalized:
        return ""
    return json.dumps(normalized, ensure_ascii=False)


def _parse_languages(raw: str):
    return [item["name"] for item in _parse_language_entries(raw)]


def _format_languages(raw: str):
    entries = _parse_language_entries(raw)
    parts = []
    for item in entries:
        if item.get("level"):
            parts.append(f'{item["name"]} ({item["level"]})')
        else:
            parts.append(item["name"])
    return ", ".join(parts)


def _sync_custom_languages(db, raw_languages) -> None:
    for lang in _parse_languages(raw_languages):
        db.execute("INSERT INTO custom_language (name) VALUES (%s) ON CONFLICT (name) DO NOTHING", (lang,))


def _image_reader_from_path(abs_photo: str):
    if not abs_photo or not os.path.isfile(abs_photo):
        return None
    try:
        with Image.open(abs_photo) as pil_img:
            if pil_img.mode not in ("RGB", "L"):
                pil_img = pil_img.convert("RGB")
            else:
                pil_img = pil_img.copy()
            return ImageReader(pil_img)
    except Exception:
        try:
            return ImageReader(abs_photo)
        except Exception:
            return None


def _image_reader_from_bytes(raw: bytes):
    if not raw:
        return None
    try:
        with Image.open(io.BytesIO(raw)) as pil_img:
            if pil_img.mode not in ("RGB", "L"):
                pil_img = pil_img.convert("RGB")
            else:
                pil_img = pil_img.copy()
            return ImageReader(pil_img)
    except Exception:
        try:
            return ImageReader(io.BytesIO(raw))
        except Exception:
            return None


def _safe_photo_reader(photo_url: str, host_url: str = ""):
    photo_url = str(photo_url or "").strip()
    if not photo_url:
        return None, "Kein Bild hinterlegt"

    # 1) Lokale /static-Pfade bevorzugen
    normalized = photo_url
    if normalized.startswith("http://") or normalized.startswith("https://"):
        absolute_url = normalized
    else:
        if not normalized.startswith("/"):
            normalized = "/" + normalized
        absolute_url = urljoin(host_url or "", normalized)

        candidate_paths = []
        if normalized.startswith("/static/"):
            rel_path = normalized.lstrip("/")
            candidate_paths.extend([
                os.path.join(app.root_path, rel_path),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), rel_path),
                os.path.join(os.getcwd(), rel_path),
            ])
        else:
            candidate_paths.extend([
                normalized,
                os.path.join(app.root_path, normalized.lstrip("/")),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), normalized.lstrip("/")),
            ])

        seen = set()
        for candidate in candidate_paths:
            candidate = os.path.abspath(candidate)
            if candidate in seen:
                continue
            seen.add(candidate)
            reader = _image_reader_from_path(candidate)
            if reader:
                return reader, None

    # 2) Fallback: per HTTP laden (hilft auf Hosting-Plattformen mit anderem Dateisystem)
    try:
        if absolute_url:
            with urlopen(absolute_url, timeout=10) as resp:
                if getattr(resp, "status", 200) >= 400:
                    return None, "Bild konnte nicht geladen werden"
                raw = resp.read()
            reader = _image_reader_from_bytes(raw)
            if reader:
                return reader, None
    except Exception:
        pass

    return None, "Bilddatei nicht gefunden"


def _format_date_de(value: str, fallback: str = "-") -> str:
    s = str(value or "").strip()
    if not s:
        return fallback
    for parser in (
        lambda v: datetime.fromisoformat(v.replace("Z", "")),
        lambda v: datetime.strptime(v, "%Y-%m-%d"),
    ):
        try:
            return parser(s).strftime("%d.%m.%Y")
        except Exception:
            pass
    return s


def _draw_wrapped_text(c, text: str, x: float, y: float, max_width: float, font_name: str = "Helvetica", font_size: int = 10, leading: float = None):
    text = str(text or "").strip() or "-"
    leading = leading or (font_size * 1.35)
    c.setFont(font_name, font_size)
    words = text.split()
    lines = []
    current = ""
    for word in words:
        trial = f"{current} {word}".strip()
        if c.stringWidth(trial, font_name, font_size) <= max_width:
            current = trial
        else:
            if current:
                lines.append(current)
            current = word
    if current:
        lines.append(current)
    if not lines:
        lines = ["-"]
    for line in lines:
        c.drawString(x, y, line)
        y -= leading
    return y


@app.route("/users/<username>/client_pdf", methods=["GET"])
def user_client_pdf(username):
    # ✅ Chef/Vorgesetzter dürfen exportieren
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        abort(404)

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    w, h = A4

    margin = 16 * mm
    content_w = w - (2 * margin)
    header_h = 22 * mm

    c.setFillColor(colors.HexColor("#1f2937"))
    c.roundRect(margin, h - margin - header_h, content_w, header_h, 5 * mm, stroke=0, fill=1)
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(margin + 8 * mm, h - margin - 8 * mm, "Mitarbeiterprofil")
    c.setFont("Helvetica", 9)
    c.drawString(margin + 8 * mm, h - margin - 14 * mm, f"Export am {datetime.now().strftime('%d.%m.%Y %H:%M')}")

    full_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip() or username
    card_top = h - margin - header_h - 8 * mm
    card_h = 55 * mm
    c.setFillColor(colors.white)
    c.setStrokeColor(colors.HexColor("#d1d5db"))
    c.roundRect(margin, card_top - card_h, content_w, card_h, 4 * mm, stroke=1, fill=1)

    text_x = margin + 8 * mm
    text_y = card_top - 10 * mm
    photo_reader, photo_error = _safe_photo_reader(u.get("photo_url") or "", request.host_url)
    photo_w = 34 * mm
    photo_h = 42 * mm
    photo_x = w - margin - 8 * mm - photo_w
    photo_y = card_top - 8 * mm - photo_h

    c.setFillColor(colors.HexColor("#f3f4f6"))
    c.roundRect(photo_x - 2 * mm, photo_y - 2 * mm, photo_w + 4 * mm, photo_h + 4 * mm, 3 * mm, stroke=0, fill=1)
    if photo_reader:
        c.drawImage(photo_reader, photo_x, photo_y, photo_w, photo_h, preserveAspectRatio=True, mask='auto', anchor='c')
    else:
        c.setFillColor(colors.HexColor("#6b7280"))
        c.setFont("Helvetica-Bold", 9)
        c.drawCentredString(photo_x + (photo_w / 2), photo_y + (photo_h / 2) + 2 * mm, "Kein Bild")
        c.setFont("Helvetica", 7)
        c.drawCentredString(photo_x + (photo_w / 2), photo_y + (photo_h / 2) - 2 * mm, photo_error or "Nicht verfügbar")

    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(text_x, text_y, full_name)
    c.setFont("Helvetica", 10)
    c.drawString(text_x, text_y - 7 * mm, f"Benutzername: {username}")
    c.drawString(text_x, text_y - 13 * mm, f"E-Mail: {(u.get('email') or '-').strip() or '-'}")
    c.drawString(text_x, text_y - 19 * mm, f"Geburtsdatum: {_format_date_de(u.get('geburtsdatum') or '', '-')}")
    c.drawString(text_x, text_y - 25 * mm, f"Staatsangehörigkeit: {(u.get('staatsangehoerigkeit') or '-').strip() or '-'}")

    section_top = card_top - card_h - 8 * mm
    col_gap = 8 * mm
    col_w = (content_w - col_gap) / 2

    def draw_section(x, top_y, title, rows):
        title_h = 8 * mm
        row_h = 8 * mm
        total_h = title_h + (len(rows) * row_h)
        c.setFillColor(colors.white)
        c.setStrokeColor(colors.HexColor("#d1d5db"))
        c.roundRect(x, top_y - total_h, col_w, total_h, 4 * mm, stroke=1, fill=1)
        c.setFillColor(colors.HexColor("#eef2ff"))
        c.roundRect(x + 1.5 * mm, top_y - title_h + 1.5 * mm, col_w - 3 * mm, title_h - 3 * mm, 3 * mm, stroke=0, fill=1)
        c.setFillColor(colors.HexColor("#111827"))
        c.setFont("Helvetica-Bold", 11)
        c.drawString(x + 4 * mm, top_y - 5.5 * mm, title)
        y = top_y - title_h - 5.5 * mm
        for label, value in rows:
            c.setFillColor(colors.HexColor("#6b7280"))
            c.setFont("Helvetica-Bold", 9)
            c.drawString(x + 4 * mm, y, f"{label}:")
            c.setFillColor(colors.black)
            c.setFont("Helvetica", 9)
            c.drawString(x + 30 * mm, y, str(value or "-"))
            y -= row_h
        return top_y - total_h

    s34a = str(u.get("s34a") or "").strip().lower()
    s34a_txt = "Ja" if s34a == "ja" else "Nein"
    s34a_art = (u.get("s34a_art") or "").strip()
    if s34a == "ja" and s34a_art:
        s34a_txt += f" ({s34a_art})"

    left_bottom = draw_section(margin, section_top, "Basisdaten", [("§ 34a GewO", s34a_txt), ("Bewacher-ID", (u.get("bewach_id") or "-").strip() or "-"), ("P-Schein", _yesno_label(u.get("pschein"))), ("BSW", _yesno_label(u.get("bsw"))), ("SVS", (f"{float(u.get('stundensatz')):.2f} €/h".replace('.', ',')) if u.get("stundensatz") not in (None, "") else "-")])

    san_txt = "Nein"
    if str(u.get("sanitaeter") or "").strip().lower() == "ja":
        art = (u.get("sanitaeter_art") or "").strip()
        san_txt = f"Ja{(' (' + art + ')') if art else ''}"

    right_bottom = draw_section(margin + col_w + col_gap, section_top, "Qualifikationen", [("Sanitätsdienst", san_txt), ("Brandschutzhelfer/in", _yesno_label(u.get("brandschutzhelfer"))), ("Deeskalation", _yesno_label(u.get("deeskalation"))), ("GSSK", _yesno_label(u.get("gssk"))), ("Fachkraft S&S", _yesno_label(u.get("fachkraft"))), ("Personenschutz", _yesno_label(u.get("personenschutz"))), ("Waffensachkunde", _yesno_label(u.get("waffensachkunde"))), ("Behördlich/Studium", _yesno_label(u.get("behoerdlich"))), ("Führerschein", ", ".join(_parse_multi_values(u.get("fuehrerschein_klasse") or "")) or "-")])

    box_top = min(left_bottom, right_bottom) - 8 * mm
    box_h = 34 * mm
    c.setFillColor(colors.white)
    c.setStrokeColor(colors.HexColor("#d1d5db"))
    c.roundRect(margin, box_top - box_h, content_w, box_h, 4 * mm, stroke=1, fill=1)
    c.setFillColor(colors.HexColor("#ecfdf5"))
    c.roundRect(margin + 1.5 * mm, box_top - 8 * mm + 1.5 * mm, content_w - 3 * mm, 8 * mm - 3 * mm, 3 * mm, stroke=0, fill=1)
    c.setFillColor(colors.HexColor("#111827"))
    c.setFont("Helvetica-Bold", 11)
    c.drawString(margin + 4 * mm, box_top - 5.5 * mm, "Fremdsprachen & Hinweise")

    languages = _format_languages(u.get("sprachen") or "") or "-"
    y = box_top - 12 * mm
    c.setFillColor(colors.HexColor("#6b7280"))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 4 * mm, y, "Sprachen:")
    c.setFillColor(colors.black)
    y = _draw_wrapped_text(c, languages, margin + 28 * mm, y, content_w - 34 * mm, "Helvetica", 9, 4.5 * mm)
    c.setFillColor(colors.HexColor("#6b7280"))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 4 * mm, y - 1 * mm, "Sonstige:")
    c.setFillColor(colors.black)
    _draw_wrapped_text(c, (u.get("sonstige") or "-").strip() or "-", margin + 28 * mm, y - 1 * mm, content_w - 34 * mm, "Helvetica", 9, 4.5 * mm)

    c.setFillColor(colors.HexColor("#6b7280"))
    c.setFont("Helvetica", 8)
    c.drawRightString(w - margin, 9 * mm, "Casutt Planungsportal – Mitarbeiterprofil-Auszug")

    c.showPage()
    c.save()

    buf.seek(0)
    filename = f"mitarbeiter_{username}_auszug.pdf"
    return send_file(buf, mimetype="application/pdf", as_attachment=True, download_name=filename)


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

    if not event_id or not username:
        return jsonify({"error": "event_id und username erforderlich"}), 400

    db = get_db()

    # --- ALT-WERTE holen (für Change-Detection) ---
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

    db.commit()

    # --- ÄNDERUNG erkennen ---
    changed_start = bool(start_time) and (start_time != old_start)
    changed_remark = (remark != old_remark)

    if changed_start or changed_remark:
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
                # Mail-Fehler sollen die API nicht kaputt machen
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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)







