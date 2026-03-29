# app.py
# Flask App – PostgreSQL/Supabase Version (Aufbau wie APP 9), Logik unverändert übernommen aus der SQLite-Version.
#
# Start:
#   export DATABASE_URL="postgresql://user:pass@host:5432/dbname?sslmode=require"
#   export SECRET_KEY="."
#   python app.py
#
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, g
import os, uuid, re, io, json
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
        "herzlich willkommen beim",
        "Casutt Veranstaltungsservice!",
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
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from reportlab.pdfbase.pdfmetrics import stringWidth

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


def yesno(v, default="nein"):
    s = str(v or "").strip().lower()
    return "ja" if s in ("1", "true", "ja", "yes", "on") else default


def parse_language_skills(value):
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def dump_language_skills(value):
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except Exception:
            return json.dumps({}, ensure_ascii=False)
    return json.dumps(value or {}, ensure_ascii=False)




def clean_image_data(value):
    value = (value or "").strip()
    if not value:
        return ""
    if value.startswith("data:image/") and ";base64," in value:
        return value
    return ""

def normalize_user_payload(d):
    language_skills = d.get("language_skills") or {}
    if isinstance(language_skills, str):
        language_skills = parse_language_skills(language_skills)

    cleaned_languages = {}
    for lang, level in (language_skills or {}).items():
        lang_name = str(lang or "").strip()
        level_name = str(level or "").strip()
        if lang_name and level_name:
            cleaned_languages[lang_name] = level_name

    return {
        "language_skills": dump_language_skills(cleaned_languages),
        "brandschutzhelfer": yesno(d.get("brandschutzhelfer")),
        "deeskalation": yesno(d.get("deeskalation")),
        "gssk": yesno(d.get("gssk")),
        "fachkraft_ss": yesno(d.get("fachkraft_ss")),
        "personenschutz": yesno(d.get("personenschutz")),
        "waffensachkunde": yesno(d.get("waffensachkunde")),
        "behoerdlich_studium": yesno(d.get("behoerdlich_studium")),
        "fuehrerschein": yesno(d.get("fuehrerschein")),
        "fuehrerschein_klassen": (d.get("fuehrerschein_klassen") or "").strip(),
        "image_data": clean_image_data(d.get("image_data")),
    }


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
            s34a TEXT,
            s34a_art TEXT,
            pschein TEXT,
            bewach_id TEXT,
            steuernummer TEXT,
            bsw TEXT,
            sanitaeter TEXT,
            bemerkung TEXT,
            is_locked BOOLEAN DEFAULT FALSE,
            stundensatz DOUBLE PRECISION,
            consent_given BOOLEAN DEFAULT FALSE,
            consent_name TEXT,
            consent_date TEXT,
            language_skills TEXT,
            brandschutzhelfer TEXT DEFAULT 'nein',
            deeskalation TEXT DEFAULT 'nein',
            gssk TEXT DEFAULT 'nein',
            fachkraft_ss TEXT DEFAULT 'nein',
            personenschutz TEXT DEFAULT 'nein',
            waffensachkunde TEXT DEFAULT 'nein',
            behoerdlich_studium TEXT DEFAULT 'nein',
            fuehrerschein TEXT DEFAULT 'nein',
            fuehrerschein_klassen TEXT,
            image_data TEXT
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
        CREATE TABLE IF NOT EXISTS board_posts (
            id SERIAL PRIMARY KEY,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL,
            created_by TEXT NOT NULL
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
        ("bewach_id", "ALTER TABLE users ADD COLUMN bewach_id TEXT"),
        ("steuernummer", "ALTER TABLE users ADD COLUMN steuernummer TEXT"),
        ("bsw", "ALTER TABLE users ADD COLUMN bsw TEXT"),
        ("sanitaeter", "ALTER TABLE users ADD COLUMN sanitaeter TEXT"),
        ("bemerkung", "ALTER TABLE users ADD COLUMN bemerkung TEXT"),
        ("is_locked", "ALTER TABLE users ADD COLUMN is_locked BOOLEAN DEFAULT FALSE"),
        ("stundensatz", "ALTER TABLE users ADD COLUMN stundensatz DOUBLE PRECISION"),
        ("consent_given", "ALTER TABLE users ADD COLUMN consent_given BOOLEAN DEFAULT FALSE"),
        ("consent_name", "ALTER TABLE users ADD COLUMN consent_name TEXT"),
        ("consent_date", "ALTER TABLE users ADD COLUMN consent_date TEXT"),
        ("s34a", "ALTER TABLE users ADD COLUMN s34a TEXT"),
        ("s34a_art", "ALTER TABLE users ADD COLUMN s34a_art TEXT"),
        ("pschein", "ALTER TABLE users ADD COLUMN pschein TEXT"),
        ("vorname", "ALTER TABLE users ADD COLUMN vorname TEXT"),
        ("nachname", "ALTER TABLE users ADD COLUMN nachname TEXT"),
        ("role", "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'mitarbeiter'"),
        ("password", "ALTER TABLE users ADD COLUMN password TEXT"),
        ("language_skills", "ALTER TABLE users ADD COLUMN language_skills TEXT"),
        ("brandschutzhelfer", "ALTER TABLE users ADD COLUMN brandschutzhelfer TEXT DEFAULT 'nein'"),
        ("deeskalation", "ALTER TABLE users ADD COLUMN deeskalation TEXT DEFAULT 'nein'"),
        ("gssk", "ALTER TABLE users ADD COLUMN gssk TEXT DEFAULT 'nein'"),
        ("fachkraft_ss", "ALTER TABLE users ADD COLUMN fachkraft_ss TEXT DEFAULT 'nein'"),
        ("personenschutz", "ALTER TABLE users ADD COLUMN personenschutz TEXT DEFAULT 'nein'"),
        ("waffensachkunde", "ALTER TABLE users ADD COLUMN waffensachkunde TEXT DEFAULT 'nein'"),
        ("behoerdlich_studium", "ALTER TABLE users ADD COLUMN behoerdlich_studium TEXT DEFAULT 'nein'"),
        ("fuehrerschein", "ALTER TABLE users ADD COLUMN fuehrerschein TEXT DEFAULT 'nein'"),
        ("fuehrerschein_klassen", "ALTER TABLE users ADD COLUMN fuehrerschein_klassen TEXT"),
        ("image_data", "ALTER TABLE users ADD COLUMN image_data TEXT"),
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
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                "",          # bemerkung
                False,       # is_locked
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
            if bool(u.get("is_locked") or False):
                return render_template("login.html", error="Account ist gesperrt")
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


# ---------------- Board / Startseite ----------------
@app.route("/board", methods=["GET"])
def get_board_posts():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    db = get_db()
    cur = db.execute(
        "SELECT id, content, created_at, created_by FROM board_posts ORDER BY id DESC LIMIT 50"
    )
    return jsonify([row_to_dict(r) for r in cur.fetchall()])


@app.route("/board", methods=["POST"])
def add_board_post():
    if "username" not in session:
        return jsonify({"error": "Nicht eingeloggt"}), 403

    role = normalize_role(session.get("role") or "")
    if role not in ["chef", "vorgesetzter", "vorgesetzter_cp"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    content = (d.get("content") or "").strip()
    if not content:
        return jsonify({"error": "Bitte einen Text eingeben."}), 400

    if len(content) > 5000:
        return jsonify({"error": "Der Beitrag ist zu lang."}), 400

    db = get_db()
    db.execute(
        "INSERT INTO board_posts (content, created_at, created_by) VALUES (%s, %s, %s)",
        (content, datetime.now().isoformat(timespec="seconds"), session.get("username")),
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
        u["language_skills"] = parse_language_skills(u.get("language_skills"))
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
        "SELECT username, vorname, nachname FROM users WHERE username NOT IN (%s,%s) AND COALESCE(is_locked, FALSE)=FALSE ORDER BY nachname, vorname",
        ("AdminTest", "TestAdmin")
    )
    users = [row_to_dict(r) for r in cur.fetchall()]
    return jsonify(users)


@app.route("/users", methods=["POST"])
def add_user():
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    d = request.json or {}
    username = (d.get("username") or "").strip()
    if not username:
        return jsonify({"error": "username ist erforderlich"}), 400

    db = get_db()
    stundensatz = d.get("stundensatz")
    stundensatz = None if stundensatz in (None, "") else float(stundensatz)

    password = d.get("password") or ""
    email = (d.get("email") or "").strip()
    employee_name = f"{(d.get('vorname') or '').strip()} {(d.get('nachname') or '').strip()}".strip() or username
    extra = normalize_user_payload(d)

    try:
        db.execute(
            """INSERT INTO users
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz,
                language_skills,brandschutzhelfer,deeskalation,gssk,fachkraft_ss,personenschutz,waffensachkunde,behoerdlich_studium,fuehrerschein,fuehrerschein_klassen,image_data)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (
                username,
                password,
                d.get("role") or "mitarbeiter",
                d.get("vorname") or "",
                d.get("nachname") or "",
                email,
                d.get("s34a") or "nein",
                normalize_s34a_art(d.get("s34a_art") or ""),
                d.get("pschein") or "nein",
                d.get("bewach_id") or "",
                d.get("steuernummer") or "",
                d.get("bsw") or "nein",
                d.get("sanitaeter") or "nein",
                d.get("bemerkung") or "",
                False,
                stundensatz,
                extra["language_skills"],
                extra["brandschutzhelfer"],
                extra["deeskalation"],
                extra["gssk"],
                extra["fachkraft_ss"],
                extra["personenschutz"],
                extra["waffensachkunde"],
                extra["behoerdlich_studium"],
                extra["fuehrerschein"],
                extra["fuehrerschein_klassen"],
                extra["image_data"],
            ),
        )
        db.commit()
    except Exception as e:
        db.rollback()
        return jsonify({"error": str(e)}), 500

    mail_sent = False
    mail_error = ""
    if email:
        subject = "Deine Zugangsdaten zum Portal"
        body = build_welcome_mail(employee_name, username, password)
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
               (username,password,role,vorname,nachname,email,s34a,s34a_art,pschein,bewach_id,steuernummer,bsw,sanitaeter,bemerkung,is_locked,stundensatz,
                language_skills,brandschutzhelfer,deeskalation,gssk,fachkraft_ss,personenschutz,waffensachkunde,behoerdlich_studium,fuehrerschein,fuehrerschein_klassen,image_data,
                consent_given,consent_name,consent_date)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
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
                old.get("bemerkung") or "",
                bool(old.get("is_locked") or False),
                old.get("stundensatz"),
                old.get("language_skills") or dump_language_skills({}),
                old.get("brandschutzhelfer") or "nein",
                old.get("deeskalation") or "nein",
                old.get("gssk") or "nein",
                old.get("fachkraft_ss") or "nein",
                old.get("personenschutz") or "nein",
                old.get("waffensachkunde") or "nein",
                old.get("behoerdlich_studium") or "nein",
                old.get("fuehrerschein") or "nein",
                old.get("fuehrerschein_klassen") or "",
                old.get("image_data") or "",
                bool(old.get("consent_given") or False),
                old.get("consent_name") or "",
                old.get("consent_date") or "",
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
              "bewach_id", "steuernummer", "bsw", "sanitaeter", "bemerkung",
              "brandschutzhelfer", "deeskalation", "gssk", "fachkraft_ss", "personenschutz",
              "waffensachkunde", "behoerdlich_studium", "fuehrerschein", "fuehrerschein_klassen", "image_data"]:
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

    if "language_skills" in d:
        updates["language_skills"] = normalize_user_payload(d)["language_skills"]

    if "image_data" in d:
        updates["image_data"] = clean_image_data(d.get("image_data"))

    extra_updates = normalize_user_payload(d)
    for k in ["brandschutzhelfer", "deeskalation", "gssk", "fachkraft_ss", "personenschutz",
              "waffensachkunde", "behoerdlich_studium", "fuehrerschein", "fuehrerschein_klassen", "image_data"]:
        if k in d:
            updates[k] = extra_updates[k]

    db.execute(
        """UPDATE users SET
           password=%s, role=%s, vorname=%s, nachname=%s, email=%s, s34a=%s, s34a_art=%s, pschein=%s,
           bewach_id=%s, steuernummer=%s, bsw=%s, sanitaeter=%s, bemerkung=%s, stundensatz=%s,
           language_skills=%s, brandschutzhelfer=%s, deeskalation=%s, gssk=%s, fachkraft_ss=%s,
           personenschutz=%s, waffensachkunde=%s, behoerdlich_studium=%s, fuehrerschein=%s, fuehrerschein_klassen=%s, image_data=%s
           WHERE username=%s""",
        (
            updates["password"], updates["role"], updates["vorname"], updates["nachname"], updates.get("email") or "",
            updates["s34a"], updates["s34a_art"], updates["pschein"],
            updates["bewach_id"], updates["steuernummer"], updates["bsw"], updates["sanitaeter"], updates.get("bemerkung") or "",
            updates["stundensatz"], updates.get("language_skills") or dump_language_skills({}),
            updates.get("brandschutzhelfer") or "nein", updates.get("deeskalation") or "nein", updates.get("gssk") or "nein", updates.get("fachkraft_ss") or "nein",
            updates.get("personenschutz") or "nein", updates.get("waffensachkunde") or "nein", updates.get("behoerdlich_studium") or "nein",
            updates.get("fuehrerschein") or "nein", updates.get("fuehrerschein_klassen") or "", clean_image_data(updates.get("image_data")), username
        )
    )
    db.commit()
    return jsonify({"status": "ok"})


@app.route("/users/<username>/lock", methods=["POST"])
def toggle_user_lock(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    u = db.execute("SELECT username, COALESCE(is_locked, FALSE) AS is_locked FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    new_state = not bool(u.get("is_locked") or False)
    db.execute("UPDATE users SET is_locked=%s WHERE username=%s", (new_state, username))
    db.commit()
    return jsonify({"status": "ok", "is_locked": new_state})


@app.route("/users/<username>/pdf", methods=["GET"])
def user_pdf(username):
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403

    db = get_db()
    u = db.execute("SELECT * FROM users WHERE username=%s", (username,)).fetchone()
    if not u:
        return jsonify({"error": "Benutzer nicht gefunden"}), 404

    from flask import send_file
    import base64

    def yn(value):
        return "Ja" if str(value or "").strip().lower() == "ja" else "Nein"

    def draw_wrapped(c, text, x, y, max_width, line_height=14, font_name="Helvetica", font_size=10, color=colors.black):
        c.setFont(font_name, font_size)
        c.setFillColor(color)
        words = str(text or "-").split()
        if not words:
            c.drawString(x, y, "-")
            return y - line_height
        line = ""
        for word in words:
            test = word if not line else f"{line} {word}"
            if stringWidth(test, font_name, font_size) <= max_width:
                line = test
            else:
                c.drawString(x, y, line)
                y -= line_height
                line = word
        if line:
            c.drawString(x, y, line)
            y -= line_height
        return y

    def draw_kv_box(c, x, y_top, w, title, items, accent=colors.HexColor("#d9e9ff")):
        label_w = 110
        line_h = 16
        inner_y = y_top - 34
        content_y = inner_y
        c.setFont("Helvetica-Bold", 14)
        for label, value in items:
            c.setFillColor(colors.HexColor("#23324d"))
            c.drawString(x + 14, content_y, f"{label}:")
            content_y = draw_wrapped(c, value, x + 14 + label_w, content_y, w - label_w - 28, line_height=line_h, font_name="Helvetica", font_size=11)
            content_y -= 4
        height_box = max(98, y_top - content_y + 12)
        c.setFillColor(colors.white)
        c.setStrokeColor(colors.HexColor("#d8dee8"))
        c.roundRect(x, y_top - height_box, w, height_box, 12, stroke=1, fill=1)
        c.setFillColor(accent)
        c.roundRect(x, y_top - 28, w, 28, 12, stroke=0, fill=1)
        c.rect(x, y_top - 28, w, 14, stroke=0, fill=1)
        c.setFillColor(colors.HexColor("#0f172a"))
        c.setFont("Helvetica-Bold", 13)
        c.drawString(x + 14, y_top - 18, title)
        c.setFillColor(colors.black)
        content_y = y_top - 48
        for label, value in items:
            c.setFont("Helvetica-Bold", 11)
            c.setFillColor(colors.HexColor("#23324d"))
            c.drawString(x + 14, content_y, f"{label}:")
            content_y = draw_wrapped(c, value, x + 14 + label_w, content_y, w - label_w - 28, line_height=line_h, font_name="Helvetica", font_size=11)
            content_y -= 4
        return y_top - height_box

    language_skills = parse_language_skills(u.get("language_skills"))
    language_text = ", ".join([f"{lang}: {level}" for lang, level in language_skills.items()]) or "-"
    fuehrerschein_text = yn(u.get("fuehrerschein"))
    if fuehrerschein_text == "Ja" and (u.get("fuehrerschein_klassen") or "").strip():
        fuehrerschein_text += f" – Klasse {(u.get('fuehrerschein_klassen') or '').strip()}"

    all_qualifications = [
        ("Sanitätsdienst", yn(u.get("sanitaeter"))),
        ("Brandschutzhelfer", yn(u.get("brandschutzhelfer"))),
        ("Deeskalation", yn(u.get("deeskalation"))),
        ("GSSK", yn(u.get("gssk"))),
        ("Fachkraft S&S", yn(u.get("fachkraft_ss"))),
        ("Personenschutz", yn(u.get("personenschutz"))),
        ("Waffensachkunde", yn(u.get("waffensachkunde"))),
        ("Behördlich/Studium", yn(u.get("behoerdlich_studium"))),
        ("BSW", yn(u.get("bsw"))),
        ("P-Schein", yn(u.get("pschein"))),
    ]
    visible_qualifications = [(k, v) for k, v in all_qualifications if v == "Ja"]
    if not visible_qualifications:
        visible_qualifications = [("Qualifikationen", "Keine zusätzlichen Qualifikationen hinterlegt")]

    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    margin = 42

    pdf.setTitle(f"Mitarbeiter_{username}")
    pdf.setAuthor("CV Planung")
    pdf.setFillColor(colors.HexColor("#08152e"))
    pdf.roundRect(margin, height - 85, width - 2 * margin, 48, 14, stroke=0, fill=1)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(margin + 18, height - 58, "Mitarbeiterprofil")
    pdf.setFont("Helvetica", 10)
    pdf.drawString(margin + 18, height - 74, f"Export am {datetime.now().strftime('%d.%m.%Y, %H:%M Uhr')}")

    card_y_top = height - 120
    left_w = width * 0.58
    right_x = margin + left_w + 18
    right_w = width - margin - right_x
    card_h = 120

    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(colors.HexColor("#d8dee8"))
    pdf.roundRect(margin, card_y_top - card_h, left_w, card_h, 16, stroke=1, fill=1)
    pdf.setFillColor(colors.HexColor("#0f172a"))
    pdf.setFont("Helvetica-Bold", 19)
    full_name = f"{(u.get('vorname') or '').strip()} {(u.get('nachname') or '').strip()}".strip() or username
    pdf.drawString(margin + 18, card_y_top - 28, full_name)
    pdf.setFont("Helvetica", 11)
    summary_lines = [
        f"Benutzername: {username}",
        f"E-Mail: {(u.get('email') or '').strip() or '-'}",
        f"Bemerkung: {(u.get('bemerkung') or '').strip() or '-'}",
        f"Datenschutz: {'Ja' if bool(u.get('consent_given') or False) else 'Nein'}",
    ]
    line_y = card_y_top - 50
    for line in summary_lines:
        pdf.setFillColor(colors.HexColor("#334155"))
        pdf.drawString(margin + 18, line_y, line)
        line_y -= 16

    image_x = right_x
    image_y = card_y_top - card_h
    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(colors.HexColor("#d8dee8"))
    pdf.roundRect(image_x, image_y, right_w, card_h, 16, stroke=1, fill=1)
    img_value = (u.get("image_data") or "").strip()
    drawn_image = False
    if img_value.startswith("data:image/") and ";base64," in img_value:
        try:
            raw = base64.b64decode(img_value.split(",", 1)[1])
            reader = ImageReader(io.BytesIO(raw))
            iw, ih = reader.getSize()
            max_w = right_w - 20
            max_h = card_h - 20
            scale = min(max_w / iw, max_h / ih)
            draw_w = iw * scale
            draw_h = ih * scale
            draw_x = image_x + (right_w - draw_w) / 2
            draw_y = image_y + (card_h - draw_h) / 2
            pdf.drawImage(reader, draw_x, draw_y, draw_w, draw_h, preserveAspectRatio=True, mask='auto')
            drawn_image = True
        except Exception:
            drawn_image = False
    if not drawn_image:
        pdf.setFillColor(colors.HexColor("#eef2f7"))
        pdf.roundRect(image_x + 16, image_y + 16, right_w - 32, card_h - 32, 10, stroke=0, fill=1)
        pdf.setFillColor(colors.HexColor("#64748b"))
        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawCentredString(image_x + right_w / 2, image_y + card_h / 2 + 6, "Kein Bild")
        pdf.setFont("Helvetica", 10)
        pdf.drawCentredString(image_x + right_w / 2, image_y + card_h / 2 - 10, "Kein Bild hinterlegt")

    box_top = card_y_top - card_h - 18
    left_box_w = (width - 2 * margin - 14) / 2
    right_box_x = margin + left_box_w + 14

    left_bottom = draw_kv_box(pdf, margin, box_top, left_box_w, "Basisdaten", [
        ("§ 34a GewO", yn(u.get("s34a")) + (f" ({u.get('s34a_art')})" if yn(u.get("s34a")) == "Ja" and (u.get("s34a_art") or "").strip() else "")),
        ("Bewacher-ID", (u.get("bewach_id") or "").strip() or "-"),
        ("BSW", yn(u.get("bsw"))),
        ("P-Schein", yn(u.get("pschein"))),
        ("Führerschein", fuehrerschein_text),
        ("SVS", f"{float(u.get('stundensatz')):.2f} €/h" if u.get("stundensatz") not in (None, "") else "-"),
    ], accent=colors.HexColor("#e8efff"))

    right_bottom = draw_kv_box(pdf, right_box_x, box_top, left_box_w, "Qualifikationen", visible_qualifications, accent=colors.HexColor("#eef8f0"))

    lower_top = min(left_bottom, right_bottom) - 18
    lower_bottom = draw_kv_box(pdf, margin, lower_top, width - 2 * margin, "Fremdsprachen & Hinweise", [
        ("Sprachen", language_text),
        ("Sonstige Hinweise", (u.get("bemerkung") or "").strip() or "-"),
        ("Erklärung", "Datenschutz-Selbsterklärung liegt vor" if bool(u.get("consent_given") or False) else "Datenschutz-Selbsterklärung fehlt"),
        ("Accountstatus", "Gesperrt" if bool(u.get("is_locked") or False) else "Aktiv"),
    ], accent=colors.HexColor("#e9f8ef"))

    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.HexColor("#64748b"))
    pdf.drawRightString(width - margin, 20, f"Export für {full_name}")

    pdf.save()
    buffer.seek(0)
    return send_file(buffer, mimetype="application/pdf", as_attachment=True, download_name=f"mitarbeiter_{username}.pdf")


@app.route("/users/<username>", methods=["DELETE"])
def delete_user(username):
    # ✅ Sensible Personaldaten: nur Chef/Vorgesetzter (NICHT vorgesetzter_cp)
    if normalize_role(session.get("role")) not in ["chef", "vorgesetzter"]:
        return jsonify({"error": "Nicht erlaubt"}), 403
    db = get_db()
    db.execute("DELETE FROM users WHERE username=%s", (username,))
    db.commit()
    return jsonify({"status": "ok"})


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
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)






