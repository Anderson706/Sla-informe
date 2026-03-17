import io
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
from flask import Flask, flash, g, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'app.db'
UPLOAD_DIR = BASE_DIR / 'instance_uploads'
UPLOAD_DIR.mkdir(exist_ok=True)

ALLOWED_EXTENSIONS = {'.xlsx', '.xls'}

app = Flask(__name__)
app.config['SECRET_KEY'] = 'troque-esta-chave-em-producao'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024


# =========================================================
# LAYOUT DOS ARQUIVOS
# =========================================================
FILE_LAYOUTS = {
    'BASE_A': {
        'required_columns': [
            'Sorting Center Warehouse',
            'Sorting Center Warehouse Code',
            'Target Warehouse',
            'Destination Warehouse Code',
            'Destination Warehouse Area',
            'Transit Shelf Tote Number',
            'Shelf Container Number',
            'Shipping Mode',
            'Whether same park',
            'Forecast status',
            'Actual Arrival Destination',
            'Creation Time',
            'Boxer',
            'Boxing Station',
            'Closing Time',
            'Locker',
            'Printing time',
            'Printed by',
            'Shipment Time',
            'Shipper',
            'Pickup Time',
            'Pickup person',
            'Signed for',
            'Signed by',
            'Shelving task claiming time',
            'Shelving task recipient',
            'Shelving operator',
            'License Plate',
            'Completion Time',
            'Transfer status',
        ],
        'optional_columns': [
            'Device Code',
            'Number of sub packages',
            'List start time',
            'Shelving target time (s)',
            'Shelving time (s)',
            'Is the shelving time out?',
        ],
        'default_code_candidates': [
            'Shelf Container Number',
            'Transit Shelf Tote Number',
            'Device Code',
        ],
        'default_time_candidates': [
            'Creation Time',
            'Completion Time',
            'Closing Time',
            'Shipment Time',
            'Pickup Time',
            'List start time',
            'Shelving task claiming time',
            'Printing time',
        ],
    },

    'BASE_B': {
        'required_columns': [
            'Warehouse',
            'Big box type',
            'Shipment Container Number',
            'Shipping container',
            'Service Provider Product',
            'Status',
            'Number of Packages',
            'Whether it is abnormal',
            'Actual Arrival Destination',
            'Creation Time',
            'Boxing start time',
            'Boxing completion time',
            'Boxer',
            'Boxing Station',
            'Shipment Time',
            'Shipper',
            'Forcibly release?',
        ],
        'optional_columns': [
            'Device Code',
        ],
        'default_code_candidates': [
            'Shipping container',
            'Shipment Container Number',
            'Device Code',
        ],
        'default_time_candidates': [
            'Creation Time',
            'Boxing start time',
            'Boxing completion time',
            'Shipment Time',
        ],
    }
}


# =========================================================
# BANCO
# =========================================================
def get_db():
    if 'db' not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(_error=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript(
        '''
        CREATE TABLE IF NOT EXISTS uploaded_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            label TEXT NOT NULL,
            filename TEXT NOT NULL,
            stored_path TEXT NOT NULL,
            code_column TEXT,
            time_column TEXT,
            uploaded_at TEXT NOT NULL,
            rows_count INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS parsed_rows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            code_norm TEXT NOT NULL,
            code_raw TEXT,
            time_raw TEXT,
            time_iso TEXT,
            row_json TEXT NOT NULL,
            FOREIGN KEY(file_id) REFERENCES uploaded_files(id)
        );

        CREATE INDEX IF NOT EXISTS idx_parsed_rows_code_norm
        ON parsed_rows(code_norm);

        CREATE INDEX IF NOT EXISTS idx_parsed_rows_label_code
        ON parsed_rows(label, code_norm);

        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_code TEXT NOT NULL,
            code_norm TEXT NOT NULL,
            scanned_at TEXT NOT NULL,
            status TEXT NOT NULL,
            first_entry_iso TEXT,
            first_processed_iso TEXT,
            last_processed_iso TEXT,
            processed_count INTEGER DEFAULT 0,
            delta_first_minutes REAL,
            delta_last_minutes REAL,
            details_json TEXT NOT NULL
        );
        '''
    )
    db.commit()
    db.close()


# =========================================================
# HELPERS
# =========================================================
def allowed_file(filename: str) -> bool:
    return Path(filename or '').suffix.lower() in ALLOWED_EXTENSIONS


def normalize_header(value) -> str:
    s = str(value or '').strip().lower()
    s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    s = ' '.join(s.split())
    return s


def normalize_code(value) -> str:
    s = str(value or '').strip().upper()
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s


def try_parse_datetime(value):
    if value in (None, '', '-', 'None'):
        return None

    try:
        dt = pd.to_datetime(value, errors='coerce', dayfirst=True)
    except Exception:
        return None

    if pd.isna(dt):
        return None

    if hasattr(dt, 'to_pydatetime'):
        dt = dt.to_pydatetime()

    return dt.replace(microsecond=0)


def detect_column(headers, candidates):
    normalized_headers = [normalize_header(h) for h in headers]
    normalized_candidates = [normalize_header(c) for c in candidates]

    for candidate in normalized_candidates:
        if candidate in normalized_headers:
            return headers[normalized_headers.index(candidate)]

    for idx, header in enumerate(normalized_headers):
        if any(candidate in header for candidate in normalized_candidates):
            return headers[idx]

    return None


def validate_expected_columns(df_columns, label):
    layout = FILE_LAYOUTS.get(label, {})
    required_columns = layout.get('required_columns', [])
    normalized_df_cols = [normalize_header(c) for c in df_columns]

    missing = []
    for col in required_columns:
        if normalize_header(col) not in normalized_df_cols:
            missing.append(col)

    return missing


def _fmt_iso(iso_str):
    if not iso_str:
        return '-'
    try:
        dt = datetime.fromisoformat(iso_str)
        return dt.strftime('%d/%m/%Y %H:%M:%S')
    except Exception:
        return str(iso_str)


def _minutes_between(start_iso, end_iso):
    if not start_iso or not end_iso:
        return None
    try:
        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso)
        return round((end_dt - start_dt).total_seconds() / 60, 2)
    except Exception:
        return None


def _seconds_between(start_iso, end_iso):
    if not start_iso or not end_iso:
        return None
    try:
        start_dt = datetime.fromisoformat(start_iso)
        end_dt = datetime.fromisoformat(end_iso)
        return int((end_dt - start_dt).total_seconds())
    except Exception:
        return None


def _format_duration(seconds):
    if seconds is None:
        return '-'

    negative = seconds < 0
    seconds = abs(int(seconds))

    days = seconds // 86400
    seconds %= 86400
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60

    parts = []
    if days:
        parts.append(f'{days} dia(s)')
    if hours:
        parts.append(f'{hours} hora(s)')
    if minutes:
        parts.append(f'{minutes} minuto(s)')
    if seconds or not parts:
        parts.append(f'{seconds} segundo(s)')

    text = ', '.join(parts)
    return f'-{text}' if negative else text


def _sla_analysis(process_deltas_seconds, total_process_seconds, sla_hours=24):
    sla_seconds = sla_hours * 3600

    if total_process_seconds is None:
        return {
            'sla_hours': sla_hours,
            'sla_seconds': sla_seconds,
            'estourou_sla': None,
            'tempo_total_segundos': None,
            'tempo_total_formatado': '-',
            'tempo_restante_segundos': None,
            'tempo_restante_formatado': '-',
            'tempo_excedido_segundos': None,
            'tempo_excedido_formatado': '-',
            'etapa_mais_demorada': '-',
            'tempo_etapa_mais_demorada_segundos': None,
            'tempo_etapa_mais_demorada_formatado': '-',
        }

    estourou = total_process_seconds > sla_seconds
    restante = sla_seconds - total_process_seconds if not estourou else 0
    excedido = total_process_seconds - sla_seconds if estourou else 0

    etapa_mais_demorada = '-'
    etapa_mais_demorada_segundos = None

    valid_items = [(k, v) for k, v in process_deltas_seconds.items() if v is not None]
    if valid_items:
        etapa_mais_demorada, etapa_mais_demorada_segundos = max(valid_items, key=lambda x: x[1])

    return {
        'sla_hours': sla_hours,
        'sla_seconds': sla_seconds,
        'estourou_sla': estourou,
        'tempo_total_segundos': total_process_seconds,
        'tempo_total_formatado': _format_duration(total_process_seconds),
        'tempo_restante_segundos': restante if not estourou else 0,
        'tempo_restante_formatado': _format_duration(restante) if not estourou else '0 segundo(s)',
        'tempo_excedido_segundos': excedido if estourou else 0,
        'tempo_excedido_formatado': _format_duration(excedido) if estourou else '0 segundo(s)',
        'etapa_mais_demorada': etapa_mais_demorada,
        'tempo_etapa_mais_demorada_segundos': etapa_mais_demorada_segundos,
        'tempo_etapa_mais_demorada_formatado': _format_duration(etapa_mais_demorada_segundos),
    }


def _extract_base_a_process_times(base_a_rows):
    creation_iso = None
    closing_iso = None
    shipment_iso = None
    signed_for_iso = None

    for row in base_a_rows:
        if creation_iso is None:
            dt = try_parse_datetime(row.get('Creation Time'))
            if dt:
                creation_iso = dt.isoformat(sep=' ')

        if closing_iso is None:
            dt = try_parse_datetime(row.get('Closing Time'))
            if dt:
                closing_iso = dt.isoformat(sep=' ')

        if shipment_iso is None:
            dt = try_parse_datetime(row.get('Shipment Time'))
            if dt:
                shipment_iso = dt.isoformat(sep=' ')

        if signed_for_iso is None:
            dt = try_parse_datetime(row.get('Signed for'))
            if dt:
                signed_for_iso = dt.isoformat(sep=' ')

    return {
        'creation_iso': creation_iso,
        'closing_iso': closing_iso,
        'shipment_iso': shipment_iso,
        'signed_for_iso': signed_for_iso,
    }


def _extract_min_creation_time(base_a_rows):
    dates = []

    for row in base_a_rows:
        dt = try_parse_datetime(row.get('Creation Time'))
        if dt:
            dates.append(dt)

    if not dates:
        return None

    min_dt = min(dates)
    return min_dt.isoformat(sep=' ')


# =========================================================
# LEITURA E ARMAZENAMENTO DAS PLANILHAS
# =========================================================
def parse_excel(file_like, label, code_column=None, time_column=None):
    df = pd.read_excel(file_like, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]

    if df.empty:
        raise ValueError(f'A planilha {label} está vazia.')

    layout = FILE_LAYOUTS.get(label, {})

    missing_columns = validate_expected_columns(df.columns, label)
    if missing_columns:
        raise ValueError(
            f'A planilha {label} não corresponde ao layout esperado. '
            f'Colunas ausentes: {", ".join(missing_columns)}'
        )

    if not code_column:
        code_column = detect_column(
            list(df.columns),
            layout.get('default_code_candidates', [])
        )

    if not code_column or code_column not in df.columns:
        raise ValueError(
            f'Não foi possível identificar a coluna de código da planilha {label}.'
        )

    if not time_column:
        time_column = detect_column(
            list(df.columns),
            layout.get('default_time_candidates', [])
        )

    if time_column and time_column not in df.columns:
        raise ValueError(
            f'A coluna de data/hora informada para {label} não existe.'
        )

    rows = []
    for _, row in df.iterrows():
        code_raw = row.get(code_column)
        code_norm = normalize_code(code_raw)

        if not code_norm:
            continue

        time_raw = row.get(time_column) if time_column else None
        dt = try_parse_datetime(time_raw)

        payload = {}
        for k, v in row.to_dict().items():
            payload[k] = None if pd.isna(v) else str(v)

        rows.append({
            'code_norm': code_norm,
            'code_raw': '' if code_raw is None else str(code_raw),
            'time_raw': '' if time_raw is None else str(time_raw),
            'time_iso': dt.isoformat(sep=' ') if dt else None,
            'row_json': json.dumps(payload, ensure_ascii=False),
        })

    return df, rows, code_column, time_column


def store_uploaded_excel(label, file_storage, code_column=None, time_column=None):
    db = get_db()

    filename = secure_filename(file_storage.filename or f'{label}.xlsx')
    if not allowed_file(filename):
        raise ValueError(f'Arquivo inválido para {label}. Envie .xlsx ou .xls.')

    memory = io.BytesIO(file_storage.read())
    memory.seek(0)

    _, rows, code_column, time_column = parse_excel(
        memory, label, code_column, time_column
    )
    memory.seek(0)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    stored_name = f'{label}_{timestamp}_{filename}'
    stored_path = UPLOAD_DIR / stored_name

    with open(stored_path, 'wb') as f:
        f.write(memory.read())

    old_rows = db.execute(
        'SELECT stored_path FROM uploaded_files WHERE label = ?',
        (label,)
    ).fetchall()

    for old in old_rows:
        try:
            path = Path(old['stored_path'])
            if path.exists():
                path.unlink()
        except Exception:
            pass

    db.execute('DELETE FROM parsed_rows WHERE label = ?', (label,))
    db.execute('DELETE FROM uploaded_files WHERE label = ?', (label,))

    cur = db.execute(
        '''
        INSERT INTO uploaded_files (
            label, filename, stored_path, code_column, time_column, uploaded_at, rows_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            label,
            filename,
            str(stored_path),
            code_column,
            time_column,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            len(rows),
        )
    )
    file_id = cur.lastrowid

    db.executemany(
        '''
        INSERT INTO parsed_rows (
            file_id, label, code_norm, code_raw, time_raw, time_iso, row_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''',
        [
            (
                file_id,
                label,
                r['code_norm'],
                r['code_raw'],
                r['time_raw'],
                r['time_iso'],
                r['row_json'],
            )
            for r in rows
        ]
    )
    db.commit()


# =========================================================
# COMPARAÇÃO
# =========================================================
def _collect_times(label, code_norm):
    db = get_db()
    rows = db.execute(
        '''
        SELECT time_iso, time_raw, row_json
        FROM parsed_rows
        WHERE label = ? AND code_norm = ?
        ORDER BY time_iso ASC
        ''',
        (label, code_norm)
    ).fetchall()

    times = []
    raw_rows = []

    for r in rows:
        if r['time_iso']:
            times.append(r['time_iso'])
        raw_rows.append(json.loads(r['row_json']))

    return times, raw_rows


def compare_code(scanned_code: str):
    code_norm = normalize_code(scanned_code)

    base_a_times, base_a_rows = _collect_times('BASE_A', code_norm)
    base_b_times, base_b_rows = _collect_times('BASE_B', code_norm)

    a_process = _extract_base_a_process_times(base_a_rows)

    creation_iso = a_process['creation_iso']
    closing_iso = a_process['closing_iso']
    shipment_iso = a_process['shipment_iso']
    signed_for_iso = a_process['signed_for_iso']

    min_creation_iso = _extract_min_creation_time(base_a_rows)

    scanned_at_dt = datetime.now().replace(microsecond=0)
    scanned_at_iso = scanned_at_dt.isoformat(sep=' ')

    first_entry = min_creation_iso or creation_iso or (base_a_times[0] if base_a_times else None)
    first_processed = base_b_times[0] if base_b_times else None
    last_processed = base_b_times[-1] if base_b_times else None
    processed_count = len(base_b_times)

    status = 'ENCONTRADO NAS DUAS'
    if not base_a_rows and not base_b_rows:
        status = 'NAO ENCONTRADO'
    elif base_a_rows and not base_b_rows:
        status = 'SOMENTE NO ARQUIVO A'
    elif not base_a_rows and base_b_rows:
        status = 'SOMENTE NO ARQUIVO B'

    delta_first = _minutes_between(first_entry, first_processed)
    delta_last = _minutes_between(first_entry, last_processed)

    delta_creation_closing = _minutes_between(creation_iso, closing_iso)
    delta_closing_shipment = _minutes_between(closing_iso, shipment_iso)
    delta_shipment_signed = _minutes_between(shipment_iso, signed_for_iso)
    delta_creation_signed = _minutes_between(creation_iso, signed_for_iso)

    delta_creation_closing_sec = _seconds_between(creation_iso, closing_iso)
    delta_closing_shipment_sec = _seconds_between(closing_iso, shipment_iso)
    delta_shipment_signed_sec = _seconds_between(shipment_iso, signed_for_iso)
    delta_creation_signed_sec = _seconds_between(creation_iso, signed_for_iso)

    process_deltas_seconds = {
        'Creation Time -> Closing Time': delta_creation_closing_sec,
        'Closing Time -> Shipment Time': delta_closing_shipment_sec,
        'Shipment Time -> Signed for': delta_shipment_signed_sec,
    }

    sla_elapsed_seconds = _seconds_between(min_creation_iso, scanned_at_iso)
    sla_info = _sla_analysis(
        process_deltas_seconds=process_deltas_seconds,
        total_process_seconds=sla_elapsed_seconds,
        sla_hours=24
    )

    details = {
        'base_a_times': [_fmt_iso(x) for x in base_a_times],
        'base_b_times': [_fmt_iso(x) for x in base_b_times],
        'base_a_rows': base_a_rows,
        'base_b_rows': base_b_rows,
        'base_a_label': 'Arquivo A',
        'base_b_label': 'Arquivo B',
        'matched_code_norm': code_norm,
        'process_times_a': {
            'Creation Time': _fmt_iso(creation_iso),
            'Closing Time': _fmt_iso(closing_iso),
            'Shipment Time': _fmt_iso(shipment_iso),
            'Signed for': _fmt_iso(signed_for_iso),
            'Min Creation Time': _fmt_iso(min_creation_iso),
            'Scanned At': _fmt_iso(scanned_at_iso),
        },
        'process_deltas_a': {
            'Creation Time -> Closing Time (min)': delta_creation_closing,
            'Closing Time -> Shipment Time (min)': delta_closing_shipment,
            'Shipment Time -> Signed for (min)': delta_shipment_signed,
            'Creation Time -> Signed for (min)': delta_creation_signed,
            'Min Creation Time -> Scanned At (min)': _minutes_between(min_creation_iso, scanned_at_iso),
        },
        'process_deltas_a_human': {
            'Creation Time -> Closing Time': _format_duration(delta_creation_closing_sec),
            'Closing Time -> Shipment Time': _format_duration(delta_closing_shipment_sec),
            'Shipment Time -> Signed for': _format_duration(delta_shipment_signed_sec),
            'Creation Time -> Signed for': _format_duration(delta_creation_signed_sec),
            'Min Creation Time -> Scanned At': _format_duration(sla_elapsed_seconds),
        },
        'process_deltas_a_seconds': {
            'Creation Time -> Closing Time': delta_creation_closing_sec,
            'Closing Time -> Shipment Time': delta_closing_shipment_sec,
            'Shipment Time -> Signed for': delta_shipment_signed_sec,
            'Creation Time -> Signed for': delta_creation_signed_sec,
            'Min Creation Time -> Scanned At': sla_elapsed_seconds,
        },
        'sla_info': {
            **sla_info,
            'sla_start': _fmt_iso(min_creation_iso),
            'sla_end': _fmt_iso(scanned_at_iso),
            'sla_base': 'Min Creation Time -> Scanned At',
        }
    }

    db = get_db()
    db.execute(
        '''
        INSERT INTO scans (
            scanned_code, code_norm, scanned_at, status, first_entry_iso,
            first_processed_iso, last_processed_iso, processed_count,
            delta_first_minutes, delta_last_minutes, details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''',
        (
            scanned_code,
            code_norm,
            scanned_at_dt.strftime('%Y-%m-%d %H:%M:%S'),
            status,
            first_entry,
            first_processed,
            last_processed,
            processed_count,
            delta_first,
            delta_last,
            json.dumps(details, ensure_ascii=False),
        )
    )
    db.commit()


# =========================================================
# CONSULTAS
# =========================================================
def get_uploaded_files():
    db = get_db()
    return db.execute(
        'SELECT * FROM uploaded_files ORDER BY label ASC'
    ).fetchall()


def _month_summary(formatted_dates):
    counter = {}
    for ds in formatted_dates:
        try:
            dt = datetime.strptime(ds, '%d/%m/%Y %H:%M:%S')
        except Exception:
            continue
        key = dt.strftime('%Y-%m')
        counter[key] = counter.get(key, 0) + 1

    return [{'mes': m, 'qtd': counter[m]} for m in sorted(counter.keys())]


def get_recent_scans(limit=100):
    db = get_db()
    rows = db.execute(
        'SELECT * FROM scans ORDER BY id DESC LIMIT ?',
        (limit,)
    ).fetchall()

    items = []
    for r in rows:
        details = json.loads(r['details_json'])
        items.append({
            'id': r['id'],
            'scanned_code': r['scanned_code'],
            'scanned_at': r['scanned_at'],
            'status': r['status'],
            'first_entry': _fmt_iso(r['first_entry_iso']),
            'first_processed': _fmt_iso(r['first_processed_iso']),
            'last_processed': _fmt_iso(r['last_processed_iso']),
            'qtd_processados': r['processed_count'],
            'delta_first_minutes': r['delta_first_minutes'],
            'delta_last_minutes': r['delta_last_minutes'],
            'todos_acessos': details.get('base_b_times', []),
            'acessos_por_mes': _month_summary(details.get('base_b_times', [])),
            'details': details,
            'process_times_a': details.get('process_times_a', {}),
            'process_deltas_a': details.get('process_deltas_a', {}),
            'process_deltas_a_human': details.get('process_deltas_a_human', {}),
            'process_deltas_a_seconds': details.get('process_deltas_a_seconds', {}),
            'sla_info': details.get('sla_info', {}),
        })
    return items


def get_latest_result():
    scans = get_recent_scans(limit=1)
    if not scans:
        return None

    r = scans[0]
    details = r['details']

    return {
        'scanned_code': r['scanned_code'],
        'status': r['status'],
        'first_entry': r['first_entry'],
        'first_processed': r['first_processed'],
        'last_processed': r['last_processed'],
        'processed_count': r['qtd_processados'],
        'delta_first_minutes': r['delta_first_minutes'],
        'delta_last_minutes': r['delta_last_minutes'],
        'details': details,
        'process_times_a': details.get('process_times_a', {}),
        'process_deltas_a': details.get('process_deltas_a', {}),
        'process_deltas_a_human': details.get('process_deltas_a_human', {}),
        'process_deltas_a_seconds': details.get('process_deltas_a_seconds', {}),
        'sla_info': details.get('sla_info', {}),
    }


def get_stats():
    db = get_db()
    arquivos = db.execute('SELECT COUNT(*) FROM uploaded_files').fetchone()[0]
    linhas = db.execute('SELECT COUNT(*) FROM parsed_rows').fetchone()[0]
    bipes = db.execute('SELECT COUNT(*) FROM scans').fetchone()[0]
    nao_encontrados = db.execute(
        "SELECT COUNT(*) FROM scans WHERE status = 'NAO ENCONTRADO'"
    ).fetchone()[0]
    encontrados = bipes - nao_encontrados

    return {
        'linhas_planilha': linhas,
        'codigos_pesquisados': bipes,
        'encontrados': encontrados,
        'nao_encontrados': nao_encontrados,
        'arquivos': arquivos,
        'linhas': linhas,
        'bipes': bipes,
    }


# =========================================================
# ROTAS
# =========================================================
@app.route('/')
def index():
    return render_template(
        'index.html',
        resumo=get_stats(),
        uploaded_files=get_uploaded_files(),
        resultados=get_recent_scans(),
        latest_result=get_latest_result(),
        rid='historico',
    )


@app.route('/upload', methods=['POST'])
def upload():
    arquivo_a = request.files.get('arquivo_a')
    arquivo_b = request.files.get('arquivo_b')

    if not arquivo_a or not arquivo_a.filename or not arquivo_b or not arquivo_b.filename:
        flash('Envie os dois arquivos Excel.', 'warning')
        return redirect(url_for('index'))

    try:
        store_uploaded_excel(
            'BASE_A',
            arquivo_a,
            request.form.get('code_column_a') or None,
            request.form.get('time_column_a') or None
        )
        store_uploaded_excel(
            'BASE_B',
            arquivo_b,
            request.form.get('code_column_b') or None,
            request.form.get('time_column_b') or None
        )
        flash('Bases importadas com sucesso.', 'success')
    except Exception as e:
        flash(f'Erro ao importar as planilhas: {e}', 'danger')

    return redirect(url_for('index'))


@app.route('/scan', methods=['POST'])
def scan():
    scanned_code = (request.form.get('scanned_code') or '').strip()

    if not scanned_code:
        flash('Informe um código antes de processar.', 'warning')
        return redirect(url_for('index'))

    try:
        compare_code(scanned_code)
        flash(f'Comparativo executado para o código {scanned_code}.', 'success')
    except Exception as e:
        flash(f'Erro ao processar o código: {e}', 'danger')

    return redirect(url_for('index'))


@app.route('/download/<int:file_id>')
def download_file(file_id):
    db = get_db()
    row = db.execute(
        'SELECT * FROM uploaded_files WHERE id = ?',
        (file_id,)
    ).fetchone()

    if not row:
        flash('Arquivo não encontrado.', 'warning')
        return redirect(url_for('index'))

    path = Path(row['stored_path'])
    if not path.exists():
        flash('O arquivo foi removido do armazenamento.', 'danger')
        return redirect(url_for('index'))

    return send_file(path, as_attachment=True, download_name=row['filename'])


@app.route('/export/bipes.xlsx')
def export_scans_excel():
    db = get_db()
    rows = db.execute('SELECT * FROM scans ORDER BY id DESC').fetchall()

    data = []
    for r in rows:
        details = json.loads(r['details_json'])
        process_times_a = details.get('process_times_a', {})
        process_deltas_a = details.get('process_deltas_a', {})
        process_deltas_a_human = details.get('process_deltas_a_human', {})
        sla_info = details.get('sla_info', {})

        data.append({
            'Codigo': r['scanned_code'],
            'CodigoNormalizado': r['code_norm'],
            'BipadoEm': r['scanned_at'],
            'Status': r['status'],
            'PrimeiraEntradaArquivoA': _fmt_iso(r['first_entry_iso']),
            'PrimeiroRegistroArquivoB': _fmt_iso(r['first_processed_iso']),
            'UltimoRegistroArquivoB': _fmt_iso(r['last_processed_iso']),
            'QtdRegistrosArquivoB': r['processed_count'],
            'DeltaPrimeiroMin': r['delta_first_minutes'],
            'DeltaUltimoMin': r['delta_last_minutes'],
            'HorariosArquivoA': ' | '.join(details.get('base_a_times', [])),
            'HorariosArquivoB': ' | '.join(details.get('base_b_times', [])),

            'Creation Time': process_times_a.get('Creation Time'),
            'Closing Time': process_times_a.get('Closing Time'),
            'Shipment Time': process_times_a.get('Shipment Time'),
            'Signed for': process_times_a.get('Signed for'),
            'Min Creation Time': process_times_a.get('Min Creation Time'),
            'Scanned At': process_times_a.get('Scanned At'),

            'Creation Time -> Closing Time (min)': process_deltas_a.get('Creation Time -> Closing Time (min)'),
            'Closing Time -> Shipment Time (min)': process_deltas_a.get('Closing Time -> Shipment Time (min)'),
            'Shipment Time -> Signed for (min)': process_deltas_a.get('Shipment Time -> Signed for (min)'),
            'Creation Time -> Signed for (min)': process_deltas_a.get('Creation Time -> Signed for (min)'),
            'Min Creation Time -> Scanned At (min)': process_deltas_a.get('Min Creation Time -> Scanned At (min)'),

            'Creation Time -> Closing Time': process_deltas_a_human.get('Creation Time -> Closing Time'),
            'Closing Time -> Shipment Time': process_deltas_a_human.get('Closing Time -> Shipment Time'),
            'Shipment Time -> Signed for': process_deltas_a_human.get('Shipment Time -> Signed for'),
            'Creation Time -> Signed for': process_deltas_a_human.get('Creation Time -> Signed for'),
            'Min Creation Time -> Scanned At': process_deltas_a_human.get('Min Creation Time -> Scanned At'),

            'SLA (horas)': sla_info.get('sla_hours'),
            'SLA Start': sla_info.get('sla_start'),
            'SLA End': sla_info.get('sla_end'),
            'SLA Base': sla_info.get('sla_base'),
            'Estourou SLA': sla_info.get('estourou_sla'),
            'Tempo total processo': sla_info.get('tempo_total_formatado'),
            'Tempo restante SLA': sla_info.get('tempo_restante_formatado'),
            'Tempo excedido SLA': sla_info.get('tempo_excedido_formatado'),
            'Etapa mais demorada': sla_info.get('etapa_mais_demorada'),
            'Tempo etapa mais demorada': sla_info.get('tempo_etapa_mais_demorada_formatado'),
        })

    df = pd.DataFrame(data or [{'Mensagem': 'Nenhum bipe encontrado.'}])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Bipes')

    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name='historico_bipes.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


@app.route('/export/comparativo.xlsx')
def export_comparison_excel():
    db = get_db()
    rows = db.execute('SELECT * FROM scans ORDER BY id DESC').fetchall()

    data = []
    for r in rows:
        details = json.loads(r['details_json'])

        base_a_rows = details.get('base_a_rows', [])
        base_b_rows = details.get('base_b_rows', [])

        base_a_first = base_a_rows[0] if base_a_rows else {}
        base_b_first = base_b_rows[0] if base_b_rows else {}

        process_times_a = details.get('process_times_a', {})
        process_deltas_a = details.get('process_deltas_a', {})
        process_deltas_a_human = details.get('process_deltas_a_human', {})
        sla_info = details.get('sla_info', {})

        item = {
            'Codigo': r['scanned_code'],
            'CodigoNormalizado': r['code_norm'],
            'Status': r['status'],
            'PrimeiraEntradaArquivoA': _fmt_iso(r['first_entry_iso']),
            'PrimeiroRegistroArquivoB': _fmt_iso(r['first_processed_iso']),
            'UltimoRegistroArquivoB': _fmt_iso(r['last_processed_iso']),
            'QtdRegistrosArquivoB': r['processed_count'],
            'DeltaPrimeiroMin': r['delta_first_minutes'],
            'DeltaUltimoMin': r['delta_last_minutes'],

            'Creation Time': process_times_a.get('Creation Time'),
            'Closing Time': process_times_a.get('Closing Time'),
            'Shipment Time': process_times_a.get('Shipment Time'),
            'Signed for': process_times_a.get('Signed for'),
            'Min Creation Time': process_times_a.get('Min Creation Time'),
            'Scanned At': process_times_a.get('Scanned At'),

            'Creation Time -> Closing Time (min)': process_deltas_a.get('Creation Time -> Closing Time (min)'),
            'Closing Time -> Shipment Time (min)': process_deltas_a.get('Closing Time -> Shipment Time (min)'),
            'Shipment Time -> Signed for (min)': process_deltas_a.get('Shipment Time -> Signed for (min)'),
            'Creation Time -> Signed for (min)': process_deltas_a.get('Creation Time -> Signed for (min)'),
            'Min Creation Time -> Scanned At (min)': process_deltas_a.get('Min Creation Time -> Scanned At (min)'),

            'Creation Time -> Closing Time': process_deltas_a_human.get('Creation Time -> Closing Time'),
            'Closing Time -> Shipment Time': process_deltas_a_human.get('Closing Time -> Shipment Time'),
            'Shipment Time -> Signed for': process_deltas_a_human.get('Shipment Time -> Signed for'),
            'Creation Time -> Signed for': process_deltas_a_human.get('Creation Time -> Signed for'),
            'Min Creation Time -> Scanned At': process_deltas_a_human.get('Min Creation Time -> Scanned At'),

            'SLA (horas)': sla_info.get('sla_hours'),
            'SLA Start': sla_info.get('sla_start'),
            'SLA End': sla_info.get('sla_end'),
            'SLA Base': sla_info.get('sla_base'),
            'Estourou SLA': sla_info.get('estourou_sla'),
            'Tempo total processo': sla_info.get('tempo_total_formatado'),
            'Tempo restante SLA': sla_info.get('tempo_restante_formatado'),
            'Tempo excedido SLA': sla_info.get('tempo_excedido_formatado'),
            'Etapa mais demorada': sla_info.get('etapa_mais_demorada'),
            'Tempo etapa mais demorada': sla_info.get('tempo_etapa_mais_demorada_formatado'),
        }

        for k, v in base_a_first.items():
            item[f'A - {k}'] = v

        for k, v in base_b_first.items():
            item[f'B - {k}'] = v

        data.append(item)

    df = pd.DataFrame(data or [{'Mensagem': 'Nenhum comparativo encontrado.'}])

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Comparativo')

    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name='comparativo_codigos.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


if __name__ == '__main__':
    init_db()
    app.run(debug=True)