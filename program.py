#!/usr/bin/env python3
"""Программа чартерной перевозки: какие направления и в какие месяцы летают.

Источник — краевая функция Supabase `charter-program`: открытый адрес без
входа, никаких ключей сборщику не нужно. Внутри она вызывает процедуру
`public.charter_program_months()` служебной ролью и отдаёт наружу только
направления, месяцы и дату снимка — ни мест, ни загрузки, ни цен.

Снаружи иначе не достать: процедура и вид `public.dash_skrydziai` отвечают
publishable-ключу «permission denied» (проверено), схема `marts` через API
не выставлена.

Если адрес недоступен — берётся файл flight-program.json рядом со
сборщиком. Это не красота, а страховка: сбой сети не должен ни обрушить
фид, ни превратиться в слепой обход всех дат года.
"""
import datetime as dt
import json
import os
import subprocess
import time

PROGRAM_URL = os.environ.get(
    "FEED_PROGRAM_URL",
    "https://sjzrnrrndgxvdartjkgw.supabase.co/functions/v1/charter-program")
PROGRAM_FILE = "flight-program.json"
STALE_DAYS = int(os.environ.get("FEED_STALE_DAYS", 14))

# Перевод идентификатора региона ARM в код IATA, идентификатор аэропорта для
# ссылки и названия. В базе этого нет — там только идентификаторы регионов.
# Неизвестный идентификатор НЕ отбрасывается молча: о нём предупреждаем,
# потому что это и есть признак нового направления.
REGIONS = {
    "1285":    {"iata": "AYT", "airport": 23134, "region_lt": "Antalija",        "country_lt": "Turkija"},
    "7067584": {"iata": "HER", "airport": 28889, "region_lt": "Heraklionas",     "country_lt": "Graikija"},
    "9003981": {"iata": "RHO", "airport": 36369, "region_lt": "Rodas",           "country_lt": "Graikija"},
    "9004032": {"iata": "CFU", "airport": 26019, "region_lt": "Korfu",           "country_lt": "Graikija"},
    "5734":    {"iata": "HRG", "airport": 29169, "region_lt": "Hurgada",         "country_lt": "Egiptas"},
    "5735":    {"iata": "SSH", "airport": 37534, "region_lt": "Šarm el Šeichas", "country_lt": "Egiptas"},
    "559866":  {"iata": "TFS", "airport": 38889, "region_lt": "Tenerifė",        "country_lt": "Ispanija"},
    "9004257": {"iata": "AGP", "airport": 32399, "region_lt": "Malaga",          "country_lt": "Ispanija"},
    "531038":  {"iata": "FNC", "airport": 26134, "region_lt": "Madeira",         "country_lt": "Portugalija"},
    "182800":  {"iata": "BOJ", "airport": 24729, "region_lt": "Burgasas",        "country_lt": "Bulgarija"},
    "9001683": {"iata": "VAR", "airport": 39919, "region_lt": "Varna",           "country_lt": "Bulgarija"},
    "3024262": {"iata": "SZG", "airport": 36849, "region_lt": "Zalcburgas",      "country_lt": "Austrija"},
    "442182":  {"iata": "OLB", "airport": 34609, "region_lt": "Olbija",          "country_lt": "Italija"},
    "636907":  {"iata": "SUF", "airport": 31469, "region_lt": "Lamecija Termė",  "country_lt": "Italija"},
    "9013222": {"iata": "RMI", "airport": 27434, "region_lt": "Riminis",         "country_lt": "Italija"},
    "12706":   {"iata": "BJV", "airport": 33119, "region_lt": "Bodrumas",        "country_lt": "Turkija"},
}


def _curl(url, headers=(), data=None, timeout=25):
    cmd = ["curl", "-sS", "-m", str(timeout), url]
    for h in headers:
        cmd += ["-H", h]
    if data is not None:
        cmd += ["-X", "POST", "-d", data]
    p = subprocess.run(cmd, capture_output=True, text=True)
    return (p.stdout or "").strip()


def _from_endpoint(url=None, attempts=3):
    """Прочитать программу с открытого адреса краевой функции."""
    url = url or PROGRAM_URL
    last = ""
    for n in range(attempts):
        if n:
            time.sleep(1.5 * n)
        last = _curl(url)
        try:
            d = json.loads(last)
        except ValueError:
            continue
        if isinstance(d, dict) and d.get("destinations"):
            rows = []
            for x in d["destinations"]:
                for m in x["months"]:
                    y, mm = m.split("-")
                    rows.append({"airport_code": x["region_id"],
                                 "program_year": int(y), "month_num": int(mm),
                                 "region": x.get("region"),
                                 "country": x.get("country")})
            return d.get("snapshot_date"), rows
    raise RuntimeError(f"программа не прочиталась: {last[:160]}")


def _from_file(path=PROGRAM_FILE):
    d = json.load(open(path))
    rows = []
    for x in d["destinations"]:
        for m in x["months"]:
            y, mm = m.split("-")
            rows.append({"airport_code": x["region_id"],
                         "program_year": int(y), "month_num": int(mm),
                         "region": x.get("region"), "country": x.get("country")})
    return d.get("snapshot_date"), rows


def load(today):
    """Вернуть (снимок, {iata: {...meta, months:[(год, месяц)]}}, предупреждения)."""
    warn = []
    try:
        snapshot, rows = _from_endpoint()
    except RuntimeError as e:
        snapshot, rows = _from_file()
        warn.append(f"{e}; взят запасной {PROGRAM_FILE}")

    dests = {}
    unknown = {}
    for r in rows:
        rid = str(r["airport_code"])
        meta = REGIONS.get(rid)
        if not meta:
            unknown[rid] = f"{r.get('country')} / {r.get('region')}"
            continue
        d = dests.setdefault(meta["iata"], dict(meta, months=set()))
        d["months"].add((int(r["program_year"]), int(r["month_num"])))
    for d in dests.values():
        d["months"] = sorted(d["months"])

    for rid, name in unknown.items():
        warn.append(f"НОВОЕ НАПРАВЛЕНИЕ в программе: регион {rid} ({name}) — "
                    f"нет в таблице перевода, цены по нему не собираются")

    if snapshot:
        age = (today - dt.date.fromisoformat(str(snapshot)[:10])).days
        if age > STALE_DAYS:
            warn.append(f"снимок программы от {snapshot} — ему {age} дней, "
                        f"выгрузка ARM, похоже, не прошла")
    return snapshot, dests, warn


def dates_in_program(dests, today, horizon):
    """Даты внутри месяцев программы — только по ним и ищем цены."""
    lo, hi = today + dt.timedelta(days=1), today + dt.timedelta(days=horizon)
    out = {}
    for iata, d in dests.items():
        days = []
        for y, m in d["months"]:
            day = dt.date(y, m, 1)
            while day.month == m:
                if lo <= day <= hi:
                    days.append(day.strftime("%d.%m.%Y"))
                day += dt.timedelta(days=1)
        if days:
            out[iata] = days
    return out


if __name__ == "__main__":
    today = (dt.date.fromisoformat(os.environ["FEED_DATE"])
             if os.environ.get("FEED_DATE") else dt.date.today())
    snap, dests, warn = load(today)
    print(f"снимок {snap}, направлений {len(dests)}")
    for w in warn:
        print(f"  ! {w}")
    cand = dates_in_program(dests, today, 365)
    total = sum(len(v) for v in cand.values())
    for iata in sorted(cand, key=lambda k: -len(cand[k])):
        print(f"  {iata} {dests[iata]['region_lt']:16} дат {len(cand[iata]):3}"
              f"  месяцев {len(dests[iata]['months'])}")
    print(f"дат всего {total}, запросов за полный круг {total * 2}")
