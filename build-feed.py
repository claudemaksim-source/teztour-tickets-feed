#!/usr/bin/env python3
"""Фид «цена от» по странам и регионам для вывода на сайте.

Цены — из открытого чартерного поиска шлюза xml.tezapi.eu/ticketsearch/search
(пароль и белый список не нужны). Направления и месяцы — из программы
перевозки ARM, см. program.py. Проверки перед публикацией — в checks.py.

Две цены считаются ОТДЕЛЬНЫМИ запросами, и это принципиально: круговой
тариф дешевле суммы двух односторонних (проверено: 140+140 за круг против
165+165 по отдельности). Сложением цену туда-обратно получить нельзя.

Цена в ответе шлюза — на всю группу, поэтому запрашивается один взрослый.
Поле свободных мест всегда 10 и смысла не несёт, в фид не попадает.

## Что опрашивается

Только даты внутри месяцев программы — вне них рейсов нет по определению,
и ходить туда незачем. Из них:
  * все даты, на которых рейсы уже известны по запасу, — каждый прогон,
    чтобы опубликованная цена всегда была свежепроверенной;
  * одна TAIL_SLICES-я часть остальных — поиск новых рейсов, полный круг
    за TAIL_SLICES прогонов.
Плюс жёсткий потолок FEED_RPS запросов в секунду: без него восемь потоков
давали 1250 в минуту, в 25 раз выше единственного ограничения, которое TEZ
где-либо документирует.
"""
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import unicodedata
import urllib.parse

import checks
import program

GATE = "https://xml.tezapi.eu/ticketsearch/search"
EUR = 18864
VNO = "VNO"
HORIZON = int(os.environ.get("FEED_HORIZON", 365))
NIGHTS = 7
WORKERS = int(os.environ.get("FEED_WORKERS", 4))
RPS = float(os.environ.get("FEED_RPS", 2))
TAIL_SLICES = int(os.environ.get("FEED_TAIL_SLICES", 7))

CACHE = "prices-cache.json"
OUT = "tickets-from.json"
LINK = "https://www.teztour.lt/flightsearch"
LINK_ORIGIN = 21129    # Вильнюс

DMY = "%d.%m.%Y"
DESTS = {}             # заполняется программой перевозки при запуске

LT_MAP = str.maketrans({"ą": "a", "č": "c", "ę": "e", "ė": "e", "į": "i",
                        "š": "s", "ų": "u", "ū": "u", "ž": "z"})


def slug(s):
    """Латиница, цифры и дефисы — как в адресах страницы предложений."""
    s = s.lower().translate(LT_MAP)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s)).strip("-")


# ── обращение к шлюзу ──────────────────────────────

class GateError(RuntimeError):
    """Шлюз не ответил осмысленно после всех попыток."""


_lock = threading.Lock()
_next_slot = [0.0]


def _throttle():
    gap = 1.0 / RPS if RPS > 0 else 0
    with _lock:
        now = time.monotonic()
        wait = _next_slot[0] - now
        _next_slot[0] = max(now, _next_slot[0]) + gap
    if wait > 0:
        time.sleep(wait)


def _get(params, attempts=4):
    """Ответ шлюза или исключение.

    Без этого любой сбой возвращал пустую строку, рейсов в ответе
    оказывалось ноль, и цена молча подскакивала до следующей по списку.
    Признак годного ответа — корневой элемент <Tickets>: при ошибке шлюз
    отдаёт HTML со страницей 500, при запрете — <reference><error>.
    """
    q = urllib.parse.urlencode(params)
    last = ""
    for n in range(attempts):
        if n:
            time.sleep(1.5 * n)
        _throttle()
        p = subprocess.run(["curl", "-sS", "-m", "25", f"{GATE}?{q}"],
                           capture_output=True, text=True)
        last = (p.stdout or "").strip()
        if last.startswith("<Tickets"):
            return last
    raise GateError(f"{params.get('arrivalIataCodeTo')} "
                    f"{params.get('departureDateTo')}: {last[:120]!r}")


def _field(block, name):
    m = re.search(rf"<{name}>([^<]*)</{name}>", block)
    return m.group(1) if m else ""


def _price(block, name):
    try:
        return float(_field(block, name))
    except ValueError:
        return None      # цена бывает дробной и бывает пустой


def search_ow(dest, date):
    x = _get({"departureIataCodeTo": VNO, "arrivalIataCodeTo": dest,
              "departureDateTo": date, "adultCount": 1, "childCount": 0,
              "infantCount": 0, "currencyId": EUR})
    out = []
    for b in re.findall(r"<ticket>(.*?)</ticket>", x, re.S):
        p = _price(b, "priceSeatYTo")
        if p is None:
            continue
        out.append({"price": p, "flight": _field(b, "flightNameTo"),
                    "dep": _field(b, "fdDepDateTo")[11:16],
                    "arr": _field(b, "fdArrDateTo")[11:16],
                    "airline": _field(b, "aircompanyNameTo")})
    return out


def search_rt(dest, date_out, date_back):
    x = _get({"departureIataCodeTo": VNO, "arrivalIataCodeTo": dest,
              "departureDateTo": date_out,
              "departureIataCodeFrom": dest, "arrivalIataCodeFrom": VNO,
              "departureDateFrom": date_back,
              "adultCount": 1, "childCount": 0, "infantCount": 0,
              "currencyId": EUR})
    out = []
    for b in re.findall(r"<ticket>(.*?)</ticket>", x, re.S):
        a, c = _price(b, "priceSeatYTo"), _price(b, "priceSeatYFrom")
        if a is None or c is None:
            continue
        out.append({"price": a + c, "flight_out": _field(b, "flightNameTo"),
                    "flight_back": _field(b, "flightNameFrom")})
    return out


def link(dest, date_out, date_back=None, adults=1):
    """sessionKey проверяется только по виду — ровно 32 знака шестнадцатеричных.

    Ключ берётся хешем от самих параметров, а не случайным: случайный менялся
    бы при каждой сборке, и в CMS дёргались бы ссылки при неизменной цене.
    """
    tail = ["from_date", date_out]
    if date_back:
        tail += ["to_date", date_back]
    tail += ["adults", str(adults),
             "route_type", "RT" if date_back else "OW",
             "v", "2", "iataFrom", str(LINK_ORIGIN),
             "iataTo", str(DESTS[dest]["airport"]), "charter", "1"]
    key = hashlib.md5("/".join(tail).encode()).hexdigest()
    return f"{LINK}/{key}/{'/'.join(tail)}.lt.html"


# ── запас данных между запусками ───────────────────────

def load_cache():
    try:
        c = json.load(open(CACHE))
    except (OSError, ValueError):
        return {"ow": {}, "rt": {}}
    return {"ow": c.get("ow", {}), "rt": c.get("rt", {})}


def key(dest, date):
    return f"{dest}|{date}"


def unkey(k):
    dest, date = k.split("|", 1)
    return dest, date


FULL = os.environ.get("FEED_FULL") == "1"


def plan(today, cache, candidates):
    """Какие (направление, дата) опрашиваем в этот прогон.

    candidates — даты внутри месяцев программы, вне них рейсов нет.

    Три случая, и все решаются сами, без переключателей:
      * запас пуст (первый запуск) — берём все даты;
      * у направления ни одного известного рейса (появилось в программе) —
        берём все его даты, иначе оно дозревает TAIL_SLICES прогонов и цена
        всё это время завышена: у Ламеции при частичном обходе нашлось
        2 рейса из 11 и 215 EUR вместо настоящих 210;
      * обычное направление — все известные даты плюс одна часть из
        TAIL_SLICES неизвестных, своя в каждый день.

    FEED_FULL=1 принуждает к полному обходу — на случай, когда нужно
    пересобрать запас целиком вручную.
    """
    known = set()
    for mode in ("ow", "rt"):
        for k, rows in cache[mode].items():
            if rows:
                known.add(unkey(k))

    if FULL or not known:
        # запас пуст — первый запуск; нарезать его частями нет смысла
        jobs = {(d, x) for d, days in candidates.items() for x in days}
        return sorted(jobs), 0, len(jobs)

    jobs, fresh, disco = set(), 0, 0
    with_flights = {d for d, _ in known}
    sl = today.toordinal() % TAIL_SLICES
    for dest, days in candidates.items():
        rest = []
        for d in days:
            if (dest, d) in known:
                jobs.add((dest, d))
                fresh += 1
            else:
                rest.append(d)
        if dest not in with_flights:
            # направление появилось в программе только что: берём все его
            # даты сразу, иначе оно дозревает TAIL_SLICES прогонов, а цена
            # всё это время завышена — у Ламеции при частичном обходе
            # нашлось 2 рейса из 11 и 215 EUR вместо настоящих 210
            for d in rest:
                jobs.add((dest, d))
                disco += 1
            continue
        for i, d in enumerate(sorted(rest)):
            if i % TAIL_SLICES == sl:
                jobs.add((dest, d))
                disco += 1
    return sorted(jobs), fresh, disco


def refresh(jobs, cache):
    """Опросить запланированное в оба вида цен. Пустой ответ — удаление."""
    failed = []

    def both(dest, date):
        back = (dt.datetime.strptime(date, DMY).date()
                + dt.timedelta(days=NIGHTS)).strftime(DMY)
        return search_ow(dest, date), search_rt(dest, date, back)

    done = 0
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [(j, ex.submit(both, *j)) for j in jobs]
        for (dest, date), f in futs:
            done += 1
            try:
                ow_rows, rt_rows = f.result()
            except GateError as e:
                failed.append(str(e))
                continue
            k = key(dest, date)
            for mode, rows in (("ow", ow_rows), ("rt", rt_rows)):
                if rows:
                    cache[mode][k] = rows
                else:
                    cache[mode].pop(k, None)
            if done % 200 == 0:
                print(f"  … {done}/{len(jobs)} дат, сбоев {len(failed)}",
                      flush=True)
    return failed


def prune(cache, today, candidates):
    """Выкинуть прошедшее, вышедшее за горизонт и выпавшее из программы."""
    lo, hi = today + dt.timedelta(days=1), today + dt.timedelta(days=HORIZON)
    allowed = {(d, x) for d, days in candidates.items() for x in days}
    removed = 0
    for mode in ("ow", "rt"):
        for k in list(cache[mode]):
            dest, date = unkey(k)
            d = dt.datetime.strptime(date, DMY).date()
            if d < lo or d > hi or (dest, date) not in allowed:
                del cache[mode][k]
                removed += 1
    return removed


def rows_from_cache(cache):
    """Запас → плоские списки. Порядок устойчивый: иначе при равных ценах
    min() выбирал бы разные рейсы и ссылка в CMS дёргалась бы впустую."""
    ow, rt = [], []
    for k, rows in cache["ow"].items():
        dest, date = unkey(k)
        for r in rows:
            ow.append(dict(r, dest=dest, date=date))
    for k, rows in cache["rt"].items():
        dest, date = unkey(k)
        back = (dt.datetime.strptime(date, DMY).date()
                + dt.timedelta(days=NIGHTS)).strftime(DMY)
        for r in rows:
            rt.append(dict(r, dest=dest, date_out=date, date_back=back))
    ow.sort(key=lambda r: (r["dest"], dt.datetime.strptime(r["date"], DMY),
                           r["flight"]))
    rt.sort(key=lambda r: (r["dest"], dt.datetime.strptime(r["date_out"], DMY),
                           r["flight_out"]))
    return ow, rt


# ── сборка фида ──────────────────────────────────

def cheapest(rows, k="price"):
    return min(rows, key=lambda r: r[k]) if rows else None


def money(x):
    """Целую цену отдавать целым числом — иначе на сайте «69.0 €»."""
    return int(x) if float(x).is_integer() else round(float(x), 2)


def build(ow, rt, today, snapshot):
    regions, by_country = [], {}
    for code in sorted(DESTS):
        meta = DESTS[code]
        mine_ow = [r for r in ow if r["dest"] == code]
        mine_rt = [r for r in rt if r["dest"] == code]
        o, t = cheapest(mine_ow), cheapest(mine_rt)
        if not o and not t:
            continue        # в программе есть, а билетов нет — как Бодрум
        dates = sorted(dt.datetime.strptime(r["date"], DMY).date()
                       for r in mine_ow)
        rec = {
            "slug": slug(meta["region_lt"]),
            "name": meta["region_lt"],
            "country": meta["country_lt"],
            "country_slug": slug(meta["country_lt"]),
            "iata": code,
            "flights_count": len(mine_ow),
            "season_first": dates[0].strftime(DMY) if dates else None,
            "season_last": dates[-1].strftime(DMY) if dates else None,
            "one_way": None,
            "round_trip": None,
        }
        if o:
            rec["one_way"] = {
                "price": money(o["price"]), "date": o["date"],
                "flight": o["flight"], "departs": o["dep"],
                "arrives": o["arr"], "airline": o["airline"],
                "url": link(code, o["date"]),
            }
        if t:
            rec["round_trip"] = {
                "price": money(t["price"]), "date_out": t["date_out"],
                "date_back": t["date_back"], "nights": NIGHTS,
                "flight_out": t["flight_out"], "flight_back": t["flight_back"],
                "url": link(code, t["date_out"], t["date_back"]),
            }
        regions.append(rec)
        by_country.setdefault(meta["country_lt"], []).append(rec)

    countries = []
    for name, regs in by_country.items():
        ows = [r["one_way"] for r in regs if r["one_way"]]
        rts = [r["round_trip"] for r in regs if r["round_trip"]]
        countries.append({
            "slug": slug(name), "name": name,
            "regions": [r["slug"] for r in regs],
            "regions_count": len(regs),
            "one_way": cheapest(ows) if ows else None,
            "round_trip": cheapest(rts) if rts else None,
        })

    # вторым ключом slug — чтобы при равных ценах порядок не плавал
    regions.sort(key=lambda r: ((r["one_way"] or r["round_trip"])["price"],
                                r["slug"]))
    countries.sort(key=lambda c: ((c["one_way"] or c["round_trip"])["price"],
                                  c["slug"]))
    return {
        "generated_at": today.isoformat(),
        "source": GATE,
        "program_snapshot": snapshot,
        "currency": "EUR",
        "origin": "VNO",
        "origin_name": "Vilnius",
        "horizon_days": HORIZON,
        "round_trip_nights": NIGHTS,
        "price_basis": "vienam asmeniui",
        "countries": countries,
        "regions": regions,
    }


if __name__ == "__main__":
    import sys
    today = (dt.date.fromisoformat(os.environ["FEED_DATE"])
             if os.environ.get("FEED_DATE") else dt.date.today())

    snapshot, dests, warn = program.load(today)
    DESTS.update(dests)
    for w in warn:
        print(f"! {w}", file=sys.stderr)

    candidates = program.dates_in_program(DESTS, today, HORIZON)
    cache = load_cache()
    jobs, fresh, disco = plan(today, cache, candidates)
    print(f"дата {today}, снимок программы {snapshot}, "
          f"направлений {len(DESTS)}, горизонт {HORIZON} дн", flush=True)
    print(f"дат в программе {sum(len(v) for v in candidates.values())}, "
          f"опрашиваем {len(jobs)} (известных {fresh}, поиск новых {disco}), "
          f"запросов {len(jobs) * 2}, потолок {RPS}/с", flush=True)

    failed = refresh(jobs, cache)
    if failed:
        print(f"НЕ ОТВЕТИЛИ {len(failed)}:", file=sys.stderr)
        for s in failed[:5]:
            print(f"  {s}", file=sys.stderr)
    if len(failed) > checks.MAX_FAILED:
        sys.exit(f"шлюз не ответил на {len(failed)} запросов — "
                 f"выборка неполная, фид не обновляем")

    dropped = prune(cache, today, candidates)
    # запас пишем до проверки: это наблюдённые факты, они верны всегда,
    # иначе отказ публикации выбрасывал бы весь прогон
    json.dump(cache, open(CACHE, "w"), ensure_ascii=False,
              indent=1, sort_keys=True)

    ow, rt = rows_from_cache(cache)
    feed = build(ow, rt, today, snapshot)

    try:
        previous = json.load(open(OUT))
    except (OSError, ValueError):
        previous = None
    problems = checks.check(feed, previous)
    if problems:
        print("ФИД НЕ ОБНОВЛЁН (запас сохранён):", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        sys.exit(1)

    json.dump(feed, open(OUT, "w"), ensure_ascii=False, indent=1)
    print(f"выкинуто дат {dropped}, в запасе "
          f"{len(cache['ow']) + len(cache['rt'])}, "
          f"стран {len(feed['countries'])}, регионов {len(feed['regions'])}")
