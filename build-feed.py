#!/usr/bin/env python3
"""Фид «цена от» по странам и регионам для вывода на сайте.

Источник — открытый чартерный поиск шлюза: xml.tezapi.eu/ticketsearch/search.
Пароль не нужен, белый список не нужен.

Две цены считаются ОТДЕЛЬНЫМИ запросами, и это принципиально: круговой
тариф дешевле суммы двух односторонних (проверено: 140+140 за круг против
165+165 по отдельности). Сложением цену туда-обратно получить нельзя.

Цена в ответе шлюза — на всю группу, поэтому запрашивается один взрослый.
Поле свободных мест всегда 10 и смысла не несёт, в фид не попадает.

## Почему обход бережливый

Полный год — 8760 запросов в сутки. Это несоразмерно двенадцати числам на
выходе: дальние цены почти не двигаются, скачет ближняя неделя-две. К тому
же единственное ограничение, которое TEZ где-либо документирует, — 50
запросов в минуту (у поиска туров), а без потолка выходило 850-1250.

Поэтому: найденные цены хранятся в prices-cache.json между запусками, а
каждый прогон опрашивает только
  * ближние NEAR_DAYS дней, где цены реально меняются;
  * даты, на которых сейчас стоит минимум, — чтобы опубликованная цена
    всегда была свежепроверенной, а не унаследованной из запаса;
  * одну седьмую дальнего хвоста, так что полный круг выходит за неделю.
Плюс жёсткий потолок FEED_RPS запросов в секунду.
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

GATE = "https://xml.tezapi.eu/ticketsearch/search"
EUR = 18864
VNO = "VNO"
# Год, а не полгода: Бургас, Варна, Корфу и Родос начинаются в мае-июне,
# и при горизонте 180 дней выпадали из фида целиком вместе с Болгарией.
HORIZON = int(os.environ.get("FEED_HORIZON", 365))      # дней вперёд
NIGHTS = 7             # для цены туда-обратно
WORKERS = int(os.environ.get("FEED_WORKERS", 4))
RPS = float(os.environ.get("FEED_RPS", 2))              # запросов в секунду
NEAR_DAYS = int(os.environ.get("FEED_NEAR_DAYS", 60))   # ближнее окно
TAIL_SLICES = int(os.environ.get("FEED_TAIL_SLICES", 7))  # хвост за столько прогонов

CACHE = "prices-cache.json"
OUT = "tickets-from.json"

# Ссылка на страницу билета. Подробности формата — в README.
LINK = "https://www.teztour.lt/flightsearch"
LINK_ORIGIN = 21129    # Вильнюс

# Двенадцать направлений; найдены перебором 302 кодов IATA в трёх сезонных
# окнах — короткий список пропускал Зальцбург и Мадейру.
# airport — id аэропорта для ссылки (НЕ id города: город Тенерифе выдачи не даёт).
DESTS = {
    "AYT": {"airport": 23134, "region_lt": "Antalija",         "country_lt": "Turkija"},
    "HER": {"airport": 28889, "region_lt": "Heraklionas",      "country_lt": "Graikija"},
    "RHO": {"airport": 36369, "region_lt": "Rodas",            "country_lt": "Graikija"},
    "CFU": {"airport": 26019, "region_lt": "Korfu",            "country_lt": "Graikija"},
    "HRG": {"airport": 29169, "region_lt": "Hurgada",          "country_lt": "Egiptas"},
    "SSH": {"airport": 37534, "region_lt": "Šarm el Šeichas",  "country_lt": "Egiptas"},
    "TFS": {"airport": 38889, "region_lt": "Tenerifė",         "country_lt": "Ispanija"},
    "AGP": {"airport": 32399, "region_lt": "Malaga",           "country_lt": "Ispanija"},
    "BOJ": {"airport": 24729, "region_lt": "Burgasas",         "country_lt": "Bulgarija"},
    "VAR": {"airport": 39919, "region_lt": "Varna",            "country_lt": "Bulgarija"},
    "SZG": {"airport": 36849, "region_lt": "Zalcburgas",       "country_lt": "Austrija"},
    "FNC": {"airport": 26134, "region_lt": "Madeira",          "country_lt": "Portugalija"},
}

LT_MAP = str.maketrans({"ą": "a", "č": "c", "ę": "e", "ė": "e", "į": "i",
                        "š": "s", "ų": "u", "ū": "u", "ž": "z"})

DMY = "%d.%m.%Y"


def slug(s):
    """Латиница, цифры и дефисы — как в адресах страницы предложений."""
    s = s.lower().translate(LT_MAP)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s)).strip("-")


# ── обращение к шлюзу ─────────────────────────────────────────

class GateError(RuntimeError):
    """Шлюз не ответил осмысленно после всех попыток."""


_gate_lock = threading.Lock()
_next_slot = [0.0]


def _throttle():
    """Потолок скорости на все потоки разом.

    Без него восемь потоков давали 1250 запросов в минуту — в 25 раз выше
    единственного ограничения, которое TEZ документирует. Отказа пока не
    было, но это недосмотр на их стороне, а не разрешение.
    """
    gap = 1.0 / RPS if RPS > 0 else 0
    with _gate_lock:
        now = time.monotonic()
        wait = _next_slot[0] - now
        _next_slot[0] = max(now, _next_slot[0]) + gap
    if wait > 0:
        time.sleep(wait)


def _get(params, attempts=4):
    """Ответ шлюза или исключение.

    Без этого любой сбой возвращал пустую строку, рейсов в ответе
    оказывалось ноль, и цена молча подскакивала до следующей по списку —
    на расписании это опубликованная неверная цена без признаков сбоя.
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
        out.append({"price": p,
                    "flight": _field(b, "flightNameTo"),
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
        out.append({"price": a + c,
                    "flight_out": _field(b, "flightNameTo"),
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


# ── запас данных между запусками ────────────────────────────────

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


def plan(today, cache):
    """Что опрашиваем в этот прогон.

    Ближнее окно — целиком; хвост — одна седьмая, своя в каждый день;
    и сверх того даты, на которых стоит нынешний минимум, иначе
    опубликованная цена могла бы неделю жить непроверенной.
    """
    days = [today + dt.timedelta(days=i) for i in range(1, HORIZON + 1)]
    near = [d.strftime(DMY) for d in days[:NEAR_DAYS]]
    tail = days[NEAR_DAYS:]
    sl = today.toordinal() % TAIL_SLICES
    tail_part = [d.strftime(DMY) for i, d in enumerate(tail)
                 if i % TAIL_SLICES == sl]
    base = set(near) | set(tail_part)

    jobs = {"ow": set(), "rt": set()}
    for dest in DESTS:
        for d in base:
            jobs["ow"].add((dest, d))
            jobs["rt"].add((dest, d))
        for mode in ("ow", "rt"):
            rows = [(k, cache[mode][k]) for k in cache[mode]
                    if unkey(k)[0] == dest and cache[mode][k]]
            if rows:
                best = min(rows, key=lambda kv: min(r["price"] for r in kv[1]))
                jobs[mode].add((dest, unkey(best[0])[1]))
    return sorted(jobs["ow"]), sorted(jobs["rt"])


def refresh(jobs_ow, jobs_rt, cache):
    """Опросить запланированное и вложить в запас. Пустой ответ — удаление."""
    failed = []

    def do(mode, dest, date):
        if mode == "ow":
            return search_ow(dest, date)
        back = (dt.datetime.strptime(date, DMY).date()
                + dt.timedelta(days=NIGHTS)).strftime(DMY)
        return search_rt(dest, date, back)

    for mode, jobs in (("ow", jobs_ow), ("rt", jobs_rt)):
        done = 0
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [(j, ex.submit(do, mode, *j)) for j in jobs]
            for (dest, date), f in futs:
                done += 1
                try:
                    rows = f.result()
                except GateError as e:
                    failed.append(str(e))
                    continue
                k = key(dest, date)
                if rows:
                    cache[mode][k] = rows
                else:
                    cache[mode].pop(k, None)
                if done % 200 == 0:
                    print(f"  {mode} … {done}/{len(jobs)}, сбоев {len(failed)}",
                          flush=True)
    return failed


def prune(cache, today):
    """Выкинуть прошедшие даты и всё за горизонтом."""
    lo, hi = today + dt.timedelta(days=1), today + dt.timedelta(days=HORIZON)
    removed = 0
    for mode in ("ow", "rt"):
        for k in list(cache[mode]):
            d = dt.datetime.strptime(unkey(k)[1], DMY).date()
            if d < lo or d > hi:
                del cache[mode][k]
                removed += 1
    return removed


def rows_from_cache(cache):
    """Запас → плоские списки, какие ждёт build()."""
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
    # порядок обязан быть устойчивым: при равных ценах min() иначе выбирал бы
    # в разных сборках разные рейсы, и ссылка в CMS дёргалась бы впустую
    ow.sort(key=lambda r: (r["dest"],
                           dt.datetime.strptime(r["date"], DMY),
                           r["flight"]))
    rt.sort(key=lambda r: (r["dest"],
                           dt.datetime.strptime(r["date_out"], DMY),
                           r["flight_out"]))
    return ow, rt


# ── сборка фида ────────────────────────────────────────────

def cheapest(rows, k="price"):
    return min(rows, key=lambda r: r[k]) if rows else None


def money(x):
    """Целую цену отдавать целым числом — иначе на сайте «69.0 €»."""
    return int(x) if float(x).is_integer() else round(float(x), 2)


def build(ow, rt, today):
    regions, by_country = [], {}
    for code, meta in DESTS.items():
        mine_ow = [r for r in ow if r["dest"] == code]
        mine_rt = [r for r in rt if r["dest"] == code]
        o, t = cheapest(mine_ow), cheapest(mine_rt)
        if not o and not t:
            continue                      # направление вне сезона целиком
        # сортировать датами, а не строками: "01.04.2027" < "31.10.2026"
        # лексикографически, и сезон выводился наизнанку
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
            "slug": slug(name),
            "name": name,
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
        "currency": "EUR",
        "origin": "VNO",
        "origin_name": "Vilnius",
        "horizon_days": HORIZON,
        "round_trip_nights": NIGHTS,
        "price_basis": "vienam asmeniui",
        "countries": countries,
        "regions": regions,
    }


# ── проверки перед публикацией ─────────────────────────────────

MAX_FAILED = 20     # выше — публиковать нельзя, цены будут с дырами
# знаем, что их 12; за год сезонность ни одного не скрывает.
# При коротком горизонте для проверки занижается через FEED_MIN_REGIONS.
MIN_REGIONS = int(os.environ.get("FEED_MIN_REGIONS", 11))


def check(feed, previous=None):
    """Лучше уронить сборку, чем опубликовать фид с дырами."""
    bad = []
    if len(feed["regions"]) < MIN_REGIONS:
        bad.append(f"регионов всего {len(feed['regions'])}, "
                   f"ожидалось не меньше {MIN_REGIONS}")
    for r in feed["regions"]:
        if not r["one_way"] and not r["round_trip"]:
            bad.append(f"{r['slug']}: нет ни одной цены")
    if previous:
        was = len(previous.get("regions", []))
        if was and len(feed["regions"]) < was - 1:
            bad.append(f"регионов стало {len(feed['regions'])}, было {was}")
        old = {r["slug"]: r for r in previous.get("regions", [])}
        for r in feed["regions"]:
            p = old.get(r["slug"])
            if not (p and p.get("one_way") and r.get("one_way")):
                continue
            a, b = p["one_way"]["price"], r["one_way"]["price"]
            if a and b / a > 3:
                bad.append(f"{r['slug']}: цена в одну {a} → {b}, рост втрое")
    return bad


if __name__ == "__main__":
    import sys
    today = (dt.date.fromisoformat(os.environ["FEED_DATE"])
             if os.environ.get("FEED_DATE") else dt.date.today())
    cache = load_cache()
    have = len(cache["ow"]) + len(cache["rt"])
    jobs_ow, jobs_rt = plan(today, cache)
    print(f"дата {today}, горизонт {HORIZON} дн, направлений {len(DESTS)}, "
          f"в запасе {have} дат, опрашиваем {len(jobs_ow) + len(jobs_rt)}, "
          f"потолок {RPS}/с", flush=True)

    failed = refresh(jobs_ow, jobs_rt, cache)
    if failed:
        print(f"НЕ ОТВЕТИЛИ {len(failed)}:", file=sys.stderr)
        for s in failed[:5]:
            print(f"  {s}", file=sys.stderr)
    if len(failed) > MAX_FAILED:
        sys.exit(f"шлюз не ответил на {len(failed)} запросов — "
                 f"выборка неполная, фид не обновляем")

    dropped = prune(cache, today)
    # запас пишем до проверки: это наблюдённые факты, они верны всегда.
    # Иначе отказ публикации выбрасывал бы весь прогон, и следующий
    # запуск опрашивал бы те же две с половиной тысячи дат заново.
    json.dump(cache, open(CACHE, "w"), ensure_ascii=False,
              indent=1, sort_keys=True)

    ow, rt = rows_from_cache(cache)
    feed = build(ow, rt, today)

    try:
        previous = json.load(open(OUT))
    except (OSError, ValueError):
        previous = None
    problems = check(feed, previous)
    if problems:
        print("ФИД НЕ ОБНОВЛЁН (запас сохранён):", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        sys.exit(1)

    json.dump(feed, open(OUT, "w"), ensure_ascii=False, indent=1)
    print(f"выкинуто дат {dropped}, в запасе "
          f"{len(cache['ow']) + len(cache['rt'])}, "
          f"стран {len(feed['countries'])}, регионов {len(feed['regions'])}")
