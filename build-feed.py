#!/usr/bin/env python3
"""Фид «цена от» по странам и регионам для вывода на сайте.

Источник — открытый чартерный поиск шлюза: xml.tezapi.eu/ticketsearch/search.
Пароль не нужен, белый список не нужен.

Две цены считаются ОТДЕЛЬНЫМИ запросами, и это принципиально: круговой
тариф дешевле суммы двух односторонних (проверено: 140+140 за круг против
165+165 по отдельности). Сложением цену туда-обратно получить нельзя.

Цена в ответе шлюза — на всю группу, поэтому запрашивается один взрослый.
Поле свободных мест всегда 10 и смысла не несёт, в фид не попадает.
"""
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
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
WORKERS = int(os.environ.get("FEED_WORKERS", 8))

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


def slug(s):
    """Латиница, цифры и дефисы — как в адресах страницы предложений."""
    s = s.lower().translate(LT_MAP)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s)).strip("-")


class GateError(RuntimeError):
    """Шлюз не ответил осмысленно после всех попыток."""


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
        out.append({"dest": dest, "date": date, "price": p,
                    "flight": _field(b, "flightNameTo"),
                    "dep": _field(b, "fdDepDateTo"),
                    "arr": _field(b, "fdArrDateTo"),
                    "airline": _field(b, "aircompanyNameTo"),
                    "country_raw": _field(b, "arrivalCountryNameTo"),
                    "region_raw": _field(b, "arrivalRegionNameTo")})
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
        out.append({"dest": dest, "date_out": date_out, "date_back": date_back,
                    "price": a + c,
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


def run(jobs, fn, label):
    """Выборка по сетке. Порядок результата — порядок jobs, а не готовности.

    Через as_completed порядок произволен, и при совпадающих ценах min()
    выбирал бы в разных сборках разные рейсы: дата и ссылка в CMS менялись
    бы без изменения цены.
    """
    res, failed = [], []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(fn, *j) for j in jobs]   # порядок futs = порядок jobs
        for i, f in enumerate(futs, 1):
            try:
                res += f.result()
            except GateError as e:
                failed.append(str(e))
            if i % 400 == 0:
                print(f"  {label} … {i}/{len(jobs)}, сбоев {len(failed)}",
                      flush=True)
    if failed:
        print(f"  {label}: НЕ ОТВЕТИЛИ {len(failed)} из {len(jobs)}", flush=True)
        for s in failed[:5]:
            print(f"    {s}", flush=True)
    return res, failed


MAX_FAILED = 20     # выше — публиковать нельзя, цены будут с дырами


def collect(today, horizon=HORIZON):
    days = [today + dt.timedelta(days=i) for i in range(1, horizon + 1)]
    fmt = lambda d: d.strftime("%d.%m.%Y")

    ow, f1 = run([(c, fmt(d)) for c in DESTS for d in days],
                 search_ow, "в одну")
    rt, f2 = run([(c, fmt(d), fmt(d + dt.timedelta(days=NIGHTS)))
                  for c in DESTS for d in days], search_rt, "круг")
    if len(f1) + len(f2) > MAX_FAILED:
        raise GateError(
            f"шлюз не ответил на {len(f1) + len(f2)} запросов — "
            f"выборка неполная, фид не обновляем")
    return ow, rt


def cheapest(rows, key="price"):
    return min(rows, key=lambda r: r[key]) if rows else None


def money(x):
    """Целую цену отдавать целым числом — иначе на сайте «69.0 €»."""
    return int(x) if float(x).is_integer() else round(float(x), 2)


def build(ow, rt, today):
    regions, by_country = [], {}
    for code, meta in DESTS.items():
        o = cheapest([r for r in ow if r["dest"] == code])
        t = cheapest([r for r in rt if r["dest"] == code])
        if not o and not t:
            continue                      # направление вне сезона целиком
        # сортировать датами, а не строками: "01.04.2027" < "31.10.2026"
        # лексикографически, и сезон выводился наизнанку
        dates = sorted((dt.datetime.strptime(r["date"], "%d.%m.%Y").date()
                        for r in ow if r["dest"] == code))
        dates = [d.strftime("%d.%m.%Y") for d in dates]
        rec = {
            "slug": slug(meta["region_lt"]),
            "name": meta["region_lt"],
            "country": meta["country_lt"],
            "country_slug": slug(meta["country_lt"]),
            "iata": code,
            "flights_count": len([r for r in ow if r["dest"] == code]),
            "season_first": dates[0] if dates else None,
            "season_last": dates[-1] if dates else None,
            "one_way": None,
            "round_trip": None,
        }
        if o:
            rec["one_way"] = {
                "price": money(o["price"]), "date": o["date"], "flight": o["flight"],
                "departs": o["dep"][11:16], "arrives": o["arr"][11:16],
                "airline": o["airline"], "url": link(code, o["date"]),
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


OUT = "tickets-from.json"
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
    print(f"дата сборки {today}, горизонт {HORIZON} дней, "
          f"направлений {len(DESTS)}, потоков {WORKERS}", flush=True)
    try:
        previous = json.load(open(OUT))
    except (OSError, ValueError):
        previous = None

    ow, rt = collect(today)
    feed = build(ow, rt, today)

    problems = check(feed, previous)
    if problems:
        print("ФИД НЕ ОБНОВЛЁН:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        sys.exit(1)

    json.dump(feed, open(OUT, "w"), ensure_ascii=False, indent=1)
    print(f"стран {len(feed['countries'])}, регионов {len(feed['regions'])}"
          f" → {OUT}")
