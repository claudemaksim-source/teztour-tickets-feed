#!/usr/bin/env python3
"""Из tickets-from.json делает файлы под синхронизацию коллекций Framer CMS.

Зачем отдельно. Плагин синхронизации CMS ждёт **плоский массив** записей
сверху, а в tickets-from.json объект с двумя списками внутри — его плагин
не возьмёт. И даты он принимает только как ГГГГ-ММ-ДД, а у нас ДД.ММ.ГГГГ,
потому что в таком виде их ждёт шлюз и показывает сайт.

Поэтому здесь: разворачиваем в массив, даты даём в двух видах (ISO для поля
типа «дата», человеческий для подписи), цены числами, ссылки строками.

Пишет два файла:
  cms-regions.json   — по одной записи на регион
  cms-countries.json — по одной на страну, цена «от» по её регионам
"""
import datetime as dt
import json
import os

SRC = os.environ.get("FEED_OUT", "tickets-from.json")
OUT_REGIONS = "cms-regions.json"
OUT_COUNTRIES = "cms-countries.json"
DMY = "%d.%m.%Y"


def iso(d):
    """ДД.ММ.ГГГГ → ГГГГ-ММ-ДД. Поле даты в CMS другого не принимает."""
    if not d:
        return None
    return dt.datetime.strptime(d, DMY).date().isoformat()


def row(rec, kind):
    """Одна запись коллекции. Плоская: вложенного CMS не понимает."""
    ow = rec.get("one_way") or {}
    rt = rec.get("round_trip") or {}
    out = {
        "slug": rec["slug"],
        "name": rec["name"],

        # в одну сторону
        "price_one_way": ow.get("price"),
        "date_one_way": iso(ow.get("date")),
        "date_one_way_text": ow.get("date"),
        "url_one_way": ow.get("url"),

        # туда-обратно, 7 ночей
        "price_round_trip": rt.get("price"),
        "date_round_trip_out": iso(rt.get("date_out")),
        "date_round_trip_back": iso(rt.get("date_back")),
        "date_round_trip_text": (f"{rt['date_out']} – {rt['date_back']}"
                                 if rt.get("date_out") else None),
        "url_round_trip": rt.get("url"),

        "currency": "EUR",
        "price_basis": "vienam asmeniui",
    }
    if kind == "region":
        out.update({
            "country": rec["country"],
            "country_slug": rec["country_slug"],
            "iata": rec["iata"],
            "airline": ow.get("airline") or None,
            "flight_one_way": ow.get("flight") or None,
            "departs": ow.get("departs") or None,
            "arrives": ow.get("arrives") or None,
            "flights_count": rec.get("flights_count"),
            "season_first": iso(rec.get("season_first")),
            "season_last": iso(rec.get("season_last")),
        })
    else:
        out.update({
            "regions_count": rec.get("regions_count"),
            "regions": ", ".join(rec.get("regions", [])),
        })
    return out


def build(feed):
    regions = [row(r, "region") for r in feed["regions"]]
    countries = [row(c, "country") for c in feed["countries"]]
    return regions, countries


if __name__ == "__main__":
    feed = json.load(open(SRC))
    regions, countries = build(feed)
    json.dump(regions, open(OUT_REGIONS, "w"), ensure_ascii=False, indent=1)
    json.dump(countries, open(OUT_COUNTRIES, "w"), ensure_ascii=False, indent=1)
    print(f"{OUT_REGIONS}: {len(regions)} записей, "
          f"{OUT_COUNTRIES}: {len(countries)} записей")
