#!/usr/bin/env python3
"""Проверки перед публикацией фида и их пороги.

Отдельным файлом нарочно: пороги — то, что придётся подкручивать, и править
их удобнее здесь, а не в теле сборщика.

Задача проверок — ловить поломку, а не сезонность. Состав направлений
законно редеет: Египет и Тенерифе кончаются апрелем, Малага живёт один
октябрь. Поэтому порог — «совсем пусто», а не «меньше, чем сегодня»:
привязка к нынешним пятнадцати роняла бы сборку каждую ночь без причины,
а это хуже, чем отсутствие проверки.
"""
import os

# Больше столько запросов без ответа — выборка неполная, публиковать нельзя.
MAX_FAILED = int(os.environ.get("FEED_MAX_FAILED", 20))

# Ниже этого числа регионов фид считается пустым.
MIN_REGIONS = int(os.environ.get("FEED_MIN_REGIONS", 6))

# Насколько состав может просесть за сутки. На сломе сезона несколько
# направлений уходят разом, поэтому «было минус один» не годится.
MAX_DROP = int(os.environ.get("FEED_MAX_DROP", 3))

# Во сколько раз цена может вырасти, прежде чем это станет подозрительным.
MAX_PRICE_JUMP = float(os.environ.get("FEED_MAX_PRICE_JUMP", 3))


def check(feed, previous=None):
    """Список замечаний. Пустой список — можно публиковать."""
    bad = []
    now = len(feed["regions"])

    if now < MIN_REGIONS:
        bad.append(f"регионов всего {now}, ожидалось не меньше {MIN_REGIONS}")

    for r in feed["regions"]:
        if not r["one_way"] and not r["round_trip"]:
            bad.append(f"{r['slug']}: нет ни одной цены")

    if not previous:
        return bad

    was = len(previous.get("regions", []))
    allowed = max(MAX_DROP, was // 3)
    if was and was - now > allowed:
        bad.append(f"регионов стало {now}, было {was} — "
                   f"пропало {was - now}, допустимо {allowed}")

    old = {r["slug"]: r for r in previous.get("regions", [])}
    for r in feed["regions"]:
        p = old.get(r["slug"])
        if not (p and p.get("one_way") and r.get("one_way")):
            continue
        a, b = p["one_way"]["price"], r["one_way"]["price"]
        if a and b / a > MAX_PRICE_JUMP:
            bad.append(f"{r['slug']}: цена в одну {a} → {b}, "
                       f"рост больше чем в {MAX_PRICE_JUMP:g} раза")
    return bad
