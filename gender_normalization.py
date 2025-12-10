from typing import Dict, Tuple

VALID_GENDERS = {"male", "female"}

def _clean_gender(value: str) -> str:
    value = (value or "").strip().lower()
    if value in {"m", "man", "boy"}:
        return "male"
    if value in {"f", "woman", "girl"}:
        return "female"
    return value

def normalize_people_dicts(
    person1: Dict, person2: Dict
) -> Tuple[Dict, Dict, bool, str]:
    p1 = dict(person1)
    p2 = dict(person2)

    p1_gender = _clean_gender(str(p1.get("gender", "")))
    p2_gender = _clean_gender(str(p2.get("gender", "")))

    if p1_gender not in VALID_GENDERS:
        return p1, p2, False, f"Invalid gender for person1: {p1.get('gender')!r}"
    if p2_gender not in VALID_GENDERS:
        return p1, p2, False, f"Invalid gender for person2: {p2.get('gender')!r}"

    p1["gender"] = p1_gender
    p2["gender"] = p2_gender

    if p1_gender == "female" and p2_gender == "male":
        p1, p2 = p2, p1
        return p1, p2, True, ""

    return p1, p2, False, ""