import re
import unicodedata

_punct_re = re.compile(r"[^\w\s]+", re.UNICODE)
_space_re = re.compile(r"\s+")

def normalize(text: str) -> str:
    if not text:
        return ""
    t = text.strip().lower()

    # replace ё -> е
    t = t.replace("ё", "е")

    # remove diacritics (e.g., é -> e)
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if not unicodedata.combining(ch))

    # remove punctuation/symbols
    t = _punct_re.sub(" ", t)

    # collapse spaces
    t = _space_re.sub(" ", t).strip()

    return t
