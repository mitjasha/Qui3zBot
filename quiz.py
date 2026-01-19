import json
import os
import random
import time
from typing import Dict, List, Iterable, Optional

from textnorm import normalize


def now_ts() -> int:
    return int(time.time())


class QuizEngine:
    """
    Loads questions from:
      - a single JSON file, OR
      - a directory containing many JSON files

    Supported JSON formats:
      A) {"dataset": {...}, "questions": [ ... ]}
      B) [ ... ]  (list of questions)

    Expected question schema (minimal):
      {
        "id": "...",
        "category": "Кино" | "География" | ... (optional but recommended),
        "question": "....",
        "answers": ["..."],
        "aliases": ["..."] (optional),
        "tags": ["..."] (optional),
        "difficulty": 1..5 (optional),
        "lang": "ru" (optional)
      }
    """

    def __init__(self, path: str):
        self.questions: List[Dict] = []
        self.by_id: Dict[str, Dict] = {}

        # Bags for random non-repeating draws
        self._bag_by_tag: dict[str, List[str]] = {}
        self._bag_by_category: dict[str, List[str]] = {}

        self.load(path)

    # ------------------- public API -------------------

    def list_tags(self) -> List[str]:
        tags = set()
        for q in self.questions:
            for t in q.get("tags", []):
                tags.add(t)
        tags.discard("all")
        return sorted(tags)

    def list_categories(self) -> List[str]:
        cats = set()
        for q in self.questions:
            c = q.get("category")
            if c:
                cats.add(c)
        return sorted(cats)

    def next_question(self, tag: str = "all", category: Optional[str] = None) -> Dict:
        """
        If category is provided, pick within that category (ignores tag),
        else pick within tag.
        """
        if category:
            return self._next_by_category(category)

        # default tag behavior
        if tag not in self._bag_by_tag or not self._bag_by_tag[tag]:
            ids = self._ids_for_tag(tag)
            random.shuffle(ids)
            self._bag_by_tag[tag] = ids

        qid = self._bag_by_tag[tag].pop()
        return self.by_id[qid]

    def check_answer(self, question: Dict, user_text: str) -> bool:
        t = normalize(user_text)
        if not t:
            return False

        correct = set()
        for a in question.get("answers", []):
            correct.add(normalize(a))
        for a in question.get("aliases", []):
            correct.add(normalize(a))

        return t in correct

    # ------------------- loading -------------------

    def load(self, path: str):
        self.questions = []
        self.by_id = {}
        self._bag_by_tag = {}
        self._bag_by_category = {}

        if os.path.isdir(path):
            files = self._iter_json_files(path)
            for fp in files:
                self._load_file(fp)
        else:
            self._load_file(path)

        # Build by_id
        for q in self.questions:
            self.by_id[str(q["id"])] = q

    def _iter_json_files(self, dir_path: str) -> List[str]:
        out = []
        for root, _, files in os.walk(dir_path):
            for fn in files:
                if fn.lower().endswith(".json"):
                    out.append(os.path.join(root, fn))
        out.sort()
        return out

    def _load_file(self, file_path: str):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, dict) and "questions" in data and isinstance(data["questions"], list):
            items = data["questions"]
        elif isinstance(data, list):
            items = data
        else:
            # unknown format -> ignore
            return

        for raw in items:
            q = self._normalize_question(raw, file_path)
            if q:
                self.questions.append(q)

    def _normalize_question(self, q: Dict, file_path: str) -> Optional[Dict]:
        if not isinstance(q, dict):
            return None

        # Must have question text
        text = q.get("question")
        if not isinstance(text, str) or not text.strip():
            return None

        # Must have answers list
        answers = q.get("answers")
        if not isinstance(answers, list) or not any(isinstance(a, str) and a.strip() for a in answers):
            return None

        qid = q.get("id")
        if qid is None:
            # fallback id
            qid = f"{os.path.basename(file_path)}::{len(self.questions)+1}"

        category = q.get("category")
        tags = q.get("tags") or []

        # If tags are missing but category exists -> add category tag (lowercase normalized)
        if not isinstance(tags, list):
            tags = []
        if category and isinstance(category, str) and category.strip():
            cat_tag = normalize(category)
            if cat_tag and cat_tag not in [normalize(t) for t in tags if isinstance(t, str)]:
                tags.append(cat_tag)

        aliases = q.get("aliases") or []
        if not isinstance(aliases, list):
            aliases = []

        # clean strings
        def clean_list(xs: Iterable) -> List[str]:
            out = []
            for x in xs:
                if isinstance(x, str):
                    s = x.strip()
                    if s:
                        out.append(s)
            return out

        out = {
            "id": str(qid),
            "category": category.strip() if isinstance(category, str) else None,
            "question": text.strip(),
            "answers": clean_list(answers),
            "aliases": clean_list(aliases),
            "tags": clean_list(tags),
            "difficulty": int(q.get("difficulty", 2)) if str(q.get("difficulty", "")).isdigit() else q.get("difficulty", 2),
            "lang": q.get("lang", "ru"),
            "meta": q.get("meta", {}),
        }
        return out

    # ------------------- selection helpers -------------------

    def _ids_for_tag(self, tag: str) -> List[str]:
        if tag == "all":
            return [str(q["id"]) for q in self.questions]

        tag_n = normalize(tag)
        ids = []
        for q in self.questions:
            tags = [normalize(t) for t in q.get("tags", [])]
            if tag_n in tags:
                ids.append(str(q["id"]))
        return ids

    def _next_by_category(self, category: str) -> Dict:
        cat_n = normalize(category)
        if cat_n not in self._bag_by_category or not self._bag_by_category[cat_n]:
            ids = []
            for q in self.questions:
                c = q.get("category") or ""
                if normalize(c) == cat_n:
                    ids.append(str(q["id"]))
            random.shuffle(ids)
            self._bag_by_category[cat_n] = ids

        if not self._bag_by_category[cat_n]:
            # fallback to all
            return self.next_question(tag="all")

        qid = self._bag_by_category[cat_n].pop()
        return self.by_id[qid]
