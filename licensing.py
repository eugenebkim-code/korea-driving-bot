# licensing.py
import secrets
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple


# =========================
# time helpers
# =========================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_iso(dt_s: str) -> Optional[datetime]:
    if not dt_s:
        return None
    s = str(dt_s).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def a1(tab: str, rng: str) -> str:
    safe = (tab or "").replace("'", "''")
    return f"'{safe}'!{rng}"


# =========================
# config
# =========================

@dataclass
class LicenseConfig:
    sheet_id: str
    keys_sheet: str = "keys"
    subs_sheet: str = "subs"
    trial_sheet: str = "trial"


# =========================
# manager
# =========================

class LicenseManager:
    """
    Google Sheets schema:

    keys (A:I):
      A key
      B plan
      C days
      D created_at
      E created_by
      F status (new/used/revoked)
      G used_by
      H used_at
      I expires_at

    subs (A:E):
      A user_id
      B expires_at
      C plan
      D activated_at
      E source_key

    trial (A:D):
      A user_id
      B created_at
      C pool_qids
      D exam_used ("0"/"1")
    """

    def __init__(self, *, service, gs_execute, cfg: LicenseConfig):
        self.service = service
        self.gs_execute = gs_execute
        self.cfg = cfg

    # ---------- low-level ----------
    def _get_values(self, range_a1: str) -> List[List[str]]:
        req = self.service.spreadsheets().values().get(
            spreadsheetId=self.cfg.sheet_id,
            range=range_a1,
        )
        return (self.gs_execute(req) or {}).get("values", []) or []

    def _append(self, range_a1: str, values: List[List[Any]]) -> Dict[str, Any]:
        req = self.service.spreadsheets().values().append(
            spreadsheetId=self.cfg.sheet_id,
            range=range_a1,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        )
        return self.gs_execute(req)

    def _update(self, range_a1: str, values: List[List[Any]]) -> Dict[str, Any]:
        req = self.service.spreadsheets().values().update(
            spreadsheetId=self.cfg.sheet_id,
            range=range_a1,
            valueInputOption="RAW",
            body={"values": values},
        )
        return self.gs_execute(req)

    # ---------- ensure headers ----------
    def ensure_headers(self) -> None:
        # keys
        keys = self._get_values(a1(self.cfg.keys_sheet, "A1:I1"))
        if not keys:
            self._update(a1(self.cfg.keys_sheet, "A1:I1"), [[
                "key", "plan", "days", "created_at", "created_by",
                "status", "used_by", "used_at", "expires_at"
            ]])

        # subs
        subs = self._get_values(a1(self.cfg.subs_sheet, "A1:E1"))
        if not subs:
            self._update(a1(self.cfg.subs_sheet, "A1:E1"), [[
                "user_id", "expires_at", "plan", "activated_at", "source_key"
            ]])

        # trial
        trial = self._get_values(a1(self.cfg.trial_sheet, "A1:D1"))
        if not trial:
            self._update(a1(self.cfg.trial_sheet, "A1:D1"), [[
                "user_id", "created_at", "pool_qids", "exam_used"
            ]])

    # =========================
    # keys helpers
    # =========================

    def generate_key(self, plan: str) -> Dict[str, Any]:
        plan = (plan or "").strip().lower()
        if plan not in ("week", "month"):
            raise ValueError("plan must be 'week' or 'month'")
        days = 7 if plan == "week" else 30

        raw = secrets.token_urlsafe(18)
        key = "KR-" + "-".join([
            raw[:4].upper(),
            raw[4:8].upper(),
            raw[8:12].upper(),
        ])
        return {"key": key, "days": days, "plan": plan}

    def create_keys(self, *, plan: str, count: int, created_by: int) -> Dict[str, Any]:
        self.ensure_headers()
        count = max(1, min(int(count), 200))
        created_at = iso_z(now_utc())

        keys: List[str] = []
        rows: List[List[str]] = []

        for _ in range(count):
            k = self.generate_key(plan)
            keys.append(k["key"])
            rows.append([
                k["key"], k["plan"], str(k["days"]),
                created_at, str(created_by),
                "new", "", "", ""
            ])

        self._append(a1(self.cfg.keys_sheet, "A:I"), rows)
        return {"plan": plan, "count": count, "keys": keys}

    def _find_key_row(self, key: str) -> Tuple[Optional[int], Optional[List[str]]]:
        """
        Returns: (row_index_in_sheet, row_values)
        row_index is 2-based (since A2 is first data row)
        """
        self.ensure_headers()
        key_u = (key or "").strip().upper()
        if not key_u:
            return None, None

        values = self._get_values(a1(self.cfg.keys_sheet, "A2:I"))
        for i, row in enumerate(values, start=2):
            if row and str(row[0]).strip().upper() == key_u:
                return i, row
        return None, None

    def revoke_key(self, *, key: str, revoked_by: int = 0) -> Dict[str, Any]:
        """
        Sets status='revoked'. Does NOT rollback existing subs.
        """
        row_i, row = self._find_key_row(key)
        if not key or not str(key).strip():
            return {"ok": False, "reason": "empty_key"}
        if not row_i or not row:
            return {"ok": False, "reason": "not_found"}

        status = (row[5] if len(row) > 5 else "").strip().lower()
        if status == "revoked":
            return {"ok": True, "already": True, "status": "revoked"}

        # Just set status column (F)
        self._update(a1(self.cfg.keys_sheet, f"F{row_i}"), [["revoked"]])
        return {"ok": True, "status": "revoked", "was": status or "new"}

    def unrevoke_key(self, *, key: str) -> Dict[str, Any]:
        """
        Sets status='new' if currently revoked.
        """
        row_i, row = self._find_key_row(key)
        if not key or not str(key).strip():
            return {"ok": False, "reason": "empty_key"}
        if not row_i or not row:
            return {"ok": False, "reason": "not_found"}

        status = (row[5] if len(row) > 5 else "").strip().lower()
        if status != "revoked":
            return {"ok": True, "noop": True, "status": status or "new"}

        self._update(a1(self.cfg.keys_sheet, f"F{row_i}"), [["new"]])
        return {"ok": True, "status": "new"}

    # =========================
    # subs
    # =========================

    def get_user_sub(self, user_id: int) -> Optional[Dict[str, Any]]:
        self.ensure_headers()
        values = self._get_values(a1(self.cfg.subs_sheet, "A2:E"))
        uid = str(user_id)

        for i, row in enumerate(values, start=2):
            if len(row) >= 2 and row[0] == uid:
                return {
                    "row": i,
                    "user_id": uid,
                    "expires_at": row[1] if len(row) > 1 else "",
                    "plan": row[2] if len(row) > 2 else "",
                    "activated_at": row[3] if len(row) > 3 else "",
                    "source_key": row[4] if len(row) > 4 else "",
                }
        return None

    def is_active(self, user_id: int) -> bool:
        sub = self.get_user_sub(user_id)
        if not sub:
            return False
        exp = parse_iso(sub.get("expires_at", ""))
        return bool(exp and exp > now_utc())

    def redeem(self, *, user_id: int, key: str) -> Dict[str, Any]:
        self.ensure_headers()
        key_u = (key or "").strip().upper()
        if not key_u:
            return {"ok": False, "reason": "empty_key"}

        row_i, row = self._find_key_row(key_u)
        if not row_i or not row:
            return {"ok": False, "reason": "not_found"}

        status = (row[5] if len(row) > 5 else "").strip().lower()
        if status == "revoked":
            return {"ok": False, "reason": "revoked"}
        if status == "used":
            return {"ok": False, "reason": "used"}

        plan = (row[1] if len(row) > 1 else "week").strip().lower() or "week"
        days_s = (row[2] if len(row) > 2 else "7").strip()
        try:
            days = int(days_s)
        except Exception:
            days = 7 if plan == "week" else 30

        # extend from current expiration if still active
        base = now_utc()
        cur = self.get_user_sub(user_id)
        if cur:
            cur_exp = parse_iso(cur.get("expires_at", ""))
            if cur_exp and cur_exp > base:
                base = cur_exp

        new_exp = base + timedelta(days=days)
        used_at = iso_z(now_utc())
        expires_at = iso_z(new_exp)
        uid = str(user_id)

        # mark key used (F:I)
        self._update(a1(self.cfg.keys_sheet, f"F{row_i}:I{row_i}"), [[
            "used", uid, used_at, expires_at
        ]])

        # upsert sub
        sub = self.get_user_sub(user_id)
        if sub:
            r = sub["row"]
            self._update(a1(self.cfg.subs_sheet, f"B{r}:E{r}"), [[
                expires_at, plan, used_at, key_u
            ]])
        else:
            self._append(a1(self.cfg.subs_sheet, "A:E"), [[
                uid, expires_at, plan, used_at, key_u
            ]])

        return {"ok": True, "plan": plan, "expires_at": expires_at, "days": days}

    def deactivate_user(self, *, user_id: int) -> Dict[str, Any]:
        """
        Admin utility: immediately deactivate subscription (set expires_at to past).
        """
        self.ensure_headers()
        sub = self.get_user_sub(user_id)
        if not sub:
            return {"ok": False, "reason": "no_sub"}

        r = sub["row"]
        past = iso_z(now_utc() - timedelta(seconds=5))
        plan = sub.get("plan", "") or ""
        activated_at = sub.get("activated_at", "") or ""
        source_key = sub.get("source_key", "") or ""
        self._update(a1(self.cfg.subs_sheet, f"B{r}:E{r}"), [[past, plan, activated_at, source_key]])
        return {"ok": True, "expires_at": past}

    # =========================
    # trial
    # =========================

    def get_trial(self, user_id: int) -> Optional[Dict[str, Any]]:
        self.ensure_headers()
        values = self._get_values(a1(self.cfg.trial_sheet, "A2:D"))
        uid = str(user_id)

        for i, row in enumerate(values, start=2):
            if row and row[0] == uid:
                return {
                    "row": i,
                    "user_id": uid,
                    "created_at": row[1] if len(row) > 1 else "",
                    "pool_qids": row[2] if len(row) > 2 else "",
                    "exam_used": row[3] if len(row) > 3 else "0",
                }
        return None

    def ensure_trial_pool(self, user_id: int, pool_qids: List[str]) -> Dict[str, Any]:
        """
        If trial exists with non-empty pool_qids -> return it.
        Else create/update with given pool_qids.
        """
        self.ensure_headers()
        t = self.get_trial(user_id)
        if t and (t.get("pool_qids") or "").strip():
            return t

        uid = str(user_id)
        created_at = iso_z(now_utc())
        pool_str = ",".join([str(x).strip().upper() for x in pool_qids if str(x).strip()])
        exam_used = "0"

        if t:
            r = t["row"]
            self._update(a1(self.cfg.trial_sheet, f"B{r}:D{r}"), [[created_at, pool_str, exam_used]])
            return self.get_trial(user_id) or {"pool_qids": pool_str, "exam_used": "0"}

        self._append(a1(self.cfg.trial_sheet, "A:D"), [[uid, created_at, pool_str, exam_used]])
        return self.get_trial(user_id) or {"pool_qids": pool_str, "exam_used": "0"}

    def trial_exam_available(self, user_id: int) -> bool:
        t = self.get_trial(user_id)
        if not t:
            return True
        return str(t.get("exam_used", "0")).strip() != "1"

    def mark_trial_exam_used(self, user_id: int) -> None:
        self.ensure_headers()
        t = self.get_trial(user_id)
        uid = str(user_id)

        if t:
            r = t["row"]
            self._update(a1(self.cfg.trial_sheet, f"D{r}"), [["1"]])
            return

        created_at = iso_z(now_utc())
        self._append(a1(self.cfg.trial_sheet, "A:D"), [[uid, created_at, "", "1"]])
