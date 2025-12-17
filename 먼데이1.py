import os
import json
from datetime import date
from typing import Optional, Dict, Any

import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

# =========================
# 환경 설정
# =========================
MONDAY_API_TOKEN = os.getenv("MONDAY_API_TOKEN")

MONDAY_API_URL = "https://api.monday.com/v2"

HEADERS = {
    "Authorization": MONDAY_API_TOKEN,
    "Content-Type": "application/json",
}

# =========================
# 환경에 맞게 교체할 값들
# =========================

# 하위아이템(메인보드 서브아이템) 컬럼 ID
SUB_SPARE_COL_ID   = "board_relation_mky9ep7a"   # spare list (board_relation)
SUB_QTY_COL_ID     = "text_mky9taeg"         # 사용 수량
SUB_SPARE_DONE_COL_ID = "boolean_mkyqj6k5"         # spare list 생성
SUB_LOG_DONE_COL_ID = "boolean_mkyqzjas"        #자재사용 내역 생성
SUB_LOCATION_COL_ID = "lookup_mky91xzr"         #분출 위치
OWNER_COL_ID = "person"  # 담당자(people) 컬럼 id


# 부모아이템(메인보드) 컬럼 ID
PARENT_PROJECT_CODE_COL_ID = "lookup_mkxev5bg"   # 프로젝트 코드
#PARENT_CUSTOMER_COL_ID     = "text_customer_name"  # 업체명

# Spare List 보드의 "자재 사용 이력" 서브아이템 컬럼 ID
SPARE_SUB_QTY_COL_ID          = "numeric_mkxensdf"   # 사용 수량
SPARE_SUB_PROJECT_CODE_COL_ID = "text_mky9afs1"   # 프로젝트 코드
SPARE_SUB_CUSTOMER_COL_ID     = "text_mky93418"       # 업체명
SPARE_SUB_DATE_COL_ID         = "date0"           # 사용 날짜

# 메인보드 ID
MAIN_BOARD_ID = 5023013838

# ✅ 자재사용 이력 보드 ID (숫자)
USAGE_LOG_BOARD_ID = 5025520203  

# ✅ 자재사용 이력 보드 컬럼 ID들 
#LOG_COL_SPARE_NAME   = "text_spare_name"    # 자재명(텍스트)
LOG_COL_SPARE_ITEMID = "board_relation_mkygrc5f"  # 자재 아이템ID
LOG_COL_QTY          = "numeric_mkygs85b"        # 사용수량(숫자)
LOG_COL_SPARE_NAME   = "text_mkyqjv7k"     # 자재명(텍스트)
LOG_COL_PROJECT      = "text_mkyhq6hz"       # 프로젝트코드
LOG_COL_LOCATION     = "text_mkyqy8j7"      # 분출위치(텍스트)
LOG_COL_DATE         = "date4"          # 날짜(Date)
LOG_COL_PERSON       = "person"  # 자재사용 이력 보드 담당자(people) 컬럼 id


# =========================
# 공용 GraphQL 호출
# =========================
def monday_query(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"query": query}
    if variables:
        payload["variables"] = variables

    resp = requests.post(MONDAY_API_URL, headers=HEADERS, json=payload, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(data["errors"])
    return data["data"]


# =========================
# 1) 서브아이템 + 부모아이템 정보 읽기
# =========================
def get_subitem_and_parent(subitem_id: int) -> Dict[str, Any]:
    """
    하위아이템(서브아이템)의 spare list, 수량,
    부모아이템의 프로젝트 코드, 업체명 가져오기 (최신 API)
    """
    query = """
    query ($id: [ID!]) {
      items(ids: $id) {
        id
        name
        board{ id name }

        # 서브아이템 컬럼들
        column_values(ids: [%(sub_spare)s, %(sub_qty)s, %(sub_loc)s, %(spare_done)s, %(log_done)s, %(owner)s]) {
          id
          type
          text
          value
          ... on BoardRelationValue {
            display_value
            linked_item_ids
          }
          ... on MirrorValue {
            display_value
          }
        }

        # 부모아이템
        parent_item {
          id
          name
          column_values(ids: [%(proj)s]) {
            id
            type
            text
            ... on MirrorValue {
              display_value
            }
          }
        }
      }
    }
    """ % {
        "sub_spare": json.dumps(SUB_SPARE_COL_ID),
        "sub_qty": json.dumps(SUB_QTY_COL_ID),
        "proj": json.dumps(PARENT_PROJECT_CODE_COL_ID),
        "sub_loc" : json.dumps(SUB_LOCATION_COL_ID),
        "spare_done": json.dumps(SUB_SPARE_DONE_COL_ID),
        "log_done": json.dumps(SUB_LOG_DONE_COL_ID),
        "owner": json.dumps(OWNER_COL_ID),
    }

    data = monday_query(query, {"id": [str(subitem_id)]})
    items = data.get("items") or []
    if not items:
        raise RuntimeError(f"Subitem {subitem_id} not found")

    sub = items[0]
    sub_board_id = int(sub["board"]["id"])

    parent = sub.get("parent_item")
    if not parent:
        raise RuntimeError(f"Subitem {subitem_id} has no parent_item")

    # 하위아이템 컬럼값 파싱
    spare_item_id = None
    spare_display_name = ""
    qty = 0

    loc_id = None
    loc = ""
    spare_done = False
    log_done = False

    for cv in sub.get("column_values") or []:
        cid = cv.get("id")

        if cid == SUB_SPARE_COL_ID:
            spare_display_name = (cv.get("display_value") or "").strip()
            linked_ids = cv.get("linked_item_ids") or []
            if linked_ids:
                # 보통 1개 연결이라고 가정
                spare_item_id = int(linked_ids[0])

        elif cid == SUB_QTY_COL_ID:
            txt = (cv.get("text") or "").strip()
            try:
                qty = int(float(txt)) if txt else 0
            except ValueError:
                qty = 0

       
        elif cid == SUB_LOCATION_COL_ID:
            loc = (cv.get("display_value") or "").strip()

        elif cid == SUB_SPARE_DONE_COL_ID:
            v = cv.get("value")
            if v:
                try:
                    spare_done = json.loads(v).get("checked", False)
                except Exception:
                    spare_done = False

        elif cid == SUB_LOG_DONE_COL_ID:
            v = cv.get("value")
            if v:
                try:
                    log_done = json.loads(v).get("checked", False)
                except Exception:
                    log_done = False

        elif cid == OWNER_COL_ID:
            v = cv.get("value")
            if v:
                try:
                    obj = json.loads(v)
                    pats = obj.get("personsAndTeams") or []
                    # 보통 1명만 쓴다고 가정
                    if pats:
                        owner_person_id = int(pats[0]["id"])
                except Exception:
                    owner_person_id = None

    # 업체명 = 부모 아이템 이름
    customer_name = parent.get("name") or ""

    # 프로젝트 코드: text 우선, 없으면 display_value(미러)
    project_code = ""
    for cv in parent["column_values"]:
        if cv["id"] == PARENT_PROJECT_CODE_COL_ID:
            # 1) 미러/룩업이면 display_value로
            project_code = (cv.get("display_value") or "").strip()
            
    return {
        "subitem_id": int(sub["id"]),
        "subitem_name": sub["name"],
        "sub_board_id": sub_board_id,
        "parent_id": int(parent["id"]),
        "parent_name": parent["name"],
        "customer_name": customer_name,
        "project_code": project_code,
        "spare_item_id": spare_item_id,
        "spare_display_name": spare_display_name,
        "quantity": qty,
        "loc":loc,
        "loc_id":loc_id,
        "spare_done": spare_done,
        "log_done": log_done,
        "owner_person_id": owner_person_id,

    }


# =========================
# 2) Spare list 쪽: 자재 아이템 밑에 서브아이템(이력) 생성
# =========================
def create_usage_subitem_on_spare(spare_item_id: int, usage: Dict[str, Any]) -> int:
    """
    Spare List 보드의 해당 자재 아이템(spare_item_id)에
    하위아이템(자재 사용 이력) 1줄 생성.
    usage dict에 필요한 정보:
      - quantity
      - project_code
      - customer_name
    """    
    qty = usage.get("quantity", 0)
    project_code = usage.get("project_code", "")
    customer_name = usage.get("customer_name", "")

    # 서브아이템 이름 포맷(빈 값 제거)
    subitem_name = f"{customer_name}"

    col_vals = {
        SPARE_SUB_QTY_COL_ID: qty,
        SPARE_SUB_PROJECT_CODE_COL_ID: project_code,
        SPARE_SUB_CUSTOMER_COL_ID: customer_name,
        SPARE_SUB_DATE_COL_ID: {"date": date.today().isoformat()},
    }

    mutation = """
    mutation ($parent_item_id: ID!, $item_name: String!, $column_values: JSON!) {
      create_subitem(
        parent_item_id: $parent_item_id,
        item_name: $item_name,
        column_values: $column_values
      ) {
        id
      }
    }
    """

    variables = {
        "parent_item_id": str(spare_item_id),
        "item_name": subitem_name,
        "column_values": json.dumps(col_vals, ensure_ascii=False),
    }

    data = monday_query(mutation, variables)
    new_id = int(data["create_subitem"]["id"])
    print(f"[INFO] Created spare usage subitem {new_id} under spare item {spare_item_id}")
    return new_id


# =========================
# 3) 자재사용 이력 보드: 아이템 생성
# =========================
def create_usage_item_on_log(usage: Dict[str, Any]) -> int:
    qty = usage.get("quantity", 0)
    project_code = usage.get("project_code", "")
    customer_name = usage.get("customer_name", "")
    spare_name = usage.get("spare_display_name") or ""
    loc = usage.get("loc")

    #아이템 이름 : 자유롭게 포맷 가능
    item_name = f"{customer_name}"

    col_vals = {
        LOG_COL_SPARE_NAME: spare_name,
        LOG_COL_QTY: qty,
        LOG_COL_PROJECT: project_code,
        LOG_COL_DATE: {"date": date.today().isoformat()},
        LOG_COL_LOCATION: loc,
    }
    if usage.get("owner_person_id"):
        col_vals[LOG_COL_PERSON] = {
        "personsAndTeams": [{"id": usage["owner_person_id"], "kind": "person"}]
        }

    mutation = """
    mutation ($board_id: ID!, $item_name: String!, $column_values: JSON!) {
      create_item(
        board_id: $board_id,
        item_name: $item_name,
        column_values: $column_values
      ) {
        id
      }
    }
    """

    variables = {
        "board_id": str(USAGE_LOG_BOARD_ID),
        "item_name": item_name,
        "column_values": json.dumps(col_vals, ensure_ascii=False),
    }

    data = monday_query(mutation, variables)
    new_id = int(data["create_item"]["id"])
    print(f"[INFO] Created usage log item {new_id} on board {USAGE_LOG_BOARD_ID}")
    return new_id


# =========================
# 4) 중복 방지: 성공 후 체크박스 ON
# =========================
def set_checkbox(item_id: int, board_id: int, checkbox_col_id: str, checked: bool = True) -> None:
    mutation = """
    mutation ($item_id: ID!, $board_id: ID!, $vals: JSON!) {
      change_multiple_column_values(item_id: $item_id, board_id: $board_id, column_values: $vals) {
        id
      }
    }
    """
    vals = {checkbox_col_id: {"checked": checked}}
    monday_query(mutation, {"item_id": str(item_id),"board_id": str(board_id), "vals": json.dumps(vals)})
    print(f"[INFO] Set {checkbox_col_id}={checked} on subitem {item_id}")



# =========================
# 5) Webhook 엔드포인트
# =========================
@app.route("/monday-webhook", methods=["POST"])
def monday_webhook():
    body = request.get_json(force=True)

    # challenge 응답
    if "challenge" in body:
        return jsonify({"challenge": body["challenge"]})

    event = body.get("event") or {}
    subitem_id = event.get("pulseId") or event.get("itemId")
    if not subitem_id:
        print("[WARN] No subitem id in webhook payload:", body)
        return "", 200

    try:
        info = get_subitem_and_parent(subitem_id)
        subitem_id =int(subitem_id)
        print("[DEBUG] Usage info:", info)

        # ✅ 둘 다 완료면 스킵
        if info.get("spare_done") and info.get("log_done"):
            print(f"[INFO] Subitem {subitem_id} already fully processed. Skip.")
            return "", 200

        # ✅ 공통 필수 체크
        if not info["spare_item_id"]:
            print(f"[WARN] Subitem {subitem_id} has no spare list linked. Skip.")
            return "", 200

        if info["quantity"] <= 0:
            print(f"[WARN] Subitem {subitem_id} has quantity <= 0. Skip.")
            return "", 200

        # ✅ 1) 자재사용 이력 보드 item 생성 (아직 안 했을 때만)
        if not info.get("log_done"):
            usage_log_item_id = create_usage_item_on_log(info)
            set_checkbox(subitem_id, info["sub_board_id"], SUB_LOG_DONE_COL_ID, True)
        else:
            print(f"[INFO] log already done for subitem {subitem_id}. Skip create_item.")

        # ✅ 2) Spare list 하위아이템 생성 (아직 안 했을 때만)
        if not info.get("spare_done"):
            spare_usage_subitem_id = create_usage_subitem_on_spare(info["spare_item_id"], info)
            set_checkbox(subitem_id, info["sub_board_id"], SUB_SPARE_DONE_COL_ID, True)
        else:
            print(f"[INFO] spare already done for subitem {subitem_id}. Skip create_subitem.")

        return "", 200


    except Exception as e:
        print("[ERROR] webhook handling failed:", e)
        # ✅ 에러면 500 → Monday 재시도 유도
        return "internal error", 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=True)
