# database.py - Kết nối Cloud Database Supabase (100% Free, hoạt động tốt trên Vercel Serverless)
import os
from datetime import datetime, timedelta
from werkzeug.security import generate_password_hash, check_password_hash
import dotenv
from supabase import create_client, Client

# Load .env
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://eenssdiqjvjrfgcdvweo.supabase.co")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

supabase: Client = None

def get_supabase() -> Client:
    global supabase
    if supabase is None:
        url = os.getenv("SUPABASE_URL", "https://eenssdiqjvjrfgcdvweo.supabase.co")
        key = os.getenv("SUPABASE_KEY", "")
        if not key:
            print("[Database Warning] Chưa có SUPABASE_KEY trong file .env!")
            return None
        try:
            supabase = create_client(url, key)
        except Exception as e:
            print(f"[Database Error] Lỗi kết nối Supabase: {e}")
            return None
    return supabase

def init_db():
    client = get_supabase()
    if client:
        print(f"[Database] Đã kết nối Supabase Cloud: {SUPABASE_URL}")
    else:
        print("[Database] Đang chờ cấu hình SUPABASE_KEY trong file .env...")

def register_user(username, password, email=None):
    client = get_supabase()
    if not client:
        raise Exception("Database chưa được kết nối. Vui lòng cấu hình SUPABASE_KEY trong .env")

    username = username.strip().lower()
    if not username or len(username) < 3:
        raise ValueError("Tên đăng nhập phải có ít nhất 3 ký tự")
    if not password or len(password) < 6:
        raise ValueError("Mật khẩu phải có ít nhất 6 ký tự")

    # Kiểm tra username đã tồn tại chưa
    res = client.table("users").select("id").eq("username", username).execute()
    if res.data:
        raise ValueError("Tên đăng nhập này đã được sử dụng")

    now = datetime.now().isoformat()
    password_hash = generate_password_hash(password)

    insert_data = {
        "username": username,
        "email": email or "",
        "password_hash": password_hash,
        "is_vip": 0,
        "created_at": now,
        "last_login": now
    }
    
    insert_res = client.table("users").insert(insert_data).execute()
    if not insert_res.data:
        raise Exception("Không thể tạo tài khoản")
        
    return insert_res.data[0]

def authenticate_user(username, password):
    client = get_supabase()
    if not client:
        return None

    username = username.strip().lower()
    res = client.table("users").select("*").or_(f"username.eq.{username},email.eq.{username}").execute()
    
    if not res.data:
        return None
        
    user = res.data[0]
    if not check_password_hash(user["password_hash"], password):
        return None

    # Cập nhật thời gian đăng nhập
    now = datetime.now().isoformat()
    client.table("users").update({"last_login": now}).eq("id", user["id"]).execute()
    return user

def get_user_by_id(user_id):
    client = get_supabase()
    if not client:
        return None

    res = client.table("users").select("id, username, email, is_vip, vip_expires_at, created_at, last_login").eq("id", user_id).execute()
    if not res.data:
        return None
    user = res.data[0]
    
    # Kiểm tra VIP hết hạn
    if user.get("is_vip") and user.get("vip_expires_at"):
        try:
            exp_date = datetime.fromisoformat(str(user["vip_expires_at"]).replace("Z", "+00:00"))
            # Chuẩn hoá timezone naive
            exp_date = exp_date.replace(tzinfo=None)
            if datetime.now() > exp_date:
                set_user_vip(user_id, False)
                user["is_vip"] = 0
        except Exception:
            pass
            
    return user

def set_user_vip(user_id, is_vip=True, days=180):
    client = get_supabase()
    if not client:
        return None

    if is_vip:
        expires_at = (datetime.now() + timedelta(days=days)).isoformat()
        client.table("users").update({"is_vip": 1, "vip_expires_at": expires_at}).eq("id", user_id).execute()
    else:
        client.table("users").update({"is_vip": 0}).eq("id", user_id).execute()
    return get_user_by_id(user_id)

def create_order(user_id, amount=30000):
    client = get_supabase()
    if not client:
        return None

    now = datetime.now()
    # Kiểm tra đơn pending gần đây
    res = client.table("orders").select("*").eq("user_id", user_id).eq("status", "pending").order("id", desc=True).limit(1).execute()
    if res.data:
        existing_order = res.data[0]
        # Nếu mã đơn là số hợp lệ thì tái sử dụng
        if str(existing_order.get("order_code", "")).isdigit():
            return existing_order

    import time
    # Sinh mã đơn dạng số nguyên duy nhất (tương thích payOS orderCode)
    order_code_int = int(time.time() * 10) % 9000000000 + (int(user_id) % 100)
    order_code = str(order_code_int)

    order_data = {
        "user_id": user_id,
        "order_code": order_code,
        "amount": amount,
        "status": "pending",
        "created_at": now.isoformat()
    }
    insert_res = client.table("orders").insert(order_data).execute()
    return insert_res.data[0] if insert_res.data else None

def get_order_by_code(order_code):
    client = get_supabase()
    if not client:
        return None

    code_raw = str(order_code).strip()
    code_clean = code_raw.upper().replace("LK", "")

    # Tìm theo mã gốc hoặc mã đã lọc bỏ tiền tố LK
    res = client.table("orders").select("*").or_(f"order_code.eq.{code_raw},order_code.eq.{code_clean}").execute()
    return res.data[0] if res.data else None

def complete_order(order_code, payment_info=None):
    client = get_supabase()
    if not client:
        return None, "Database chưa sẵn sàng"

    order = get_order_by_code(order_code)
    if not order:
        return None, "Không tìm thấy đơn hàng"
        
    if order["status"] == "completed":
        return order, "Đơn hàng đã được thanh toán trước đó"
        
    paid_at = datetime.now().isoformat()
    info_str = str(payment_info) if payment_info else ""
    
    client.table("orders").update({
        "status": "completed",
        "paid_at": paid_at,
        "payment_info": info_str
    }).eq("id", order["id"]).execute()
    
    # Kích hoạt VIP 6 tháng (180 ngày)
    set_user_vip(order["user_id"], is_vip=True, days=180)
    
    updated_order = get_order_by_code(order_code)
    return updated_order, None

def record_activation(user_id, locket_username, locket_uid, product_id, expires_date, status='completed'):
    client = get_supabase()
    if not client:
        return

    now = datetime.now().isoformat()
    client.table("activations").insert({
        "user_id": user_id,
        "locket_username": locket_username,
        "locket_uid": locket_uid or "",
        "product_id": product_id or "",
        "expires_date": expires_date or "",
        "status": status,
        "created_at": now
    }).execute()

def get_user_activations(user_id, limit=20):
    client = get_supabase()
    if not client:
        return []

    res = client.table("activations").select("*").eq("user_id", user_id).order("id", desc=True).limit(limit).execute()
    return res.data if res.data else []

def get_stats():
    client = get_supabase()
    if not client:
        return {"total_users": 0, "vip_users": 0, "total_orders": 0, "total_activations": 0}

    users_res = client.table("users").select("id", count="exact").execute()
    vip_res = client.table("users").select("id", count="exact").eq("is_vip", 1).execute()
    orders_res = client.table("orders").select("id", count="exact").eq("status", "completed").execute()
    activations_res = client.table("activations").select("id", count="exact").eq("status", "completed").execute()
    
    return {
        "total_users": users_res.count or 0,
        "vip_users": vip_res.count or 0,
        "total_orders": orders_res.count or 0,
        "total_activations": activations_res.count or 0
    }
