from payos import PayOS
from payos.types import CreatePaymentLinkRequest, ItemData
import os
import sys
import json
import time
import html
import requests
import queue
import threading
import uuid
import re
from datetime import datetime
import dotenv
from flask import Flask, render_template, request, jsonify, send_from_directory, session
from api import LocketAPI
import database

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)

# Cấu hình Secret Key cho Flask Session
app.secret_key = os.getenv("SECRET_KEY", "locketgold_vip_secret_key_sgu_2026")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

# Khởi tạo Database SQLite (100% Miễn phí, lưu người dùng & VIP)
database.init_db()

# Cấu hình ngân hàng VietQR & giá gói
BANK_ID = os.getenv("BANK_ID", "MB")
ACCOUNT_NO = os.getenv("ACCOUNT_NO", "0333835697")
ACCOUNT_NAME = os.getenv("ACCOUNT_NAME", "TRAN QUANG")
VIP_PRICE = int(os.getenv("VIP_PRICE", "30000"))

# Cấu hình payOS Payment Gateway
PAYOS_CLIENT_ID = os.getenv("PAYOS_CLIENT_ID")
PAYOS_API_KEY = os.getenv("PAYOS_API_KEY")
PAYOS_CHECKSUM_KEY = os.getenv("PAYOS_CHECKSUM_KEY")

payos_client = None
if PAYOS_CLIENT_ID and PAYOS_API_KEY and PAYOS_CHECKSUM_KEY:
    try:
        payos_client = PayOS(PAYOS_CLIENT_ID, PAYOS_API_KEY, PAYOS_CHECKSUM_KEY)
        print("[payOS] Khởi tạo cổng thanh toán payOS thành công.")
    except Exception as e:
        print(f"[payOS Error] Không thể khởi tạo payOS: {e}")


# Initialize API Locket
subscription_ids = [
    "locket_1600_1y",
    "locket_199_1m",
    "locket_199_1m_only",
    "locket_3600_1y",
    "locket_399_1m_only",
]

try:
    api = LocketAPI()
    print("API initialized successfully (No Auth needed).")
except Exception as e:
    print(f"Error initializing API: {e}")
    api = None


# Queue Management System
class QueueManager:
    def __init__(self):
        self.queue = queue.Queue()
        self.lock = threading.Lock()
        self.client_requests = {}
        self.processing_times = []
        self.current_processing = None
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        print("Queue manager initialized and worker thread started")

    def add_to_queue(self, username):
        client_id = str(uuid.uuid4())
        request_data = {
            "username": username,
            "status": "waiting",
            "result": None,
            "error": None,
            "added_at": datetime.now(),
            "started_at": None,
            "completed_at": None,
        }

        with self.lock:
            self.client_requests[client_id] = request_data

        self.queue.put(client_id)
        print(f"Added {username} to queue with client_id: {client_id}")
        return client_id

    def process_direct(self, username):
        """Xử lý trực tiếp (tối ưu cho Vercel Serverless Function & phản hồi tức thì)"""
        client_id = str(uuid.uuid4())
        with self.lock:
            self.client_requests[client_id] = {
                "username": username,
                "status": "processing",
                "result": None,
                "error": None,
                "added_at": datetime.now(),
                "started_at": datetime.now(),
                "completed_at": None,
            }
        self._process_request(client_id)
        with self.lock:
            req = self.client_requests.get(client_id, {})
        return client_id, req

    def get_status(self, client_id):
        with self.lock:
            if client_id not in self.client_requests:
                return None

            request_data = self.client_requests[client_id].copy()
            position = self._get_position(client_id)
            total_queue = self.queue.qsize()
            if self.current_processing and self.current_processing != client_id:
                total_queue += 1

            estimated_time = self._estimate_wait_time(position)

            return {
                "client_id": client_id,
                "status": request_data["status"],
                "position": position,
                "total_queue": total_queue,
                "estimated_time": estimated_time,
                "result": request_data["result"],
                "error": request_data["error"],
            }

    def _get_position(self, client_id):
        if self.current_processing == client_id:
            return 0

        queue_list = list(self.queue.queue)
        if client_id in queue_list:
            return queue_list.index(client_id) + 1

        if client_id in self.client_requests:
            status = self.client_requests[client_id]["status"]
            if status in ["completed", "error"]:
                return 0
        return 0

    def _estimate_wait_time(self, position):
        if position == 0:
            return 0
        avg_time = 5
        if self.processing_times:
            avg_time = sum(self.processing_times[-10:]) / len(self.processing_times[-10:])
        return int(position * avg_time)

    def _process_queue(self):
        print("Queue worker thread started")
        while True:
            try:
                client_id = self.queue.get(timeout=1)

                with self.lock:
                    if client_id not in self.client_requests:
                        continue
                    self.current_processing = client_id
                    self.client_requests[client_id]["status"] = "processing"
                    self.client_requests[client_id]["started_at"] = datetime.now()

                print(f"Processing request for client_id: {client_id}")

                self._process_request(client_id)

                with self.lock:
                    self.current_processing = None
                    if client_id in self.client_requests:
                        self.client_requests[client_id]["completed_at"] = datetime.now()
                        started = self.client_requests[client_id]["started_at"]
                        completed = self.client_requests[client_id]["completed_at"]
                        duration = (completed - started).total_seconds()
                        self.processing_times.append(duration)
                        if len(self.processing_times) > 20:
                            self.processing_times.pop(0)

                self.queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                print(f"Error in queue processing: {e}")
                with self.lock:
                    self.current_processing = None

    def _process_request(self, client_id):
        username = None
        try:
            with self.lock:
                username = self.client_requests[client_id]["username"]

            print(f"Processing restore for: {username}")

            # 1. User lookup trực tiếp (Không Firebase)
            account_info = api.getUserByUsername(username)

            if not account_info or "result" not in account_info:
                raise Exception(f"Không tìm thấy thông tin của @{username} trên Locket.")

            user_data = account_info.get("result", {}).get("data")
            if not user_data:
                raise Exception(f"Không tìm thấy dữ liệu của @{username}.")

            uid_target = user_data.get("uid")
            if not uid_target:
                raise Exception(f"Không tìm thấy UID của @{username}.")

            # 2. Restore purchase trực tiếp từ RevenueCat
            restore_result = api.restorePurchase(uid_target)

            # Check entitlement thật từ RevenueCat
            subscriber = restore_result.get("subscriber", {})
            entitlements = subscriber.get("entitlements", {})
            gold_entitlement = entitlements.get("Gold", {})

            if gold_entitlement:
                real_product_id = gold_entitlement.get("product_identifier", "locket_1600_1y")
                real_expires_date = gold_entitlement.get("expires_date") or "N/A"
                real_purchase_date = gold_entitlement.get("purchase_date") or "N/A"

                send_telegram_notification(
                    username,
                    uid_target,
                    real_product_id,
                    restore_result,
                )

                with self.lock:
                    self.client_requests[client_id]["status"] = "completed"
                    self.client_requests[client_id]["result"] = {
                        "success": True,
                        "msg": f"Nâng cấp Gold cho @{username} thành công!",
                        "product": real_product_id,
                        "uid": uid_target,
                        "expires_date": real_expires_date,
                        "purchase_date": real_purchase_date,
                    }
            else:
                raise Exception(f"Kích hoạt Gold thất bại. Không tìm thấy gói Gold cho @{username}.")

        except Exception as e:
            err_msg = str(e)
            print(f"Error processing request for {client_id}: {err_msg}")
            send_telegram_error(username, err_msg)
            with self.lock:
                self.client_requests[client_id]["status"] = "error"
                self.client_requests[client_id]["error"] = err_msg


queue_manager = QueueManager()


# ==============================================================================
# TELEGRAM NOTIFICATIONS
# ==============================================================================

def send_telegram_notification(username, uid, product_id, raw_json):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"

    if not bot_token or not chat_id:
        return

    try:
        gold_info = raw_json.get("subscriber", {}).get("entitlements", {}).get("Gold", {})
        expires_date = gold_info.get("expires_date") or "N/A"
        product = gold_info.get("product_identifier", product_id or "locket_1600_1y")

        safe_user = html.escape(str(username or "unknown"))
        safe_uid = html.escape(str(uid or "unknown"))
        safe_product = html.escape(str(product))
        safe_expires = html.escape(str(expires_date))
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        subscription_info = json.dumps(gold_info, indent=2, ensure_ascii=False)
        safe_sub_info = html.escape(subscription_info)

        message = (
            f"✅ <b>Locket Gold Unlocked Thành Công!</b>\n\n"
            f"👤 <b>Username:</b> @{safe_user}\n"
            f"🆔 <b>UID:</b> <code>{safe_uid}</code>\n"
            f"📦 <b>Gói:</b> <code>{safe_product}</code>\n"
            f"⏳ <b>Hạn dùng:</b> <code>{safe_expires}</code>\n"
            f"⏰ <b>Thời gian:</b> {now_str}\n\n"
            f"<b>Chi tiết đăng ký:</b>\n<pre>{safe_sub_info}</pre>"
        )

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[Telegram] Failed to send notification: {e}")


def send_telegram_error(username, error_msg):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"

    if not bot_token or not chat_id:
        return

    try:
        safe_user = html.escape(str(username or "unknown"))
        safe_err = html.escape(str(error_msg or "Lỗi không xác định"))
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        message = (
            f"❌ <b>Locket Gold Nâng Cấp Thất Bại</b>\n\n"
            f"👤 <b>Username:</b> @{safe_user}\n"
            f"⚠️ <b>Lỗi:</b> <code>{safe_err}</code>\n"
            f"⏰ <b>Thời gian:</b> {now_str}"
        )

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[Telegram] Failed to send error notification: {e}")


def send_telegram_payment_alert(user, order):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"

    if not bot_token or not chat_id:
        return

    try:
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        safe_user = html.escape(str(user.get("username", "Khách hàng")))
        safe_order = html.escape(str(order.get("order_code", "LK")))
        amount_fmt = f"{order.get('amount', 30000):,}đ"

        message = (
            f"🎉 <b>NHẬN THANH TOÁN 30K - TỰ ĐỘNG NÂNG CẤP VIP!</b>\n\n"
            f"👤 <b>Tài khoản web:</b> @{safe_user}\n"
            f"💵 <b>Số tiền nạp:</b> <code>{amount_fmt}</code>\n"
            f"🧾 <b>Mã giao dịch:</b> <code>{safe_order}</code>\n"
            f"💎 <b>Gói:</b> VIP ACCOUNT (Bảo hành & dùng tối thiểu 6 tháng)\n"
            f"⏰ <b>Thời gian:</b> {now_str}"
        )

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"[Telegram] Failed to send payment alert: {e}")


# ==============================================================================
# AUTHENTICATION ROUTES (ĐĂNG KÝ / ĐĂNG NHẬP / THÔNG TIN TÀI KHOẢN)
# ==============================================================================

@app.route("/api/auth/register", methods=["POST"])
def auth_register():
    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")
    email = data.get("email", "").strip()

    if not username:
        return jsonify({"success": False, "msg": "Vui lòng nhập tên đăng nhập"}), 400
    if not password:
        return jsonify({"success": False, "msg": "Vui lòng nhập mật khẩu"}), 400

    try:
        user = database.register_user(username, password, email)
        session["user_id"] = user["id"]
        return jsonify({
            "success": True,
            "msg": "Đăng ký tài khoản thành công!",
            "user": user
        })
    except ValueError as ve:
        return jsonify({"success": False, "msg": str(ve)}), 400
    except Exception as e:
        return jsonify({"success": False, "msg": f"Lỗi hệ thống: {str(e)}"}), 500


@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not username or not password:
        return jsonify({"success": False, "msg": "Vui lòng nhập đầy đủ tên đăng nhập và mật khẩu"}), 400

    try:
        user = database.authenticate_user(username, password)
        if not user:
            return jsonify({"success": False, "msg": "Tên đăng nhập hoặc mật khẩu không chính xác"}), 401

        session["user_id"] = user["id"]
        # Không trả về hash password ra ngoài
        clean_user = {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "is_vip": user["is_vip"],
            "vip_expires_at": user["vip_expires_at"],
            "created_at": user["created_at"],
        }
        return jsonify({
            "success": True,
            "msg": "Đăng nhập thành công!",
            "user": clean_user
        })
    except Exception as e:
        return jsonify({"success": False, "msg": f"Lỗi: {str(e)}"}), 500


@app.route("/api/auth/logout", methods=["POST"])
def auth_logout():
    session.pop("user_id", None)
    return jsonify({"success": True, "msg": "Đã đăng xuất thành công"})


@app.route("/api/auth/me", methods=["GET"])
def auth_me():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"logged_in": False})

    user = database.get_user_by_id(user_id)
    if not user:
        session.pop("user_id", None)
        return jsonify({"logged_in": False})

    # Tính số ngày VIP còn lại (Hỗ trợ định dạng ISO Supabase)
    days_left = 99999
    if user.get("is_vip") and user.get("vip_expires_at"):
        try:
            iso_str = str(user["vip_expires_at"]).replace("Z", "+00:00")
            exp_date = datetime.fromisoformat(iso_str).replace(tzinfo=None)
            diff = (exp_date - datetime.now()).days
            days_left = max(0, diff)
        except Exception:
            days_left = 99999

    return jsonify({
        "logged_in": True,
        "user": {
            **user,
            "days_left": days_left
        }
    })


# ==============================================================================
# PAYMENT ROUTES (MÃ QR VIETQR TỰ ĐỘNG THANH TOÁN 30K & WEBHOOK DUYỆT VIP)
# ==============================================================================

@app.route("/api/payment/create-order", methods=["POST"])
def payment_create_order():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"success": False, "msg": "Vui lòng đăng nhập trước khi nạp tiền"}), 401

    user = database.get_user_by_id(user_id)
    if not user:
        return jsonify({"success": False, "msg": "Tài khoản không hợp lệ"}), 401

    # Tạo đơn hàng 30.000 VNĐ
    order = database.create_order(user_id, amount=VIP_PRICE)
    order_code = order["order_code"]
    amount = order["amount"]

    checkout_url = None
    bank_id = BANK_ID
    account_no = ACCOUNT_NO
    account_name = ACCOUNT_NAME
    content = f"LK{order_code}"

    # Tích hợp payOS chính thức
    if payos_client:
        try:
            domain = request.host_url.rstrip("/")
            order_code_int = int(order_code)
            req = CreatePaymentLinkRequest(
                order_code=order_code_int,
                amount=amount,
                description=f"LK{order_code_int}"[:25],
                return_url=f"{domain}/?payment_success={order_code}",
                cancel_url=f"{domain}/?payment_cancel={order_code}",
                items=[ItemData(name="VIP Locket Gold 6 Thang", quantity=1, price=amount)]
            )
            payos_res = payos_client.payment_requests.create(req)
            checkout_url = payos_res.checkout_url
            bank_id = payos_res.bin
            account_no = payos_res.account_number
            account_name = payos_res.account_name
            content = payos_res.description
            qr_url = (
                f"https://img.vietqr.io/image/{bank_id}-{account_no}-compact2.png"
                f"?amount={amount}&addInfo={requests.utils.quote(content)}&accountName={requests.utils.quote(account_name)}"
            )
            print(f"[payOS] Created checkout link: {checkout_url}")
        except Exception as e:
            print(f"[payOS Error] Fallback VietQR: {e}")
            qr_url = (
                f"https://img.vietqr.io/image/{BANK_ID}-{ACCOUNT_NO}-compact2.png"
                f"?amount={amount}&addInfo={content}&accountName={requests.utils.quote(ACCOUNT_NAME)}"
            )
    else:
        qr_url = (
            f"https://img.vietqr.io/image/{BANK_ID}-{ACCOUNT_NO}-compact2.png"
            f"?amount={amount}&addInfo={content}&accountName={requests.utils.quote(ACCOUNT_NAME)}"
        )

    return jsonify({
        "success": True,
        "order": order,
        "qr_url": qr_url,
        "checkout_url": checkout_url,
        "bank_info": {
            "bank_id": bank_id,
            "account_no": account_no,
            "account_name": account_name,
            "amount": amount,
            "content": content,
        }
    })


@app.route("/api/payment/check-status", methods=["GET"])
def payment_check_status():
    order_code = request.args.get("order_code")
    if not order_code:
        return jsonify({"success": False, "msg": "Thiếu mã đơn hàng"}), 400

    order = database.get_order_by_code(order_code)
    if not order:
        return jsonify({"success": False, "msg": "Không tìm thấy đơn hàng"}), 404

    user = database.get_user_by_id(order["user_id"])
    is_vip = bool(user and user.get("is_vip"))

    return jsonify({
        "success": True,
        "order_code": order["order_code"],
        "status": order["status"],
        "is_paid": order["status"] == "completed",
        "is_vip": is_vip,
        "paid_at": order["paid_at"]
    })


@app.route("/api/payment/webhook", methods=["POST"])
def payment_webhook():
    """
    Webhook nhận thông báo biến động số dư từ payOS chính thức.
    BẮT BUỘC xác thực chữ ký số HMAC-SHA256 để chống giả mạo / bypass 100%!
    """
    data = request.json or {}
    print(f"[Payment Webhook] Received payload: {data}")

    # Xử lý trường hợp test ping xác nhận webhook URL từ payOS Dashboard
    if data.get("desc") == "Webhook confirm" or (data.get("data") and data["data"].get("description") == "Webhook confirm"):
        print("[payOS Webhook] Webhook URL confirmed by payOS")
        return jsonify({"success": True, "msg": "Webhook confirmed"}), 200

    if not payos_client:
        return jsonify({"success": False, "msg": "payOS chưa được khởi tạo"}), 500

    # 1. BẮT BUỘC XÁC THỰC BẢO MẬT CHỮ KÝ HMAC-SHA256 TỪ PAYOS
    try:
        webhook_data = payos_client.webhooks.verify(data)
        print(f"[payOS Webhook Verified]: {webhook_data}")
        order_code = str(webhook_data.order_code)
        amount = webhook_data.amount
    except Exception as e:
        print(f"[Security Alert] Giả mạo Webhook hoặc sai chữ ký: {e}")
        return jsonify({"success": False, "msg": "Chữ ký bảo mật không hợp lệ. Truy cập bị từ chối!"}), 403

    # 2. Tìm đơn hàng trong cơ sở dữ liệu Supabase
    order = database.get_order_by_code(order_code)
    if not order:
        print(f"[Payment Webhook] Không tìm thấy đơn {order_code} trong database")
        return jsonify({"success": True, "msg": f"Bỏ qua đơn {order_code}"}), 200

    # 3. Kiểm tra số tiền
    if int(amount) < int(order["amount"]):
        print(f"[Payment Webhook] Số tiền {amount} ít hơn giá trị đơn {order['amount']}")
        return jsonify({"success": False, "msg": "Số tiền không đủ"}), 200

    # 4. Hoàn tất đơn hàng và tự động nâng cấp VIP VĨNH VIỄN (Lifetime)
    updated_order, err = database.complete_order(order["order_code"], payment_info=json.dumps(data))
    if err and err != "Đơn hàng đã được thanh toán trước đó":
        return jsonify({"success": False, "msg": err}), 400

    user = database.get_user_by_id(order["user_id"])
    if user:
        send_telegram_payment_alert(user, order)

    print(f"[Payment Webhook] Đã kích hoạt VIP Vĩnh Viễn cho User #{order['user_id']} đơn {order_code}")
    return jsonify({
        "success": True,
        "msg": f"Duyệt đơn hàng {order_code} và kích hoạt VIP Vĩnh Viễn thành công!"
    })



@app.route("/download-config")
def download_config():
    return send_from_directory(
        "static", "locket.mobileconfig", mimetype="application/x-apple-aspen-config"
    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/get-user-info", methods=["POST"])
def get_user_info():
    if not api:
        return jsonify({"success": False, "msg": "API chưa được khởi tạo. Vui lòng kiểm tra server."}), 500

    data = request.json or {}
    username = data.get("username")

    if not username:
        return jsonify({"success": False, "msg": "Vui lòng nhập username"}), 400

    try:
        print(f"Looking up user: {username}")
        account_info = api.getUserByUsername(username)

        if not account_info or "result" not in account_info:
            return jsonify({"success": False, "msg": f"Không tìm thấy người dùng '@{username}'."}), 404

        user_data = account_info.get("result", {}).get("data")
        if not user_data or not user_data.get("uid"):
            return jsonify({"success": False, "msg": f"Không tìm thấy dữ liệu hoặc UID của '@{username}'."}), 404

        user_info = {
            "uid": user_data.get("uid"),
            "username": user_data.get("username"),
            "first_name": user_data.get("first_name", ""),
            "last_name": user_data.get("last_name", ""),
            "profile_picture_url": user_data.get("profile_picture_url", ""),
        }

        return jsonify({"success": True, "data": user_info})

    except Exception as e:
        print(f"Error in get user info: {e}")
        return jsonify({"success": False, "msg": str(e)}), 400


@app.route("/api/restore", methods=["POST"])
def restore_purchase():
    if not api:
        return jsonify({"success": False, "msg": "API chưa được khởi tạo. Vui lòng kiểm tra server."}), 500

    # 1. KIỂM TRA ĐĂNG NHẬP
    current_user_id = session.get("user_id")
    if not current_user_id:
        return jsonify({
            "success": False,
            "require_login": True,
            "msg": "Vui lòng Đăng nhập hoặc Tạo tài khoản để sử dụng tính năng Nâng cấp Locket Gold!"
        }), 401

    user = database.get_user_by_id(current_user_id)
    if not user:
        session.pop("user_id", None)
        return jsonify({"success": False, "require_login": True, "msg": "Tài khoản không tồn tại. Vui lòng đăng nhập lại."}), 401

    # 2. KIỂM TRA QUYỀN VIP ACCOUNT (BẢO MẬT CHẶT CHẼ TRÊN SERVER)
    if not user.get("is_vip"):
        return jsonify({
            "success": False,
            "require_vip": True,
            "msg": "Tài khoản của bạn chưa nâng cấp VIP. Vui lòng thanh toán 30.000đ (1 lần duy nhất) để mở khóa tính năng kích hoạt vĩnh viễn trên web!"
        }), 403

    # Chống spam / rate-limit per user session (tối thiểu 4 giây giữa các lượt bấm)
    now_ts = time.time()
    last_act = session.get("last_activation_ts", 0)
    if now_ts - last_act < 4:
        return jsonify({"success": False, "msg": "Thao tác quá nhanh! Vui lòng chờ 4 giây trước khi kích hoạt lại."}), 429
    session["last_activation_ts"] = now_ts

    data = request.json or {}
    raw_username = str(data.get("username", "")).strip()

    # Sanitize username chặt chẽ: chỉ cho phép ký tự hợp lệ, độ dài từ 2 đến 60 ký tự
    import re
    username = re.sub(r"[^a-zA-Z0-9._-]", "", raw_username)

    if not username or len(username) < 2 or len(username) > 60:
        return jsonify({"success": False, "msg": "Tên người dùng Locket không hợp lệ!"}), 400

    try:
        # Xử lý trực tiếp nâng cấp Locket Gold
        client_id, req_data = queue_manager.process_direct(username)

        if req_data.get("status") == "completed":
            result = req_data.get("result", {})
            real_product = result.get("product", "locket_1600_1y")
            real_expires = result.get("expires_date", "2027")
            locket_uid = result.get("uid", "")

            # Lưu vào lịch sử kích hoạt của User trong Database SQLite
            database.record_activation(
                user_id=user["id"],
                locket_username=username,
                locket_uid=locket_uid,
                product_id=real_product,
                expires_date=real_expires,
                status="completed"
            )

            return jsonify({
                "success": True,
                "status": "completed",
                "client_id": client_id,
                "result": result,
                "warranty_info": "Cam kết tài khoản Locket sau khi kích hoạt dùng ổn định ít nhất 3 tháng. Tài khoản VIP được kích hoạt lại vĩnh viễn trên web.",
                "position": 0,
                "total_queue": 0,
                "estimated_time": 0
            })
        else:
            err = req_data.get("error") or "Không thể kích hoạt Gold. Vui lòng thử lại sau."
            return jsonify({"success": False, "msg": err}), 400

    except Exception as e:
        print(f"Error restoring: {e}")
        return jsonify({"success": False, "msg": f"Lỗi: {str(e)}"}), 500


@app.route("/api/user/activations", methods=["GET"])
def user_activations():
    user_id = session.get("user_id")
    if not user_id:
        return jsonify({"success": False, "msg": "Chưa đăng nhập"}), 401

    history = database.get_user_activations(user_id)
    return jsonify({"success": True, "history": history})


@app.route("/api/public-stats", methods=["GET"])
def public_stats():
    stats = database.get_stats()
    return jsonify({"success": True, "stats": stats})


@app.route("/api/queue/status", methods=["POST"])
def queue_status():
    data = request.json or {}
    client_id = data.get("client_id")

    if not client_id:
        return jsonify({"success": False, "msg": "client_id is required"}), 400

    status = queue_manager.get_status(client_id)

    if status is None:
        return jsonify({"success": False, "msg": "Client ID not found"}), 404

    return jsonify({"success": True, **status})


@app.route("/api/test-telegram", methods=["GET"])
def test_telegram_route():
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"
    if not bot_token or not chat_id:
        return jsonify({"success": False, "msg": "TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa cấu hình"}), 400
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        res = requests.post(url, json={
            "chat_id": chat_id,
            "text": f"🔔 <b>Kiểm tra kết nối Bot Telegram thành công!</b>\n⏰ <b>Thời gian:</b> {now_str}\n🌐 <b>Server:</b> LocketGold đang hoạt động bình thường.",
            "parse_mode": "HTML"
        }, timeout=10)
        if res.status_code == 200:
            return jsonify({"success": True, "msg": "Đã gửi tin nhắn test đến Telegram admin thành công!"})
        else:
            return jsonify({"success": False, "msg": f"Telegram API lỗi ({res.status_code}): {res.text}"}), 502
    except Exception as e:
        return jsonify({"success": False, "msg": f"Lỗi kết nối Telegram: {str(e)}"}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"Server is running on http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)