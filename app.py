import os
import sys
import json
import time
import html
import requests
import queue
import threading
import uuid
from datetime import datetime
import dotenv
from flask import Flask, render_template, request, jsonify, send_from_directory
from api import LocketAPI

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(BASE_DIR, ".env"))

app = Flask(
    __name__,
    template_folder=os.path.join(BASE_DIR, "templates"),
    static_folder=os.path.join(BASE_DIR, "static"),
)

# Initialize API (Không cần Auth nữa)
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

            # Check entitlement thật từ RevenueCat (dữ liệu thật, không ghi đè fake)
            subscriber = restore_result.get("subscriber", {})
            entitlements = subscriber.get("entitlements", {})
            gold_entitlement = entitlements.get("Gold", {})

            # Nếu mua thành công
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


def send_telegram_notification(username, uid, product_id, raw_json):
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"

    if not bot_token or not chat_id:
        print("[Telegram] Skip: TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa được đặt trong .env")
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

        res = requests.post(url, json=payload, timeout=10)
        if res.status_code == 200:
            print(f"[Telegram] Sent success notification for @{username}")
        else:
            print(f"[Telegram] HTML parse error ({res.status_code}): {res.text}. Retrying with plain text...")
            plain_msg = (
                f"✅ Locket Gold Unlocked Thành Công!\n\n"
                f"User: @{username} ({uid})\n"
                f"Gói: {product}\n"
                f"Hạn: {expires_date}\n"
                f"Thời gian: {now_str}"
            )
            requests.post(url, json={"chat_id": chat_id, "text": plain_msg}, timeout=10)
    except Exception as e:
        print(f"[Telegram] Failed to send Telegram notification: {e}")


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

        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            requests.post(url, json={
                "chat_id": chat_id,
                "text": f"❌ Locket Gold Nâng Cấp Thất Bại cho @{username}: {error_msg}"
            }, timeout=10)
    except Exception as e:
        print(f"[Telegram] Failed to send error notification: {e}")


@app.route("/api/test-telegram", methods=["GET"])
def test_telegram_route():
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or "8858813567:AAH5KAfVQDXnf7w_bDvQsq5oQnHVggRqT2w"
    chat_id = os.getenv("TELEGRAM_CHAT_ID") or "7530810928"
    if not bot_token or not chat_id:
        return jsonify({"success": False, "msg": "TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa được cấu hình trong .env"}), 400
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        res = requests.post(url, json={
            "chat_id": chat_id,
            "text": f"🔔 <b>Kiểm tra kết nối Bot Telegram thành công!</b>\n⏰ <b>Thời gian:</b> {now_str}\n🌐 <b>Server:</b> LocketGold đang hoạt động bình thường.",
            "parse_mode": "HTML"
        }, timeout=10)
        if res.status_code == 200:
            return jsonify({"success": True, "msg": "Đã gửi tin nhắn test đến Telegram admin thành công! Kiểm tra tin nhắn Telegram của bạn."})
        else:
            return jsonify({"success": False, "msg": f"Telegram API lỗi ({res.status_code}): {res.text}"}), 502
    except Exception as e:
        return jsonify({"success": False, "msg": f"Lỗi kết nối Telegram: {str(e)}"}), 500


@app.route("/api/restore", methods=["POST"])
def restore_purchase():
    if not api:
        return jsonify({"success": False, "msg": "API chưa được khởi tạo. Vui lòng kiểm tra server."}), 500

    data = request.json or {}
    username = data.get("username")

    if not username:
        return jsonify({"success": False, "msg": "Vui lòng nhập username"}), 400

    try:
        # Xử lý trực tiếp (tối ưu cho Vercel serverless & local)
        client_id, req_data = queue_manager.process_direct(username)

        if req_data.get("status") == "completed":
            return jsonify({
                "success": True,
                "status": "completed",
                "client_id": client_id,
                "result": req_data.get("result"),
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


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    print(f"Server is running on http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=True, use_reloader=False)