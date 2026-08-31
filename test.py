import asyncio
import re
import json
from urllib.parse import urlparse, parse_qs

import aiohttp
import requests


# =========================================================
# REVENUECAT HEADERS
# =========================================================

HEADERS = {
    'Host': 'api.revenuecat.com',
    'Authorization': 'Bearer appl_JngFETzdodyLmCREOlwTUtXdQik',
    'Content-Type': 'application/json',
    'Accept': '*/*',
    'X-Platform': 'iOS',
    'X-Platform-Version': 'Version 26.2 (Build 23C55)',
    'X-Platform-Device': 'iPhone15,3',
    'X-Platform-Flavor': 'native',
    'X-Version': '5.41.0',
    'X-Client-Version': '2.32.2',
    'X-Client-Bundle-ID': 'com.locket.Locket',
    'X-Client-Build-Version': '3',
    'X-StoreKit2-Enabled': 'true',
    'X-StoreKit-Version': '2',
    'X-Observer-Mode-Enabled': 'false',
    'X-Is-Sandbox': 'true',
    'X-Storefront': 'VNM',
    'X-Apple-Device-Identifier': '39A73C25-1E05-4350-ADA7-5CD3FE1079E8',
    'X-Preferred-Locales': 'vi_KR,ko_KR,en_KR',
    'X-Nonce': 'w0Mlb6+AmV4WYuVv',
    'X-Is-Backgrounded': 'false',
    'X-Retry-Count': '0',
    'X-Is-Debug-Build': 'false',
    'User-Agent': 'Locket/3 CFNetwork/3860.300.31 Darwin/25.2.0',
    'Accept-Language': 'vi-VN,vi;q=0.9',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'X-RevenueCat-ETag': ''
}


# =========================================================
# USERNAME -> UID
# =========================================================

def get_uid(username):
    username = username.strip().lstrip("@")

    if not username:
        print("[-] Username không được để trống")
        return None

    url = f"https://locket.cam/{username}"

    print("\n========== USER LOOKUP ==========")
    print(f"[>] Username    : {username}")
    print(f"[>] Request URL : {url}")

    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            },
            timeout=15,
        )

        print(f"[<] Status      : {response.status_code}")
        print(f"[<] Final URL   : {response.url}")
        print(f"[<] Body length : {len(response.text)}")

        if response.status_code != 200:
            print("[-] Locket trả về HTTP error")
            return None

        html = response.text

        # Tìm Dynamic Link
        match = re.search(
            r'window\.location\.href\s*=\s*"([^"]+)"',
            html
        )

        if not match:
            print("[-] Không tìm thấy Dynamic Link")
            return None

        dynamic_link = match.group(1)

        print(f"[+] Dynamic Link: {dynamic_link}")

        # Lấy link thật trong ?link=
        parsed = urlparse(dynamic_link)

        link = parse_qs(
            parsed.query
        ).get("link", [None])[0]

        if not link:
            print("[-] Không tìm thấy invite link")
            return None

        print(f"[+] Invite Link : {link}")

        # Lấy UID từ /invites/<uid>
        invite_path = urlparse(link).path

        uid_match = re.search(
            r"/invites/([^/?]+)",
            invite_path
        )

        if not uid_match:
            print("[-] Không tìm thấy UID trong invite link")
            return None

        uid = uid_match.group(1)

        print(f"[+] UID FOUND   : {uid}")
        print("=================================\n")

        return uid

    except requests.RequestException as e:
        print(f"[!] Lookup error: {e}")
        return None


# =========================================================
# CHECK REVENUECAT
# =========================================================

async def check_status(uid):
    url = f"https://api.revenuecat.com/v1/subscribers/{uid}"

    print("\n========== REVENUECAT ==========")
    print(f"[>] UID         : {uid}")
    print(f"[>] Request URL : {url}")

    try:
        timeout = aiohttp.ClientTimeout(total=10)

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers=HEADERS,
                timeout=timeout
            ) as res:

                print(f"[<] Status      : {res.status}")

                text = await res.text()

                print("\n---------- RESPONSE ----------")
                print(text[:5000])
                print("------------------------------")

                if not 200 <= res.status < 300:
                    print("[-] RevenueCat request failed")

                    return {
                        "success": False,
                        "gold": None,
                        "status_code": res.status
                    }

                try:
                    data = json.loads(text)
                except json.JSONDecodeError as e:
                    print(f"[-] Response không phải JSON: {e}")

                    return {
                        "success": False,
                        "gold": None,
                        "status_code": res.status
                    }

                subscriber = data.get("subscriber", {})

                # LẤY NGUYÊN OBJECT GOLD
                gold = (
                    subscriber
                    .get("entitlements", {})
                    .get("Gold")
                )

                if gold is None:
                    print("[-] Gold entitlement: NOT FOUND")
                else:
                    print("\n[+] Gold entitlement: FOUND")
                    print(
                        json.dumps(
                            gold,
                            indent=4,
                            ensure_ascii=False
                        )
                    )

                return {
                    "success": True,
                    "gold": gold
                }

    except asyncio.TimeoutError:
        print("[!] RevenueCat request timeout")
        return None

    except aiohttp.ClientError as e:
        print(f"[!] RevenueCat error: {e}")
        return None

    except Exception as e:
        print(f"[!] Unexpected error: {e}")
        return None


# =========================================================
# MAIN
# =========================================================

async def main():

    print("""
========================================
          LOCKET USER CHECKER
========================================
""")

    username = input("Nhập username: ").strip()

    if not username:
        print("[-] Username không hợp lệ")
        return

    # 1. Username -> UID
    uid = get_uid(username)

    if not uid:
        print("\n[-] Không lấy được UID")
        return

    # 2. UID -> RevenueCat
    result = await check_status(uid)

    # 3. Result
    print("\n========== RESULT ==========")
    print(f"Username : {username}")
    print(f"UID      : {uid}")

    if result is None:
        print("Status   : ERROR")

    elif not result.get("success"):
        print("Status   : REQUEST ERROR")

    elif result.get("gold") is None:
        print("Status   : NO GOLD")

    else:
        print("Status   : GOLD FOUND")
        print("\nGold entitlement:")

        print(
            json.dumps(
                result["gold"],
                indent=4,
                ensure_ascii=False
            )
        )

    print("============================")


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    asyncio.run(main())