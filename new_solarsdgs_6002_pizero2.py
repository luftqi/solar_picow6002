import BlynkLib
from paho.mqtt import client as mqtt_client
import time
import datetime
import json
import logging
import sqlite3
import requests
import os
from os import system

# --- 1NCE 與 Blynk 設定 ---
iot = '6002'
blynk_token = 'hOALK-HCU1uYRuZ7daGMci5adH1PyqZY'
nceid = '8988228066614762250'
nceid_token = "Basic Z3JheUBzb2xhcnNkZ3MuY29tOjk2NzYyMzY0"

# --- 設定 ---
# [OTA] 新增版本號和 GitHub Repo 的設定
CURRENT_VERSION = 2.0 # 您可以自行定義當前版本
# 請將下面的連結換成您自己 GitHub Repo 中的 "Raw" 連結
VERSION_URL = "https://raw.githubusercontent.com/luftqi/solar_picow6002/main/pizero_version.txt"
SCRIPT_URL = "https://raw.githubusercontent.com/luftqi/solar_picow6002/main/MQTT_SQLit_Blynk.py"

# --- 全域變數 ---
factor_a, factor_p = 1.0, 1.0
pizero2_on, pizero2_off = "30", "50"
message, message_check = [], []

# --- MQTT 設定 ---
broker = '127.0.0.1'
port = 1883
topic_sub = "pg_pa_pp"
topic_pub = "pizero2onoff"
topic_ack = "pico/ack"
client_id = f'pizero{iot}_0'
username = f'solarsdgs{iot}'
password = '82767419'

# --- [OTA] 新增 OTA 更新檢查函數 ---
def check_for_updates():
    """
    檢查 GitHub 上是否有新版本，如果有，則下載、覆蓋並重啟。
    """
    print("[OTA] 正在檢查更新...")
    try:
        # 1. 獲取遠端版本號
        response = requests.get(VERSION_URL, timeout=10)
        if response.status_code != 200:
            print(f"[OTA] 錯誤：無法獲取版本文件，狀態碼: {response.status_code}")
            blynk.virtual_write(11, f"無法獲取版本文件: {response.status_code}") # 將錯誤訊息回傳到Blynk
            return

        remote_version = float(response.text.strip())
        print(f"[OTA] 當前版本: {CURRENT_VERSION}, 遠端版本: {remote_version}")

        # 2. 比較版本號
        if remote_version > CURRENT_VERSION:
            print(f"[OTA] 發現新版本 {remote_version}，準備下載...")
            blynk.virtual_write(11, f"發現新版本 {remote_version}...")

            # 3. 下載新版程式碼
            script_response = requests.get(SCRIPT_URL, timeout=30)
            if script_response.status_code != 200:
                print(f"[OTA] 錯誤：無法下載新版程式碼，狀態碼: {script_response.status_code}")
                blynk.virtual_write(11, f"下載失敗: {script_response.status_code}")
                return

            new_script_content = script_response.text

            # 4. 覆蓋當前運行的腳本
            script_path = os.path.abspath(__file__)
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(new_script_content)
            
            print(f"[OTA] 程式碼已成功更新至版本 {remote_version}。")
            print("[OTA] 準備在 3 秒後重啟程式...")
            blynk.virtual_write(11, f"更新完畢，重啟中...")
            time.sleep(3)

            # 5. 重啟程式
            # os.execv 是最可靠的重啟方式
            os.execv(sys.executable, ['python'] + sys.argv)
        else:
            print("[OTA] 目前已是最新版本。")
            blynk.virtual_write(11, "已是最新版本")

    except Exception as e:
        print(f"[OTA] 更新過程中發生未知錯誤: {e}")
        blynk.virtual_write(11, f"更新時發生錯誤: {e}")


# --- 函數定義 (其餘保持不變) ---
def locator():
    print("正在透過 1NCE API 獲取 GPS 位置...")
    url_token = "https://api.1nce.com/management-api/oauth/token"
    payload = {"grant_type": "client_credentials"}
    headers_token = {"accept": "application/json", "content-type": "application/json", "authorization": nceid_token}
    access_token = None
    try:
        token_response = requests.post(url_token, json=payload, headers=headers_token, timeout=10)
        token_response.raise_for_status()
        access_token = token_response.json().get('access_token')
        if not access_token: return None
        print("成功獲取 1NCE Access Token。")
    except:
        return None

    url_location = "https://api.1nce.com/management-api/v1/locate/positions/latest?page=1&per_page=10"
    headers_location = {"accept": "application/json", "authorization": f"Bearer {access_token}"}
    try:
        gps_response = requests.get(url_location, headers=headers_location, timeout=15)
        gps_response.raise_for_status()
        data = gps_response.json()
        positions_list = data.get('coordinates')
        if positions_list:
            for position_data in positions_list:
                if position_data.get('deviceId') == nceid:
                    coord_array = position_data.get('coordinate')
                    if coord_array and len(coord_array) == 2:
                        location_str = f"{coord_array[1]},{coord_array[0]}" # 緯度,經度
                        print(f"成功解析 1NCE 位置: {location_str}")
                        return location_str
            return None
        return None
    except:
        return None

def power_read_and_send(message_list, client, location):
    pggg, paaa, pppp, pgaa, pgpp = [], [], [], [], []
    for data_string in message_list:
        try:
            parts = data_string.split('/')
            time_parts = parts[0].split('_')
            timestruct = '%s-%s-%s %s:%s:%s' % tuple(time_parts)
            localtime = time.strptime(timestruct ,'%Y-%m-%d %H:%M:%S')
            time_stamp_utc = int(time.mktime(localtime))*1000
            pg_val, pa_val, pp_val = map(int, parts[1:])
            pa_calibrated = int(pa_val * factor_a)
            pp_calibrated = int(pp_val * factor_p)
            pga_efficiency = (pa_val - pg_val) / pg_val if pg_val != 0 else 0
            pgp_efficiency = (pp_val - pg_val) / pg_val if pg_val != 0 else 0
            pggg.append([time_stamp_utc, pg_val])
            paaa.append([time_stamp_utc, pa_calibrated])
            pppp.append([time_stamp_utc, pp_calibrated])
            pgaa.append([time_stamp_utc, f"{pga_efficiency:.3f}"])
            pgpp.append([time_stamp_utc, f"{pgp_efficiency:.3f}"])
        except:
            continue
    if not pggg: return
    if len(pggg) == 1:
        try:
            blynk.virtual_write(4, pggg[0][1])
            blynk.virtual_write(5, paaa[0][1])
            blynk.virtual_write(6, pppp[0][1])
            blynk.virtual_write(7, float(pgaa[0][1]))
            blynk.virtual_write(8, float(pgpp[0][1]))
        except: pass
    elif len(pggg) > 1:
        headers = {'Content-type': 'application/json'}
        base_url = f'https://blynk.cloud/external/api/batch/update?token={blynk_token}'
        data_to_upload = {'v4': pggg, 'v5': paaa, 'v6': pppp, 'v7': pgaa, 'v8': pgpp}
        try:
            for pin_name, data_list in data_to_upload.items():
                upload_url = f'{base_url}&pin={pin_name}'
                requests.post(upload_url, headers=headers, json=data_list, timeout=15)
        except: pass
    if location:
        try:
            loc_parts = location.split(',')
            blynk.virtual_write(10, loc_parts[0], loc_parts[1], "Solar Tracker")
        except: pass

def connect_mqtt():
    def on_connect(client, userdata, flags, rc): print(f"連接本地 MQTT Broker {'成功' if rc == 0 else '失敗'}")
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        global message
        message = [item for item in msg.payload.decode().strip('"').split(',') if item]
    client.subscribe(topic_sub)
    client.on_message = on_message

db_name = f"solarsdgs{iot}.db"
def create_database():
    with sqlite3.connect(db_name) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS TatungForeverEnergy (
                     ID INTEGER PRIMARY KEY AUTOINCREMENT, TIME TEXT, LOCATION TEXT, PG INTEGER, PA INTEGER, PP INTEGER)""")
    print("資料庫確認完畢。")

def insert_database(timestring, location, pg, pa, pp):
    with sqlite3.connect(db_name) as conn:
        conn.cursor().execute("INSERT INTO TatungForeverEnergy(TIME, LOCATION, PG, PA, PP) VALUES(?, ?, ?, ?, ?)",
                      (timestring, location, pg, pa, pp))

# --- Blynk 設定 ---
try:
    blynk = BlynkLib.Blynk(blynk_token)
except Exception as e:
    print(f"Blynk 連線失敗: {e}, 10秒後重啟。"); time.sleep(10); system('reboot')

@blynk.on("V0")
def v0_write_handler(value): global factor_a; factor_a = float(value[0]); print(f'factor_a 更新為: {factor_a}')
@blynk.on("V1")
def v1_write_handler(value): global factor_p; factor_p = float(value[0]); print(f'factor_p 更新為: {factor_p}')
@blynk.on("V3")
def v3_write_handler(value): global pizero2_on; pizero2_on = str(value[0]); print(f'pizero2_on 更新為: {pizero2_on}')
@blynk.on("V9")
def v9_write_handler(value): global pizero2_off; pizero2_off = str(value[0]); print(f'pizero2_off 更新為: {pizero2_off}')

# [OTA] 新增 Blynk 虛擬腳位 V11 的回呼函數，用於觸發更新
@blynk.on("V11")
def v11_write_handler(value):
    # 按鈕的值從 '0' 變成 '1' 時觸發
    if value and value[0] == '1':
        print("[OTA] 收到來自 Blynk 的更新指令！")
        blynk.virtual_write(11, "檢查更新中...") # 在 Blynk App 上顯示狀態
        check_for_updates()


@blynk.on("connected")
def blynk_connected(): 
    print("Blynk 已連接，同步伺服器數值...")
    blynk.sync_virtual(0, 1, 3, 9, 11) # [OTA] 同步 V11 的狀態

# --- 主程式初始化 ---
create_database()
client = connect_mqtt()
default_location = "24.960938,121.247177"
location = locator()
if location is None:
    location = default_location
    print("無法從 1NCE 獲取準確位置，將使用預設值。")

# --- 主迴圈 ---
while True:
    client.loop_start()
    subscribe(client)
    blynk.run()

    if message and message != message_check:
        print(f"偵測到 {len(message)} 筆新數據，開始處理...")
        for data_string in message:
            try:
                sql_data = data_string.split('/')
                insert_database(sql_data[0], location, int(sql_data[1]), int(sql_data[2]), int(sql_data[3]))
            except: continue
        print("資料庫儲存完畢。")
        power_read_and_send(message, client, location)
        message_check = list(message)
        print(f"處理完成，發送 ACK 確認訊息至主題 '{topic_ack}'")
        client.publish(topic_ack, "OK")
        client.publish(topic_pub, f"{pizero2_on}_{pizero2_off}")
    else:
        print("無新數據。")
    client.loop_stop()
    time.sleep(5)