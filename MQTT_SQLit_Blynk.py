# new_solarsdgs_6002_pizero2.py (最終版 - 恢復自身OTA功能)

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

# --- 設定 ---
iot = '6002'
blynk_token = 'hOALK-HCU1uYRuZ7daGMci5adH1PyqZY'
nceid = '8988228066614762250'
nceid_token = "Basic Z3JheUBzb2xhcnNkZ2MuY29tOjk2NzYyMzY0"

# --- Pi Zero 自身 OTA 更新設定 (來自您的原始程式碼) ---
PIZERO_CURRENT_VERSION = 3.5 
PIZERO_VERSION_URL = f"https://raw.githubusercontent.com/luftqi/solar_picow{iot}/main/pizero_version.txt"
PIZERO_SCRIPT_URL = f"https://raw.githubusercontent.com/luftqi/solar_picow{iot}/main/MQTT_SQLit_Blynk.py"

# --- Pico W 遠端管理設定 ---
SAFE_PICO_MAIN_PY_PATH = 'main_pico_safe_copy.py' 

# --- 全域變數 ---
factor_a, factor_p = 1.0, 1.0
pizero2_on, pizero2_off = "30", "50"
message, message_check = [], []
blynk = None 
client = None

# --- MQTT 設定 ---
broker = '127.0.0.1'
port = 1883
topic_sub = "pg_pa_pp"
topic_pub = "pizero2onoff"
topic_ack = "pico/ack"
pico_control_topic = "pico/control" 
client_id = f'pizero{iot}_0'
username = f'solarsdgs{iot}'
password = '82767419'
topic_pico_cmd_in = f'pico/{iot}/cmd/in'
topic_pico_cmd_out = f'pico/{iot}/cmd/out'
topic_pico_admin_ota = f'pico/{iot}/admin/run_ota'
topic_pico_admin_rescue = f'pico/{iot}/admin/enter_rescue'
topic_pico_rescue_in = f'pico/{iot}/rescue/in'
topic_pico_rescue_out = f'pico/{iot}/rescue/out'


# --- 函數定義 ---
def check_for_updates():
    """檢查 GitHub 上是否有 Pi Zero 自身的新版本，並執行更新。"""
    print("[PIZERO_OTA] 正在檢查 Pi Zero 自身更新...")
    try:
        blynk.virtual_write(12, "檢查 PiZero 版本...")
        response = requests.get(PIZERO_VERSION_URL, timeout=10)
        if response.status_code != 200:
            blynk.virtual_write(12, f"無法獲取 PiZero 版本文件: {response.status_code}")
            return
        remote_version = float(response.text.strip())
        print(f"[PIZERO_OTA] 當前版本: {PIZERO_CURRENT_VERSION}, 遠端版本: {remote_version}")
        if remote_version > PIZERO_CURRENT_VERSION:
            blynk.virtual_write(12, f"發現 PiZero 新版本 {remote_version}...")
            script_response = requests.get(PIZERO_SCRIPT_URL, timeout=30)
            if script_response.status_code != 200:
                blynk.virtual_write(12, f"PiZero 下載失敗: {script_response.status_code}")
                return
            new_script_content = script_response.text
            script_path = os.path.abspath(__file__)
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(new_script_content)
            blynk.virtual_write(12, f"PiZero 更新完畢，重啟中...")
            time.sleep(3)
            system('reboot')
        else:
            blynk.virtual_write(12, "PiZero 已是最新版本")
    except Exception as e:
        print(f"[PIZERO_OTA] 更新過程中發生未知錯誤: {e}")
        blynk.virtual_write(12, f"PiZero 更新時發生錯誤")

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
    except: return None
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
                        return f"{coord_array[1]},{coord_array[0]}"
            return None
        return None
    except: return None

def power_read_and_send(message_list, client_mqtt, location): 
    all_uploads_successful = True
    pggg, paaa, pppp, pgaa, pgpp = [], [], [], [], []
    for data_string in message_list:
        try:
            parts = data_string.split('/')
            time_parts = parts[0].split('_')
            timestruct = '%s-%s-%s %s:%s:%s' % tuple(time_parts)
            localtime = time.strptime(timestruct ,'%Y-%m-%d %H:%M:%S')
            time_stamp_utc = int(time.mktime(localtime))*1000
            pg_val, pa_val, pp_val = map(int, parts[1:])
            pa_calibrated = int(pa_val * factor_a); pp_calibrated = int(pp_val * factor_p)
            pga_efficiency = (pa_val - pg_val)*100 / pg_val if pg_val != 0 else 0
            pgp_efficiency = (pp_val - pg_val)*100 / pg_val if pg_val != 0 else 0
            pggg.append([time_stamp_utc, pg_val]); paaa.append([time_stamp_utc, pa_calibrated])
            pppp.append([time_stamp_utc, pp_calibrated]); pgaa.append([time_stamp_utc, pga_efficiency])
            pgpp.append([time_stamp_utc, pgp_efficiency])
        except: continue
    if not pggg: return True

    if len(pggg) == 1:
        try:
            blynk.virtual_write(4, pggg[0][1]); blynk.virtual_write(5, paaa[0][1]); blynk.virtual_write(6, pppp[0][1])
            blynk.virtual_write(7, pgaa[0][1]); blynk.virtual_write(8, pgpp[0][1])
        except Exception as e: all_uploads_successful = False
    elif len(pggg) > 1:
        headers = {'Content-type': 'application/json'}
        base_url = f'https://blynk.cloud/external/api/batch/update?token={blynk_token}'
        data_to_upload = {'v4': pggg, 'v5': paaa, 'v6': pppp, 'v7': pgaa, 'v8': pgpp}
        try:
            for pin_name, data_list in data_to_upload.items():
                upload_url = f'{base_url}&pin={pin_name}'
                response = requests.post(upload_url, headers=headers, json=data_list, timeout=15)
                if response.status_code != 200: all_uploads_successful = False
        except: all_uploads_successful = False
    
    if location:
        try:
            loc_parts = location.split(','); blynk.virtual_write(10, loc_parts[0], loc_parts[1], "Solar Tracker")
        except: pass
    return all_uploads_successful

_mqtt_connected_once = False 

def connect_mqtt():
    global _mqtt_connected_once
    def on_connect(client_mqtt, userdata, flags, rc): 
        global _mqtt_connected_once
        if rc == 0:
            if not _mqtt_connected_once:
                print("連接本地 MQTT Broker 成功")
                _mqtt_connected_once = True
            subscribe(client_mqtt)
            print("已訂閱所有相關 MQTT 主題。")
        else:
            print(f"連接本地 MQTT Broker 失敗，錯誤碼: {rc}")
            _mqtt_connected_once = False 

    client_mqtt = mqtt_client.Client(client_id); 
    client_mqtt.username_pw_set(username, password)
    client_mqtt.on_connect = on_connect; 
    
    try:
        client_mqtt.connect(broker, port)
    except Exception as e:
        print(f"連接 MQTT Broker 時發生異常: {e}")
        _mqtt_connected_once = False
        raise

    return client_mqtt

def subscribe(client_mqtt: mqtt_client):
    def on_message(client_mqtt, userdata, msg):
        global message
        topic = msg.topic
        payload = msg.payload.decode()

        print(f"收到來自主題 '{topic}' 的訊息: {payload}")

        if topic == topic_sub:
            message = [item for item in payload.strip('"').split(',') if item]
        elif topic == topic_pico_cmd_out:
            print(f"<<< 收到來自 Pico REPL 的結果: {payload}")
            blynk.virtual_write(20, payload)
        elif topic == topic_pico_rescue_out:
            print(f"[RESCUE] 來自 Pico 救援通道的回應: {payload}")
            blynk.virtual_write(20, f"[RESCUE] {payload}")
    
    print("正在訂閱主題...")
    client_mqtt.subscribe(topic_sub)
    client_mqtt.subscribe(topic_pico_cmd_out)
    client_mqtt.subscribe(topic_pico_rescue_out)
    client_mqtt.on_message = on_message

db_name = f"solarsdgs{iot}.db"
def create_database():
    with sqlite3.connect(db_name) as conn:
        conn.cursor().execute("""CREATE TABLE IF NOT EXISTS TatungForeverEnergy (
                     ID INTEGER PRIMARY KEY AUTOINCREMENT, TIME TEXT UNIQUE, LOCATION TEXT, PG INTEGER, PA INTEGER, PP INTEGER)""")
    print("資料庫確認完畢 (TIME 欄位已設為唯一)。")

def insert_database_batch(new_data_list):
    if not new_data_list: return
    with sqlite3.connect(db_name) as conn:
        conn.executemany("INSERT OR IGNORE INTO TatungForeverEnergy(TIME, LOCATION, PG, PA, PP) VALUES(?, ?, ?, ?, ?)", new_data_list)
    print(f"資料庫批次寫入完成，共處理 {len(new_data_list)} 筆新數據。")

def get_existing_timestamps(c):
    c.execute('SELECT TIME FROM TatungForeverEnergy')
    return {row[0] for row in c.fetchall()}

# --- Blynk 設定 ---
while blynk is None:
    try:
        print("正在嘗試連接到 Blynk 伺服器...")
        blynk = BlynkLib.Blynk(blynk_token)
        print("Blynk 連接成功！")
    except Exception as e:
        print(f"Blynk 連接失敗: {e}")
        print("將在 15 秒後重試...")
        time.sleep(15)

# --- Blynk 虛擬腳位處理函數 ---
@blynk.on("V0")
def v0_write_handler(value): 
    global factor_a
    if value: factor_a = float(value[0])
    print(f'factor_a 更新為: {factor_a}')

@blynk.on("V1")
def v1_write_handler(value): 
    global factor_p
    if value: factor_p = float(value[0])
    print(f'factor_p 更新為: {factor_p}')

@blynk.on("V3")
def v3_write_handler(value): 
    global pizero2_on
    if value: pizero2_on = str(value[0])
    print(f'pizero2_on 更新為: {pizero2_on}')

@blynk.on("V9")
def v9_write_handler(value): 
    global pizero2_off
    if value: pizero2_off = str(value[0])
    print(f'pizero2_off 更新為: {pizero2_off}')

# --- 程式碼恢復：V11 處理函數，用於 Pi Zero 自身更新 ---
# 註解：此函數為您原始程式碼中的功能，現已恢復正常運作。
@blynk.on("V11")
def v11_write_handler(value):
    print(f"[PIZERO_OTA_HANDLER] V11 write event received! Value: {value}")
    if value and value[0] == '1':
        print("[PIZERO_OTA] Switch ON detected. Initiating self-update process...")
        blynk.virtual_write(11, 0)
        check_for_updates()

@blynk.on("V13") 
def v13_write_handler(value):
    print(f"[REBOOT_PICO] V13 write event received! Value: {value}")
    if value and value[0] == '1':
        print("[REBOOT_PICO] 收到重啟 Pico 指令，發送 MQTT 訊息...")
        blynk.virtual_write(13, 0)
        try:
            if client and client.is_connected():
                client.publish(pico_control_topic, "reboot")
                print("[REBOOT_PICO] MQTT 重啟指令已發送。")
            else:
                print("[REBOOT_PICO] 發送失敗，MQTT 未連線。")
        except Exception as e:
            print(f"[REBOOT_PICO] 發送 MQTT 重啟指令失敗: {e}")

@blynk.on("V14")
def v14_trigger_pico_ota_handler(value):
    if value and value[0] == '1':
        blynk.virtual_write(14, 0)
        print("[PICO_MANAGE] 收到觸發 Pico OTA 指令...")
        blynk.virtual_write(20, "[PICO_MANAGE] 正在命令 Pico 執行自身 OTA...")
        if client and client.is_connected():
            client.publish(topic_pico_admin_ota, '1')
        else:
            blynk.virtual_write(20, "錯誤：MQTT 未連線！")

@blynk.on("V15")
def v15_enter_rescue_handler(value):
    if value and value[0] == '1':
        blynk.virtual_write(15, 0)
        print("[PICO_RESCUE] 收到指令：讓 Pico 進入永久救援模式...")
        blynk.virtual_write(20, "[PICO_RESCUE] 正在命令 Pico 建立救援旗標並重啟...")
        if client and client.is_connected():
            client.publish(topic_pico_admin_rescue, '1')
        else:
            blynk.virtual_write(20, "[PICO_RESCUE] 錯誤：MQTT 未連線！")

@blynk.on("V16")
def v16_send_rescue_code_handler(value):
    if value and value[0] == '1':
        blynk.virtual_write(16, 0)
        print("[PICO_RESCUE] 收到指令：發送安全的 main.py 備份...")
        blynk.virtual_write(20, f"[PICO_RESCUE] 讀取備份檔 {SAFE_PICO_MAIN_PY_PATH} 並發送...")
        try:
            with open(SAFE_PICO_MAIN_PY_PATH, 'r', encoding='utf-8') as f:
                safe_code = f.read()
            if client and client.is_connected():
                client.publish(topic_pico_rescue_in, safe_code, qos=1)
                blynk.virtual_write(20, "救援包已發送，請觀察 Pico 的回應。")
            else:
                blynk.virtual_write(20, "[PICO_RESCUE] 錯誤：MQTT 未連線！")
        except FileNotFoundError:
            blynk.virtual_write(20, f"[PICO_RESCUE] 錯誤：找不到備份檔 {SAFE_PICO_MAIN_PY_PATH}！")
        except Exception as e:
            blynk.virtual_write(20, f"[PICO_RESCUE] 發送備份時出錯: {e}")

@blynk.on("V20")
def v20_terminal_handler(value):
    if value:
        command = value[0]
        print(f">>> 發送指令到 Pico: {command}")
        blynk.virtual_write(20, f'>>> {command}\n') 
        try:
            if client and client.is_connected():
                client.publish(topic_pico_cmd_in, command)
            else:
                err_msg = "錯誤：MQTT 未連線，無法發送指令。"
                print(err_msg)
                blynk.virtual_write(20, err_msg)
        except Exception as e:
            err_msg = f"錯誤：發送指令時發生異常: {e}"
            print(err_msg)
            blynk.virtual_write(20, err_msg)

@blynk.on("connected")
def blynk_connected():
    print("Blynk 已連接，同步伺服器數值...")
    blynk.sync_virtual(0, 1, 3, 9, 11, 12, 13, 14, 15, 16, 20) 

# --- 主程式初始化 ---
create_database()
try:
    client = connect_mqtt() 
    # --- 程式碼修正：停用會造成衝突的 loop_start() ---
    # client.loop_start() 
except Exception as e:
    print(f"MQTT 客戶端初始化失敗，程式將重試: {e}")
    client = None

default_location = "24.960938,121.247177"
location = locator()
if location is None: location = default_location
print(f"目前使用的位置: {location}")

# --- 主迴圈 ---
while True:
    blynk.run() 
    
    # --- 程式碼修正：將 MQTT 事件處理移至主迴圈，與 Blynk 協同運作 ---
    if client:
        client.loop(timeout=0.01) # 給予 MQTT 客戶端一小段時間來處理網路事件

    if client and client.is_connected(): 
        if message and message != message_check:
            print(f"\n偵測到新訊息 (包含 {len(message)} 筆數據)，開始處理...")
            new_data_to_process = []
            new_data_for_db = []
            try:
                with sqlite3.connect(db_name) as conn:
                    existing_timestamps = get_existing_timestamps(conn.cursor())
                
                for data_string in message:
                    try:
                        timestamp = data_string.split('/')[0]
                        if timestamp not in existing_timestamps:
                            new_data_to_process.append(data_string)
                            sql_data = data_string.split('/')
                            new_data_for_db.append((sql_data[0], location, int(sql_data[1]), int(sql_data[2]), int(sql_data[3])))
                    except: continue

                if new_data_to_process:
                    print(f"去重後，有 {len(new_data_to_process)} 筆全新數據需要處理。")
                    insert_database_batch(new_data_for_db)
                    upload_successful = power_read_and_send(new_data_to_process, client, location) 
                    if upload_successful:
                        print("Blynk 上傳成功，已發送 ACK。")
                        message_check = list(message)
                        client.publish(topic_ack, "OK")
                    else:
                        print("Blynk 上傳失敗，未發送 ACK，數據將在下一輪重試。")
                else:
                    print("收到的均為重複數據，直接發送 ACK 以協助 Pico 清除暫存。")
                    message_check = list(message)
                    client.publish(topic_ack, "OK")
                
                client.publish(topic_pub, f"{pizero2_on}_{pizero2_off}")

            except Exception as e:
                print(f"數據處理主流程發生嚴重錯誤: {e}")
        else:
            if message == message_check:
                print("無新數據。")
    else:
        print("MQTT 客戶端未連線，跳過數據處理。")

    time.sleep(5)
