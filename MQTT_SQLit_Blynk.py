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
CURRENT_VERSION = 3.0 
VERSION_URL = f"https://raw.githubusercontent.com/luftqi/solar_picow{iot}/main/pizero_version.txt"
SCRIPT_URL = "https://raw.githubusercontent.com/luftqi/solar_picow{iot}/main/MQTT_SQLit_Blynk.py"

# --- 全域變數 ---
factor_a, factor_p = 1.0, 1.0
pizero2_on, pizero2_off = "30", "50"
message, message_check = [], []
blynk = None # <-- 在此處初始化 blynk 變數 (保持 None，等待初始化迴圈)

# --- MQTT 設定 ---
broker = '127.0.0.1'
port = 1883
topic_sub = "pg_pa_pp"
topic_pub = "pizero2onoff"
topic_ack = "pico/ack"
pico_control_topic = "pico/control" # <--- 新增 Pico 控制主題
client_id = f'pizero{iot}_0'
username = f'solarsdgs{iot}'
password = '82767419'

# --- 函數定義 ---
def check_for_updates():
    """檢查 GitHub 上是否有新版本，如果有，則下載、覆蓋並重啟。"""
    print("[OTA] 正在檢查更新...")
    try:
        blynk.virtual_write(12, "檢查版本...")
        response = requests.get(VERSION_URL, timeout=10)
        if response.status_code != 200:
            blynk.virtual_write(12, f"無法獲取版本文件: {response.status_code}")
            return
        remote_version = float(response.text.strip())
        print(f"[OTA] 當前版本: {CURRENT_VERSION}, 遠端版本: {remote_version}")
        if remote_version > CURRENT_VERSION:
            blynk.virtual_write(12, f"發現新版本 {remote_version}...")
            script_response = requests.get(SCRIPT_URL, timeout=30)
            if script_response.status_code != 200:
                blynk.virtual_write(12, f"下載失敗: {script_response.status_code}")
                return
            new_script_content = script_response.text
            script_path = os.path.abspath(__file__)
            with open(script_path, 'w', encoding='utf-8') as f:
                f.write(new_script_content)
            blynk.virtual_write(12, f"更新完畢，重啟中...")
            time.sleep(3)
            system('reboot')
        else:
            blynk.virtual_write(12, "已是最新版本")
    except Exception as e:
        print(f"[OTA] 更新過程中發生未知錯誤: {e}")
        blynk.virtual_write(12, f"更新時發生錯誤")

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

def power_read_and_send(message_list, client_mqtt, location): # 更改參數名避免與 client_id 衝突
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

def connect_mqtt():
    def on_connect(client_mqtt, userdata, flags, rc): print(f"連接本地 MQTT Broker {'成功' if rc == 0 else '失敗'}")
    client_mqtt = mqtt_client.Client(client_id); client_mqtt.username_pw_set(username, password)
    client_mqtt.on_connect = on_connect; client_mqtt.connect(broker, port)
    return client_mqtt

def subscribe(client_mqtt: mqtt_client):
    def on_message(client_mqtt, userdata, msg):
        global message
        message = [item for item in msg.payload.decode().strip('"').split(',') if item]
    client_mqtt.subscribe(topic_sub); client_mqtt.on_message = on_message

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

@blynk.on("V11")
def v11_write_handler(value):
    print(f"[OTA HANDLER] V11 write event received! Value: {value}")
    if value and value[0] == '1':
        print("[OTA] Switch ON detected. Initiating update process...")
        blynk.virtual_write(11, 0)
        check_for_updates()

@blynk.on("V13") # <--- 新增的 Blynk 按鈕處理
def v13_write_handler(value):
    print(f"[REBOOT_PICO] V13 write event received! Value: {value}")
    if value and value[0] == '1':
        print("[REBOOT_PICO] 收到重啟 Pico 指令，發送 MQTT 訊息...")
        try:
            client.publish(pico_control_topic, "reboot")
            blynk.virtual_write(13, 0) # 將按鈕狀態重置為 OFF
            print("[REBOOT_PICO] MQTT 重啟指令已發送。")
        except Exception as e:
            print(f"[REBOOT_PICO] 發送 MQTT 重啟指令失敗: {e}")
            blynk.virtual_write(13, 0) # 無論如何都重置按鈕

@blynk.on("connected")
def blynk_connected():
    print("Blynk 已連接，同步伺服器數值...")
    blynk.sync_virtual(0, 1, 3, 9, 11, 12, 13) # <--- 新增同步 V13

# --- 主程式初始化 ---
create_database()
client = connect_mqtt() # 確保這裡的 client 是你的 MQTT 客戶端物件
client.loop_start() # <--- 將 client.loop_start() 移到主迴圈外部
subscribe(client) # <--- 將 subscribe(client) 移到主迴圈外部

default_location = "24.960938,121.247177"
location = locator()
if location is None: location = default_location
print(f"目前使用的位置: {location}")

# --- 主迴圈 ---
while True:
    # client.loop_start() # <--- 從這裡移除
    # subscribe(client) # <--- 從這裡移除
    blynk.run() # Blynk 的 run 方法會處理 MQTT 的 loop，這是正確的

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
                upload_successful = power_read_and_send(new_data_to_process, client, location) # 確保這裡傳遞的是 MQTT 客戶端物件
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
            
    # client.loop_stop() # <--- 從這裡移除，因為 loop_start 移出去了
    time.sleep(5)
