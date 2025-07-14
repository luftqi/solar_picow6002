# main.py - 主應用程式 (最終版 - 修正 OTA 相關錯誤)

# 引入函式庫
import network
import time
import utime
import ntptime
import gc
import machine
import os
import ina226
import ubinascii
from simple import MQTTClient
from machine import Pin, I2C, Timer
import io

# --- 功能恢復：OTA 相關引入 ---
try:
    from ota import OTAUpdater
except ImportError:
    print("錯誤：找不到 ota.py 函式庫，OTA 功能將無法使用。")
    OTAUpdater = None

# --- 功能新增：定義 Pico W 當前版本號 ---
PICO_CURRENT_VERSION = "7.0" 

# 啟用看門狗，超時時間8秒
wdt = machine.WDT(timeout=8000)

# 全域變數
ack_received = False
iot = "6002"
wifi_wait_time = 60
LOOP_INTERVAL = 33 

# --- 設定最小可用空間閾值 (位元組) ---
MIN_FREE_SPACE_BYTES = 50 * 1024 

# --- 硬體引腳設定 ---
led = machine.Pin("LED", machine.Pin.OUT)
pin_6 = Pin(6, mode=Pin.OUT)
pin_7 = Pin(7, mode=Pin.OUT) 
pin_6.off(); pin_7.off(); led.on() 

# --- Wi-Fi & I2C ---
wlan = network.WLAN(network.STA_IF)
wlan.active(True)
wlan.config(pm = 0xa11140)
ssid = b'solarsdgs'+iot
password = b'82767419'
i2c = I2C(0,scl=Pin(1), sda=Pin(0))
devices = i2c.scan()
if not devices: print("錯誤: 找不到任何 I2C 設備!")

# --- 函數定義 ---
def power_read():
    try:
        ina = ina226.INA226(i2c, int(devices[0]))
        inb = ina226.INA226(i2c, int(devices[1]))
        inc = ina226.INA226(i2c, int(devices[2]))
        ina.set_calibration(); inb.set_calibration(); inc.set_calibration()
        utime.sleep_ms(10); vg = ina.bus_voltage
        utime.sleep_ms(10); va = inb.bus_voltage
        utime.sleep_ms(10); vp = inc.bus_voltage
        pin_7.on(); time.sleep(1)
        ig = ina.shunt_voltage * 100000
        ia = inb.shunt_voltage * 100000
        ip = inc.shunt_voltage * 100000
        pin_7.off()
        pg = int((ig if ig > 10 else 0) * (vg if vg > 1 else 0))
        pa = int((ia if ia > 10 else 0) * (va if va > 1 else 0))
        pp = int((ip if ip > 10 else 0) * (vp if vp > 1 else 0))
        # --- 舊版程式碼 (有 Bug，會覆蓋真實讀值) ---
        # pg = 1000
        # pa = 2000
        # pp = 3000
        print(f"Pg={pg}W, Pa={pa}W, Pp={pp}W")
        return pg, pa, pp
    except Exception as e:
        print(f"讀取功率時發生錯誤: {e}")
        return 0, 0, 0

def set_time(hrs_offset=8):
    try:
        ntptime.settime() 
        now_time = time.localtime((time.time() + hrs_offset*3600))
        machine.RTC().datetime((now_time[0], now_time[1], now_time[2], now_time[6], now_time[3], now_time[4], now_time[5], 0))
        print("RTC 時間設定完成")
    except Exception as e:
        print(f"NTP 時間同步失敗: {e}")

def wifi_connect(ssid, password):
    if wlan.isconnected(): return
    wlan.connect(ssid, password)
    for _ in range(30): 
        wdt.feed() 
        if wlan.status() >= 3:
            print(f'Wi-Fi 連線成功，IP: {wlan.ifconfig()[0]}')
            return
        print('等待 Wi-Fi 連線...')
        time.sleep(1)
    print('Wi-Fi 連線失敗')

def run_ota_check():
    """執行 Pico W 自身的 OTA 更新檢查"""
    global client 
    print("[PICO_OTA] 收到指令，開始執行自身 OTA 更新檢查...")
    try:
        # --- 舊版程式碼 (會導致錯誤) ---
        # if client and client.is_connected():
        # --- 新版程式碼 (修正 is_connected 錯誤) ---
        if client:
            client.publish(f'pico/{iot}/cmd/out'.encode(), "[PICO_OTA] Checking for updates...")
    except Exception as e:
        print(f"發布 OTA 狀態時出錯: {e}")

    if wlan.isconnected():
        if OTAUpdater is None:
            print("[PICO_OTA] 錯誤：OTAUpdater 函式庫未載入。")
            if client:
                client.publish(f'pico/{iot}/cmd/out'.encode(), "[PICO_OTA] Error: OTAUpdater library not found.")
            return

        disable_wdt()
        time.sleep(2) 
        print("Connect Github OTA")
        # --- 舊版程式碼 (URL 格式不正確) ---
        # firmware_url = f"https://github.com/luftqi/solar_picow{iot}/refs/heads/main/" 
        # --- 新版程式碼 (修正為正確的 GitHub Raw 內容 URL) ---
        firmware_url = f"https://raw.githubusercontent.com/luftqi/solar_picow{iot}/main/"
        
        print(f"Firmware base URL: {firmware_url}")
        try:
            ota_updater = OTAUpdater(firmware_url, "main.py")
            ota_updater.download_and_install_update_if_available()
        except Exception as e:
            print(f"OTA 更新過程中發生未知錯誤: {e}")
            if client:
                 client.publish(f'pico/{iot}/cmd/out'.encode(), f"[PICO_OTA] Error: {e}")
    else:
        print("[PICO_OTA] Wi-Fi 未連線，無法執行 OTA。")
        if client:
             client.publish(f'pico/{iot}/cmd/out'.encode(), "[PICO_OTA] Error: Wi-Fi not connected.")

def connect_mqtt():
    try:
        random_suffix = str(time.time())
        client_id = b'solarsdgs' + iot.encode() + b'-' + random_suffix.encode()
        print(f"使用時間戳隨機 Client ID: {client_id.decode()}")
        client = MQTTClient(client_id=client_id, server='10.42.0.1', user=b'solarsdgs'+iot, password=b'82767419', keepalive=7200)
        client.connect() 
        print('成功連接到 MQTT Broker')
        return client
    except Exception as e:
        print('連接 MQTT 失敗:', e); 
        time.sleep(5)
        machine.reset() 

def my_callback(topic, message):
    global pizero2_on, pizero2_off, ack_received, client, PICO_CURRENT_VERSION
    topic_str = topic.decode()
    message_str = message.decode()
    
    if topic_str == 'pico/ack' and message_str == 'OK':
        ack_received = True
    elif topic_str == 'pizero2onoff':
        try:
            on_time, off_time = map(int, message_str.split('_'))
            
            if on_time > off_time:
                on_time, off_time = off_time, on_time 
                print(f"注意：收到 on_time > off_time，已自動對調為 {on_time}_{off_time}")

            pizero2_on, pizero2_off = on_time, off_time
            with open("pizero2on.txt", "w") as f1: f1.write(str(pizero2_on))
            with open("pizero2off.txt", "w") as f2: f2.write(str(pizero2_off))
            print(f"pizero2_on/off 更新為: {pizero2_on}_{pizero2_off}") 
        except ValueError: 
            print(f"pizero2onoff 訊息格式錯誤: {message_str}")
    
    elif topic_str == f'pico/{iot}/admin/run_ota':
        run_ota_check()
        
    elif topic_str == f'pico/{iot}/cmd/in':
        command = message_str
        print(f"收到遠端指令: {command}")
        
        output_buffer = io.StringIO()
        original_terminal = os.dupterm()
        try:
            os.dupterm(output_buffer)
            exec(command, globals())
        except Exception as e:
            print(f"Error executing command: {e}")
        finally:
            os.dupterm(original_terminal)
        
        result = output_buffer.getvalue()
        if not result:
            result = "[OK] (指令已執行，無輸出)"
        
        print(f"指令執行結果: {result.strip()}")
        if client:
            client.publish(f'pico/{iot}/cmd/out'.encode(), result.strip())

    elif topic_str == f'pico/{iot}/admin/enter_rescue':
        print(">>> 收到進入救援模式指令！正在建立旗標並重啟...")
        try:
            with open('rescue.flag', 'w') as f:
                f.write('1')
            if client:
                client.publish(f'pico/{iot}/cmd/out'.encode(), "[OK] Rescue flag created. Rebooting now.")
            time.sleep(1)
            machine.reset()
        except Exception as e:
            if client:
                client.publish(f'pico/{iot}/cmd/out'.encode(), f"[ERROR] Failed to create rescue flag: {e}")

    elif topic_str == 'pico/control' and message_str == 'reboot': 
        print("[CONTROL] 收到重啟指令，正在重啟...")
        time.sleep(2) 
        machine.reset()

def disable_wdt():
    """
    禁用看門狗，用於執行 ntptime, mqtt connect 等
    無法在內部循環餵狗的長時間阻塞型操作。
    """
    print("看門狗已暫時禁用。")
    machine.mem32[0x40058000] &= ~(1 << 30)
    
# --- 主程式初始化 ---
try:
    with open("pizero2on.txt", "r") as f1: pizero2_on = int(f1.read())
    with open("pizero2off.txt", "r") as f2: pizero2_off = int(f2.read())
except (OSError, ValueError):
    pizero2_on, pizero2_off = 30, 50

reset_hour, reset_minute = 12, 10
sleep_hour, sleep_minute = 19, 5
long_sleep_seconds = 11 * 3600

timer = Timer() 
timer.init(freq=1, mode=Timer.PERIODIC, callback=lambda t: led.toggle())

client = None 

# 啟動期邏輯
intervals = wifi_wait_time // LOOP_INTERVAL
print(f"開始 {wifi_wait_time} 秒的啟動等待期...")
for i in range(intervals):
    wdt.feed()
    pg, pa, pp = power_read()
    nowtimestamp = "_".join(map(str, time.localtime()[0:6]))
    
    try:
        f_frsize = os.statvfs('/') [0]
        f_bfree = os.statvfs('/') [3]
        current_free_space = f_frsize * f_bfree
        
        if current_free_space > MIN_FREE_SPACE_BYTES:
            with open('data.txt', 'a') as f: f.write(f"{nowtimestamp}/{pg}/{pa}/{pp},")
            print(f"啟動期數據已暫存... (可用空間: {current_free_space/1024:.2f} KB)")
        else:
            print(f"空間不足，數據未暫存。")
    except Exception as e:
        print(f"檢查空間或寫入數據時發生錯誤: {e}")

    print(f"等待 {LOOP_INTERVAL} 秒...")
    for _ in range(LOOP_INTERVAL):
        wdt.feed()
        time.sleep(1)

# --- 在主迴圈開始前，確保 Wi-Fi 已連線 ---
print("確保 Wi-Fi 連線...")
if not wlan.isconnected():
    wifi_connect(ssid, password)

# --- 主迴圈 ---
while True:
    wdt.feed() 
    loop_start_time = time.time()
    gc.collect()
    current_time = time.localtime()
    current_hour = current_time[3]
    current_minute = current_time[4]

    # [夜間假休眠功能] 
    if current_hour == sleep_hour and current_minute == sleep_minute:
        print("="*40)
        print(f"到達夜間休眠時間 ({sleep_hour}:{sleep_minute:02d})，準備進入長時間假休眠...")
        print("="*40)
        
        pin_6.off() 
        timer.deinit() 
        led.off() 
        try: 
            if client: 
                client.disconnect()
                client = None 
                print("MQTT 已離線。")
        except: pass 
        if wlan.isconnected(): 
            wlan.disconnect()
            wlan.active(False)
            print("Wi-Fi 已關閉。")

        disable_wdt() 
        print(f"系統將進入假休眠 {long_sleep_seconds} 秒 ({long_sleep_seconds // 3600}小時)...")
        time.sleep(long_sleep_seconds) 

        print("假休眠結束，正在重新啟動硬體和網路...")
        led.on() 
        timer = Timer() 
        timer.init(freq=1, mode=Timer.PERIODIC, callback=lambda t: led.toggle()) 
        pin_6.on() 
        
        print("等待 Pi Zero Wi-Fi 熱點啟動就緒 (至少 30 秒)...")
        time.sleep(40) 
        print("硬體和網路重啟指令已發送。")

    # ------ 日間工作邏輯 ------
    if not wlan.isconnected(): 
        print("偵測到 Wi-Fi 未連線，執行連線...")
        wifi_connect(ssid, password)
    
    if wlan.isconnected() and client is None: 
        print("偵測到 MQTT 未連線，執行連線...")
        
        print("正在設定時間與連線MQTT，暫時禁用看門狗...")
        disable_wdt() 
        
        set_time()
        new_client = None
        try:
            new_client = connect_mqtt()
        except Exception as e:
            print(f"connect_mqtt 函數執行失敗: {e}")
        
        wdt = machine.WDT(timeout=8000)
        print("看門狗已重新啟用。")
        wdt.feed()
        
        client = new_client
        
        if client: 
            try:
                client.set_callback(my_callback)
                client.subscribe(b'pizero2onoff')
                client.subscribe(b'pico/ack')
                client.subscribe(b'pico/control')
                client.subscribe(f'pico/{iot}/cmd/in'.encode())
                client.subscribe(f'pico/{iot}/admin/enter_rescue'.encode())
                client.subscribe(f'pico/{iot}/admin/run_ota'.encode())
            except Exception as e:
                print(f"設定 MQTT 回調與訂閱時失敗: {e}")
                client = None
        else:
            print("MQTT 連線物件無效，跳過訂閱。")

    print("="*40)
    
    pg, pa, pp = power_read()
    nowtimestamp = "_".join(map(str, current_time[0:6]))
    
    try:
        f_frsize = os.statvfs('/') [0]
        f_bfree = os.statvfs('/') [3]
        current_free_space = f_frsize * f_bfree
        
        if current_free_space > MIN_FREE_SPACE_BYTES:
            with open('data.txt', 'a') as f: f.write(f"{nowtimestamp}/{pg}/{pa}/{pp},")
            print(f"數據已暫存 (可用空間: {current_free_space/1024:.2f} KB)")
        else:
            print(f"空間不足，數據未暫存。")
    except Exception as e:
        print(f"檢查空間或寫入數據時發生錯誤: {e}")

    if pizero2_on <= current_minute < pizero2_off: 
        if not wlan.isconnected(): wifi_connect(ssid, password)
        if wlan.isconnected() and client:
            all_data_to_send = ""
            try:
                with open('data.txt', 'r') as f: all_data_to_send = f.read()
            except OSError: pass 

            if all_data_to_send:
                payload = f'"{all_data_to_send}"'
                try:
                    client.publish(b'pg_pa_pp', payload)
                    ack_received = False
                    for _ in range(10):
                        wdt.feed()
                        client.check_msg()
                        if ack_received:
                            with open('data.txt', 'w') as f: f.write('')
                            print("ACK 確認成功，暫存檔已清空。")
                            break
                        time.sleep(1)
                    if not ack_received: print("警告：未收到 ACK 確認，數據將保留重試。")
                except Exception as e:
                    print(f"MQTT 發布失敗: {e}。數據將保留。")
                    try: client.disconnect()
                    except: pass
                    client = None 
    else: # 非工作時段
        pin_6.off() 
        if client:
            try: client.disconnect()
            except: pass
            client = None
            print("非工作時段，MQTT 已斷開連線。")

    if current_hour == reset_hour and current_minute == reset_minute:
        print("執行每日定時重啟...");
        time.sleep(5)
        machine.reset() 

    # --- 修正後的安全延遲迴圈 ---
    work_duration = time.time() - loop_start_time
    sleep_for = LOOP_INTERVAL - work_duration
    if sleep_for > 0:
        for _ in range(int(sleep_for)):
            wdt.feed()
            if client:
                try:
                    client.check_msg()
                except Exception as e:
                    print(f"延遲期間檢查 MQTT 訊息時出錯: {e}")
                    client = None 
                    break 
            time.sleep(1)
        wdt.feed()
        time.sleep(sleep_for % 1)
