# 引入函式庫
import network
import socket
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

# 啟用看門狗，超時時間8秒
wdt = machine.WDT(timeout=8000)

# 全域變數
ack_received = False
iot = "6002"
wifi_wait_time = 60
LOOP_INTERVAL = 33 

# --- 新增變數：設定最小可用空間閾值 (位元組) ---
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
        # machine.reset() # <--- 這裡維持 reset，因為這是 MQTT 連線的錯誤處理

def my_callback(topic, message):
    global pizero2_on, pizero2_off, ack_received
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
    elif topic_str == 'pico/control' and message_str == 'reboot': 
        print("[CONTROL] 收到重啟指令，正在重啟...")
        disable_wdt() 
        time.sleep(2) 
        machine.reset()

def disable_wdt():
    """
    暫時禁用看門狗以執行長時間的阻塞操作。
    """
    print("看門狗已暫時禁用。")
    machine.mem32[0x40058000] &= ~(1 << 30)
    # machine.mem32[0x400d8000] &= ~(1 << 30)


# --- 主程式初始化 ---
try:
    with open("pizero2on.txt", "r") as f1: pizero2_on = int(f1.read())
    with open("pizero2off.txt", "r") as f2: pizero2_off = int(f2.read())
except (OSError, ValueError):
    pizero2_on, pizero2_off = 30, 50

reset_hour, reset_minute = 12, 10
sleep_hour, sleep_minute = 19, 05
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
            print(f"空間不足，啟動期數據未暫存 (可用空間: {current_free_space/1024:.2f} KB < {MIN_FREE_SPACE_BYTES/1024:.2f} KB)")
    except Exception as e:
        print(f"檢查空間或寫入啟動期數據時發生錯誤: {e}")

    print(f"等待 {LOOP_INTERVAL} 秒...")
    for _ in range(LOOP_INTERVAL):
        wdt.feed()
        time.sleep(1)

# --- 主迴圈 ---
while True:
    wdt.feed() 
    loop_start_time = time.time()
    gc.collect()
    current_time = time.localtime()
    current_hour = current_time[3]
    current_minute = current_time[4]

    # [夜間假休眠功能] 不再重啟，而是等待重新連線
    if current_hour == sleep_hour and current_minute == sleep_minute:
        print("="*40)
        print(f"到達夜間休眠時間 ({sleep_hour}:{sleep_minute:02d})，準備進入長時間假休眠...")
        print("="*40)
        
        # 步驟 1: 關閉周邊硬體和網路連線
        pin_6.off() # 關閉 pin_6 (Wi-Fi 模組電源)
        timer.deinit() # 禁用 Timer
        led.off() # 關閉 LED
        try: # 斷開 MQTT
            if client: 
                client.disconnect()
                client = None 
                print("MQTT 已離線。")
        except:
            pass 
        if wlan.isconnected(): # 關閉 Wi-Fi
            wlan.disconnect()
            wlan.active(False)
            print("Wi-Fi 已關閉。")

        # 步驟 2: 禁用看門狗
        disable_wdt() 

        # 步驟 3: 執行長時間的 time.sleep()
        
        print(f"系統將進入假休眠 {long_sleep_seconds} 秒 ({long_sleep_seconds // 3600}小時)...")
        time.sleep(long_sleep_seconds) 

        # --- 新增：假休眠結束後的硬體及網路重新啟動 ---
        print("假休眠結束，正在重新啟動硬體和網路...")
        led.on() 
        timer = Timer() 
        timer.init(freq=1, mode=Timer.PERIODIC, callback=lambda t: led.toggle()) 
        pin_6.on() # <--- 關鍵：重新開啟 pin6 以便 Wi-Fi 工作
        
        # --- 新增：等待 Pi Zero Wi-Fi 熱點啟動就緒 ---
        print("等待 Pi Zero Wi-Fi 熱點啟動就緒 (至少 30 秒)...")
        # 這裡給予更長的延遲，確保 Pi Zero 的 Wi-Fi 熱點有足夠時間啟動
        # 如果 Pi Zero 開機需要 30 秒才分享 Wi-Fi，這裡可以設定 35-40 秒
        time.sleep(40) # 等待 40 秒，確保 Pi Zero Wi-Fi 熱點就緒
        # --- 結束新增 ---

        print("硬體和網路重啟指令已發送。")
        # ----------------------------------------------------

    # ------ 日間工作邏輯 ------
    # 在主迴圈中，根據 client 和 wlan 狀態來嘗試連線
    if not wlan.isconnected(): # 優先檢查 Wi-Fi
        print("偵測到 Wi-Fi 未連線，執行連線...")
        wifi_connect(ssid, password)
    
    if wlan.isconnected() and client is None: # Wi-Fi 連線成功但 MQTT 未連線
        print("偵測到 MQTT 未連線，執行連線...")
        disable_wdt(); set_time(); 
        wdt = machine.WDT(timeout=8000); wdt.feed() 
        
        try:
            disable_wdt(); client = connect_mqtt(); 
            wdt = machine.WDT(timeout=8000); wdt.feed() 
            if client: 
                client.set_callback(my_callback)
                client.subscribe(b'pizero2onoff')
                client.subscribe(b'pico/ack')
                client.subscribe(b'pico/control') 
        except Exception as e:
            print(f"重新連接 MQTT 失敗: {e}")
            client = None 

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
            print(f"空間不足，數據未暫存 (可用空間: {current_free_space/1024:.2f} KB < {MIN_FREE_SPACE_BYTES/1024:.2f} KB)")
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

    try: 
        if client: client.check_msg()
    except Exception as e:
        print(f"檢查MQTT訊息時出錯: {e}")
        try: client.disconnect()
        except: pass
        client = None

    if current_hour == reset_hour and current_minute == reset_minute:
        disable_wdt()
        print("執行每日定時重啟...");
        time.sleep(5)
        machine.reset() 

    work_duration = time.time() - loop_start_time
    sleep_for = LOOP_INTERVAL - work_duration
    if sleep_for > 0:
        for _ in range(int(sleep_for)):
            wdt.feed()
            time.sleep(1)
        wdt.feed()
        time.sleep(sleep_for % 1)

