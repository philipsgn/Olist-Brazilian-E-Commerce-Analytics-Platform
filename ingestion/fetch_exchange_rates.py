import requests
import json
from datetime import datetime

def fetch_exchange_rates():
    """
    Gọi API lấy tỷ giá hối đoái mới nhất
    """
    url = "https://open.er-api.com/v6/latest/USD"
    
    # Gọi API - giống như bấm URL trên browser, nhưng bằng Python
    response = requests.get(url)
    
    # Kiểm tra có lỗi không (200 = thành công)
    if response.status_code == 200:
        data = response.json()  # Chuyển JSON thành dict Python
        
        # Lấy ra những tỷ giá mình cần
        rates = {
            "fetched_at": datetime.now().isoformat(),
            "base_currency": "USD",
            "brl_to_usd": 1 / data["rates"]["BRL"],  # Đổi ngược lại
            "vnd_to_usd": 1 / data["rates"]["VND"],
        }
        
        print(f"Tỷ giá lấy lúc: {rates['fetched_at']}")
        print(f"1 BRL = {rates['brl_to_usd']:.4f} USD")
        return rates
    else:
        print(f"Lỗi API: {response.status_code}")
        return None

# Chạy thử
if __name__ == "__main__":
    result = fetch_exchange_rates()
    print(json.dumps(result, indent=2))