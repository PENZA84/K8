import re
import yaml
import base64
import requests
import threading
import queue
from loguru import logger
from tqdm import tqdm

# ======================
# 配置与数据
# ======================
MAX_URLS = 10000
WORKER_THREADS = 32
url_queue = queue.Queue()
processed_urls = set()
all_nodes = set()
lock = threading.Lock()

# 基础协议正则（使用非捕获组）
NODE_REGEX = re.compile(r'(?:vless|hy2|hysteria2|hysteria|tuic)://[^\s"\'<>]+')
# 查找订阅链接的正则
LINK_REGEX = re.compile(r'https?://[^\s"\'<>]+')

# ======================
# 工具函数
# ======================
def safe_b64_decode(data):
    try:
        # 处理可能包含的 Base64 杂质
        data = re.sub(r'[^A-Za-z0-9+/=]', '', data)
        pad = '=' * (-len(data) % 4)
        return base64.b64decode(data + pad).decode('utf-8', errors='ignore')
    except:
        return ""

def extract_nodes(content):
    """提取 Base64 订阅或纯节点文本中的节点"""
    nodes = NODE_REGEX.findall(content)
    # 处理 Clash YAML 格式的节点 (简化提取)
    if 'proxies:' in content:
        # 尝试提取 server: 后的 IP/域名 (粗略提取，仅作补充)
        servers = re.findall(r'server:\s*([^\s]+)', content)
        # 这里可以加入更复杂的 Clash 配置解析逻辑
    return nodes

# ======================
# 工作线程逻辑
# ======================
def worker():
    while True:
        try:
            url = url_queue.get(timeout=3)
        except queue.Empty:
            break
            
        try:
            res = requests.get(url, timeout=10, headers={'User-Agent': 'clash-verge/v2.0.2'})
            if res.status_code == 200:
                content = res.text
                
                # 1. 尝试提取节点
                found_nodes = []
                if any(x in content for x in ["proxies:", "proxy-groups:"]):
                    found_nodes = extract_nodes(content)
                else:
                    decoded = safe_b64_decode(content)
                    found_nodes = NODE_REGEX.findall(decoded) if decoded else NODE_REGEX.findall(content)
                
                with lock:
                    all_nodes.update(found_nodes)
                
                # 2. 递归发现更多订阅
                if len(processed_urls) < MAX_URLS:
                    new_links = LINK_REGEX.findall(content)
                    with lock:
                        for link in new_links:
                            if any(k in link for k in ['sub', 'clash', 'proxy', 'raw.githubusercontent.com']) and link not in processed_urls:
                                processed_urls.add(link)
                                url_queue.put(link)
        except:
            pass
        finally:
            url_queue.task_done()

# ======================
# 主程序
# ======================
if __name__ == '__main__':
    # 从 latest.yaml 加载种子
    with open('latest.yaml', 'r', encoding="utf-8") as f:
        data = yaml.safe_load(f)
        for urls in data.values():
            for url in urls:
                if url not in processed_urls:
                    processed_urls.add(url)
                    url_queue.put(url)

    logger.info(f"开始解析，初始队列: {url_queue.qsize()}")
    
    threads = []
    for _ in range(WORKER_THREADS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)
        
    url_queue.join()
    
    with open('all_nodes.txt', 'w', encoding="utf-8") as f:
        f.write('\n'.join(all_nodes))
    logger.info(f"全部完成，共提取节点: {len(all_nodes)}")
