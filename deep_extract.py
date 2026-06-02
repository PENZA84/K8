import re
import yaml
import base64
import requests
import threading
import queue
from loguru import logger

# ======================
# 配置与数据
# ======================
MAX_URLS = 10000
WORKER_THREADS = 32
url_queue = queue.Queue()
processed_urls = set()
# 使用字典进行去重：key为唯一标识(UUID或URI)
all_nodes_dict = {} 
lock = threading.Lock()

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64)
session.mount('https://', adapter)

NODE_URI_REGEX = re.compile(r'(?:vless|hy2|hysteria2|hysteria|tuic)://[^\s"\'<>]+')
LINK_REGEX = re.compile(r'https?://[^\s"\'<>]+')

# ======================
# 工具函数
# ======================
def safe_b64_decode(data):
    # 长度校验：Base64 编码通常是 4 的倍数，且节点数据量大
    if len(data) < 16: return ""
    clean_data = re.sub(r'[^A-Za-z0-9+/=]', '', data)
    try:
        return base64.b64decode(clean_data).decode('utf-8', errors='ignore')
    except:
        return ""

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
            res = session.get(url, timeout=10, headers={'User-Agent': 'clash-verge/v2.0.2'}, allow_redirects=True)
            content = res.text
            real_url = res.url
            
            found_nodes = []
            
            # 1. YAML 解析 (保留完整对象)
            if "proxies:" in content:
                try:
                    data = yaml.safe_load(content)
                    if isinstance(data, dict) and 'proxies' in data:
                        for p in data['proxies']:
                            p['source_url'] = real_url # 添加溯源
                            found_nodes.append(p)
                except: pass
            
            # 2. URI 解析 (Base64 或 纯文本)
            if not re.fullmatch(r'[A-Za-z0-9+/=\r\n]+', content.strip()):
                uris = NODE_URI_REGEX.findall(content)
                found_nodes.extend([{'type': 'uri', 'uri': u, 'source_url': real_url} for u in uris])
            else:
                decoded = safe_b64_decode(content)
                uris = NODE_URI_REGEX.findall(decoded)
                found_nodes.extend([{'type': 'uri', 'uri': u, 'source_url': real_url} for u in uris])
            
            # 3. 去重与存储
            if found_nodes:
                with lock:
                    for n in found_nodes:
                        # 以 uuid 或 uri 作为唯一性判断
                        key = n.get('uuid') or n.get('uri') or str(n)
                        if key not in all_nodes_dict:
                            all_nodes_dict[key] = n
            
            # 4. 递归发现
            if len(processed_urls) < MAX_URLS:
                new_links = LINK_REGEX.findall(content)
                new_links.append(real_url)
                with lock:
                    for link in set(new_links):
                        if any(k in link for k in ['sub', 'subscribe', 'proxy', 'raw.githubusercontent.com']) and link not in processed_urls:
                            processed_urls.add(link)
                            url_queue.put(link)
        except Exception as e:
            logger.debug(f"解析 {url} 失败: {e}")
        finally:
            url_queue.task_done()

# ======================
# 主程序
# ======================
if __name__ == '__main__':
    with open('latest.yaml', 'r', encoding="utf-8") as f:
        for urls in yaml.safe_load(f).values():
            for url in urls:
                if url not in processed_urls:
                    processed_urls.add(url)
                    url_queue.put(url)

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(WORKER_THREADS)]
    for t in threads: t.start()
    url_queue.join()
    
    with open('all_nodes.yaml', 'w', encoding="utf-8") as f:
        yaml.dump(list(all_nodes_dict.values()), f, allow_unicode=True)
    
    logger.info(f"解析完成，共获取唯一节点: {len(all_nodes_dict)}")
