import os
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from playwright.sync_api import sync_playwright

# ================= 配置区域 (变量来自 GitHub Secrets) =================
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET")
DATA_TABLE_ID = os.environ.get("DATA_TABLE_ID")  # 主数据表ID
LOG_TABLE_ID = os.environ.get("LOG_TABLE_ID")    # 日志表ID
APP_TOKEN = os.environ.get("APP_TOKEN")          # 多维表格App Token
WEB_USER = os.environ.get("WEB_USER")            # 网站账号
WEB_PASS = os.environ.get("WEB_PASS")            # 网站密码

# ================= 飞书 API 工具类 =================
class FeishuBot:
    def __init__(self):
        self.token = self.get_tenant_access_token()
    
    def get_tenant_access_token(self):
        """获取飞书 Tenant Access Token"""
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        try:
            resp = requests.post(url, json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET})
            resp.raise_for_status()
            return resp.json().get("tenant_access_token")
        except Exception as e:
            print(f"❌ 获取飞书Token失败: {e}")
            raise

    def add_records(self, table_id, records):
        """批量写入数据到多维表格"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_create"
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        
        batch_size = 100
        total_added = 0
        
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            payload = {"records": [{"fields": r} for r in batch]}
            try:
                resp = requests.post(url, headers=headers, json=payload)
                resp_json = resp.json()
                code = resp_json.get("code")
                
                if code == 0:
                    total_added += len(batch)
                else:
                    msg = resp_json.get("msg", "")
                    print(f"⚠️ 写入失败 (Batch {i}): Code {code} - {msg}")
                    if code == 1254045:
                        print("👉 原因分析：【列名不匹配】。请检查飞书表格里是否缺了某个列，或者列名写错了。")
                    elif code == 1254302:
                        print("👉 原因分析：【权限拒绝】。可能是试图写入'系统字段'，或者应用没发布版本。")
                    
                    if table_id == LOG_TABLE_ID:
                        raise Exception(f"飞书返回错误: {resp_json}")
            except Exception as e:
                print(f"❌ 写入请求错误: {e}")
                if table_id == LOG_TABLE_ID:
                    raise e
        return total_added

    def delete_oldest_day(self, table_id, date_field_name="下单时间"):
        """查找并删除最早一天(整天)的所有数据"""
        print("🔍 正在检查是否有旧数据需要清理...")
        
        # 1. 查找最早的一条记录，确定"最早日期"
        url_list = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        # 只取一条，用来定锚点
        params_sort = {"sort": f'["{date_field_name} ASC"]', "page_size": 1}
        
        try:
            resp = requests.get(url_list, headers=headers, params=params_sort)
            data = resp.json().get("data", {}).get("items", [])
        except Exception as e:
            print(f"⚠️ 无法获取旧数据(可能列名不对): {e}")
            return "获取失败", 0
        
        if not data:
            print("✅ 表格是空的，无需删除。")
            return "无数据", 0

        # 获取最早的时间戳
        oldest_ts = data[0]["fields"].get(date_field_name)
        if not isinstance(oldest_ts, (int, float)):
             print(f"⚠️ 最早的一条数据日期格式不对({oldest_ts})，跳过删除。")
             return "格式错误", 0

        # 计算当天的 00:00:00 和 23:59:59 时间戳
        dt = datetime.fromtimestamp(oldest_ts / 1000)
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        ts_start = int(day_start.timestamp() * 1000)
        ts_end = int(day_end.timestamp() * 1000)
        
        date_str = day_start.strftime("%Y-%m-%d")
        print(f"🗑️ 锁定最早日期: {date_str}，正在搜索该天所有数据...")

        # 2. 使用 filter 搜索该时间范围内的所有数据
        # 语法: AND(CurrentValue.[下单时间]>=ts_start, CurrentValue.[下单时间]<=ts_end)
        filter_str = f'AND(CurrentValue.[{date_field_name}]>={ts_start},CurrentValue.[{date_field_name}]<={ts_end})'
        
        # 设置 page_size 为 500 (飞书单次查询上限)，如果不止500条可能需要循环，但对于一天的数据通常够了
        params_filter = {"filter": filter_str, "page_size": 500}
        
        resp_filter = requests.get(url_list, headers=headers, params=params_filter)
        items_to_delete = resp_filter.json().get("data", {}).get("items", [])
        
        if not items_to_delete:
            return f"{date_str} (未找到)", 0

        # 3. 批量删除
        record_ids = [item["record_id"] for item in items_to_delete]
        print(f"👋 找到 {len(record_ids)} 条数据属于 {date_str}，准备全部删除...")
        
        total_deleted = 0
        batch_size = 100
        url_del = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_delete"
        
        # 分批删除 (每次100条)
        for i in range(0, len(record_ids), batch_size):
            batch_ids = record_ids[i:i+batch_size]
            resp_del = requests.post(url_del, headers=headers, json={"records": batch_ids})
            if resp_del.json().get("code") == 0:
                total_deleted += len(batch_ids)
            else:
                print(f"⚠️ 删除失败: {resp_del.json()}")

        print(f"🗑️ 已删除 {date_str} 的 {total_deleted} 条记录。")
        return date_str, total_deleted

    def log_result(self, status, added, deleted_info, deleted_count, error=""):
        """将运行结果写入日志表"""
        if deleted_info is None:
            deleted_info = "无"
            
        fields = {
            "执行状态": status,
            "新增条数": added,
            "删除日期": str(deleted_info),
            "删除条数": deleted_count,
            "错误详情": str(error)
        }
        try:
            print(f"📋 准备写入日志: {fields}")
            self.add_records(LOG_TABLE_ID, [fields])
            print("✅ 日志已记录")
        except Exception as e:
            print(f"❌ 日志写入失败! 原因: {e}")

# ================= 浏览器自动化 =================
def download_excel_from_web():
    """使用 Playwright 模拟下载"""
    # 设定时间逻辑：获取昨天的数据
    yesterday = datetime.now() - timedelta(days=1)
    start_str = yesterday.strftime("%Y-%m-%d 00:00:00")
    end_str = yesterday.strftime("%Y-%m-%d 23:59:59")
    
    print(f"📅 准备下载数据区间: {start_str} 到 {end_str}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--safebrowsing-disable-download-protection", "--allow-running-insecure-content", "--disable-web-security"]
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        try:
            print("🔄 正在打开网页...")
            page.goto("http://111.230.72.108:8082/orderQuery.htm")
            print("🔑 正在登录...")
            page.fill('#username', WEB_USER)       
            page.fill('#inputPassword', WEB_PASS)  
            page.click('input[value="登 录"]')
            
            print("⏳ 等待页面跳转...")
            menu_selector = 'text=全部订单/导出 >> visible=true'
            page.wait_for_selector(menu_selector, timeout=60000)
            print("📂 进入导出页面...")
            page.click(menu_selector) 
            
            print("⏳ 等待日期输入框加载...")
            page.wait_for_selector('#sTime', state='attached', timeout=30000)
            
            print("📅 正在设置日期...")
            js_script = f"""
                document.getElementById('sTime').removeAttribute('readonly');
                document.getElementById('sTime').value = '{start_str}';
                document.getElementById('eTime').removeAttribute('readonly');
                document.getElementById('eTime').value = '{end_str}';
            """
            page.evaluate(js_script)
            
            print("⬇️ 点击下载...")
            with page.expect_download() as download_info:
                page.click('button:has-text("下载")')
            
            download = download_info.value
            save_path = os.path.join(os.getcwd(), "result.xls")
            download.save_as(save_path)
            
            browser.close()
            print(f"✅ 下载完成: {save_path}")
            return save_path
            
        except Exception as e:
            browser.close()
            raise e

# ================= 主流程 =================
if __name__ == "__main__":
    bot = FeishuBot()
    try:
        print("🚀 任务开始...")
        
        file_path = download_excel_from_web()
        
        print("📖 正在解析 Excel...")
        df = pd.read_excel(file_path, header=0, engine='xlrd') 
        df.dropna(how='all', inplace=True)

        print("🔧 正在重命名冲突字段...")
        df.rename(columns={'创建时间': '下单时间'}, inplace=True)

        date_columns = ["下单时间", "出货时间", "打印时间"]
        
        print(f"⏳ 正在强制转换日期列: {date_columns} ...")
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        records = df.to_dict(orient="records")
        print(f"📊 解析到 {len(records)} 条数据")

        for r in records:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None
                    continue
                if isinstance(v, (pd.Timestamp, datetime)):
                    try:
                        r[k] = int(v.timestamp() * 1000)
                    except:
                        r[k] = None

        # 3. 写入飞书
        added_count = 0
        if records:
            print("☁️ 正在上传到飞书...")
            added_count = bot.add_records(DATA_TABLE_ID, records)
        else:
            print("⚠️ 没下载到数据，跳过上传")
        
        # 4. 清理旧数据 (这里已重新开启)
        print("🗑️ 准备执行旧数据清理...")
        # 即使没上传新数据，也会检查并清理最老的一天，保持数据量平衡
        del_info, del_count = bot.delete_oldest_day(DATA_TABLE_ID, date_field_name="下单时间")
        
        # 5. 记录成功日志
        bot.log_result("成功", added_count, del_info, del_count)
        print("🎉 任务全部完成！")
        
    except Exception as e:
        print(f"❌ 任务出错: {e}")
        bot.log_result("失败", 0, "无", 0, str(e))
        raise e
