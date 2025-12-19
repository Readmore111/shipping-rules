import os
import time
from datetime import datetime, timedelta, timezone
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
                        print("👉 原因分析：【权限拒绝】。【极其重要】请检查该表的列类型！不要往'自动生成'的系统字段(如创建时间/创建人)里写数据！日志表所有列建议都设为'文本'类型。")
                    
                    if table_id == LOG_TABLE_ID:
                        raise Exception(f"飞书返回错误: {resp_json}")
            except Exception as e:
                print(f"❌ 写入请求错误: {e}")
                if table_id == LOG_TABLE_ID:
                    raise e
        return total_added

    def delete_oldest_day(self, table_id, date_field_name="创建时间"):
        """查找并删除最早一天(整天)的所有数据 (Python内存过滤版 - 更精准)"""
        print(f"🔍 正在按照字段[{date_field_name}]查找最早的数据...")
        
        url_list = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        
        # 1. 既然 filter 容易有时区误差，我们直接把最早的 500 条数据全抓回来
        # 在 Python 内存里比对日期，这样绝对不会错
        params = {
            "sort": f'["{date_field_name} ASC"]', 
            "page_size": 500  # 一次抓500条来检查，通常够删一天的数据了
        }
        
        try:
            resp = requests.get(url_list, headers=headers, params=params)
            data = resp.json().get("data", {}).get("items", [])
        except Exception as e:
            print(f"⚠️ 无法获取旧数据: {e}")
            return "获取失败", 0
        
        if not data:
            print("✅ 表格是空的，无需删除。")
            return "无数据", 0

        # 2. 确定"最早的一天"是哪天
        first_item_ts = data[0]["fields"].get(date_field_name)
        if not isinstance(first_item_ts, (int, float)):
             print(f"⚠️ 第一条数据日期格式不对({first_item_ts})，跳过删除。")
             return "格式错误", 0

        # 转为北京时间字符串 (例如 "2025-05-14")
        utc_dt = datetime.fromtimestamp(first_item_ts / 1000, tz=timezone.utc)
        bj_dt = utc_dt.astimezone(timezone(timedelta(hours=8)))
        target_date_str = bj_dt.strftime("%Y-%m-%d")
        
        print(f"🗑️ 锁定最早日期(北京时间): {target_date_str}，正在筛选该天数据...")

        # 3. 在内存里循环遍历，挑出属于这一天的数据 ID
        ids_to_delete = []
        for item in data:
            ts = item["fields"].get(date_field_name)
            if isinstance(ts, (int, float)):
                # 同样转为北京时间进行比对
                item_utc = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
                item_bj = item_utc.astimezone(timezone(timedelta(hours=8)))
                item_date_str = item_bj.strftime("%Y-%m-%d")
                
                # 如果日期一致，就加入删除名单
                if item_date_str == target_date_str:
                    ids_to_delete.append(item["record_id"])
        
        if not ids_to_delete:
            print("⚠️ 奇怪，逻辑上应该有数据但没匹配到，跳过。")
            return f"{target_date_str} (未匹配)", 0

        print(f"👋 在前500条中，找到 {len(ids_to_delete)} 条属于 {target_date_str} 的数据，准备删除...")

        # 4. 批量删除
        total_deleted = 0
        batch_size = 100
        url_del = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_delete"
        
        for i in range(0, len(ids_to_delete), batch_size):
            batch_ids = ids_to_delete[i:i+batch_size]
            resp_del = requests.post(url_del, headers=headers, json={"records": batch_ids})
            if resp_del.json().get("code") == 0:
                total_deleted += len(batch_ids)
            else:
                print(f"⚠️ 删除失败: {resp_del.json()}")

        print(f"🗑️ 已删除 {target_date_str} 的 {total_deleted} 条记录。")
        return target_date_str, total_deleted

    def log_result(self, status, added, deleted_info, deleted_count, error=""):
        """将运行结果写入日志表"""
        if deleted_info is None:
            deleted_info = "无"
            
        # 【重要修复】你将日志表改成了全文本列，所以这里必须把数字转为字符串 (str)
        # 否则会报 TextFieldConvFail
        fields = {
            "执行状态": str(status),
            "新增条数": str(added),           # 修复点：转字符串
            "删除日期": str(deleted_info),    # 修复点：转字符串
            "删除条数": str(deleted_count),   # 修复点：转字符串
            "错误详情": str(error)
        }
        try:
            print(f"📋 准备写入日志: {fields}")
            self.add_records(LOG_TABLE_ID, [fields])
            print("✅ 日志已记录")
        except Exception as e:
            # 捕获异常，防止因为日志写不进去导致整个任务显示红色失败
            print(f"❌ 日志写入失败 (仅日志跳过): {e}")

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

        # 【已恢复】不再重命名为"下单时间"，直接使用原始的"创建时间"
        # 请确保飞书里的"创建时间"列是【非系统字段】的普通日期类型
        # print("🔧 正在重命名冲突字段...")
        # df.rename(columns={'创建时间': '下单时间'}, inplace=True)

        # 强制指定哪些列是日期
        date_columns = ["创建时间", "出货时间", "打印时间"]
        
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
        
        # 4. 清理旧数据
        print("🗑️ 准备执行旧数据清理...")
        # 恢复使用 "创建时间" 进行排序删除
        del_info, del_count = bot.delete_oldest_day(DATA_TABLE_ID, date_field_name="创建时间")
        
        # 5. 记录成功日志
        bot.log_result("成功", added_count, del_info, del_count)
        print("🎉 任务全部完成！")
        
    except Exception as e:
        print(f"❌ 任务出错: {e}")
        bot.log_result("失败", 0, "无", 0, str(e))
        raise e
