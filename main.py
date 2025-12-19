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
        
        # 分批写入
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            payload = {"records": [{"fields": r} for r in batch]}
            try:
                resp = requests.post(url, headers=headers, json=payload)
                resp_json = resp.json()
                if resp_json.get("code") == 0:
                    total_added += len(batch)
                else:
                    # 打印详细错误信息帮助调试
                    print(f"⚠️ 写入失败 (Batch {i}): {resp_json}")
                    # 如果是日志表写入失败，抛出异常以便外层捕获
                    if table_id == LOG_TABLE_ID:
                        raise Exception(f"飞书返回错误: {resp_json}")
            except Exception as e:
                print(f"❌ 写入请求错误: {e}")
                if table_id == LOG_TABLE_ID:
                    raise e
        return total_added

    def delete_oldest_day(self, table_id, date_field_name="下单时间"):
        """查找并删除最早一天的数据"""
        # 1. 查找最早的记录
        url_list = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {"sort": f'["{date_field_name} ASC"]', "page_size": 100}
        
        resp = requests.get(url_list, headers=headers, params=params)
        data = resp.json().get("data", {}).get("items", [])
        
        if not data:
            return "无数据", 0

        oldest_date_val = data[0]["fields"].get(date_field_name, "未知日期")
        records_to_delete = [item["record_id"] for item in data]
        
        if records_to_delete:
            url_del = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_delete"
            resp = requests.post(url_del, headers=headers, json={"records": records_to_delete})
            if resp.json().get("code") != 0:
                print(f"⚠️ 删除失败: {resp.json()}")
        
        return oldest_date_val, len(records_to_delete)

    def log_result(self, status, added, deleted_info, deleted_count, error=""):
        """将运行结果写入日志表"""
        beijing_time = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        
        # 确保 deleted_info 是字符串，防止 None 报错
        if deleted_info is None:
            deleted_info = "无"
            
        fields = {
            "运行时间": beijing_time, 
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

        # 【核心修复 0】: 字段重命名，避开系统字段冲突
        # 将Excel里的 "创建时间" 改名为 "下单时间"
        print("🔧 正在重命名冲突字段...")
        df.rename(columns={'创建时间': '下单时间'}, inplace=True)

        # 【核心修复 1】: 强制指定哪些列是日期
        # 注意：这里必须使用重命名后的 "下单时间"
        date_columns = ["下单时间", "出货时间", "打印时间"]
        
        print(f"⏳ 正在强制转换日期列: {date_columns} ...")
        for col in date_columns:
            if col in df.columns:
                # errors='coerce' 意思是：如果遇到转不了的乱码，就设为 NaT (空时间)
                df[col] = pd.to_datetime(df[col], errors='coerce')

        records = df.to_dict(orient="records")
        print(f"📊 解析到 {len(records)} 条数据")

        # 【核心修复 2】: 再次清洗，将所有 Pandas 时间对象转为飞书时间戳，处理空值
        for r in records:
            for k, v in r.items():
                # 先处理空值 (NaN, NaT, None) -> 设为 None
                if pd.isna(v):
                    r[k] = None
                    continue
                
                # 再处理时间戳
                if isinstance(v, (pd.Timestamp, datetime)):
                    try:
                        r[k] = int(v.timestamp() * 1000)
                    except:
                        r[k] = None # 如果时间错乱，设为空，保命要紧

        # 3. 写入飞书
        added_count = 0
        if records:
            print("☁️ 正在上传到飞书...")
            added_count = bot.add_records(DATA_TABLE_ID, records)
        else:
            print("⚠️ 没下载到数据，跳过上传")
        
        # 4. 清理旧数据
        # 注意：这里使用新的字段名 "下单时间" 进行排序删除
        print("🗑️ 正在清理旧数据...")
        del_info, del_count = bot.delete_oldest_day(DATA_TABLE_ID, date_field_name="下单时间")
        
        # 5. 记录成功日志
        bot.log_result("成功", added_count, del_info, del_count)
        print("🎉 任务全部完成！")
        
    except Exception as e:
        print(f"❌ 任务出错: {e}")
        bot.log_result("失败", 0, "无", 0, str(e))
        raise e
