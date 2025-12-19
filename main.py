import os
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
from playwright.sync_api import sync_playwright

# ================= 配置区域 (变量来自 GitHub Secrets) =================
# 务必确保在 GitHub 仓库的 Secrets 中配置了以下 7 个变量
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
        
        # 数据清洗：飞书不支持 NaN (空值)，必须转换成 None
        for r in records:
            for k, v in r.items():
                if pd.isna(v):
                    r[k] = None

        # 分批写入，每次100条
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            payload = {"records": [{"fields": r} for r in batch]}
            try:
                resp = requests.post(url, headers=headers, json=payload)
                if resp.json().get("code") == 0:
                    total_added += len(batch)
                else:
                    print(f"⚠️ 写入部分失败: {resp.json().get('msg')}")
            except Exception as e:
                print(f"❌ 写入请求错误: {e}")
        return total_added

    def delete_oldest_day(self, table_id, date_field_name="创建时间"):
        """查找并删除最早一天的数据"""
        # 1. 查找最早的记录
        url_list = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records"
        headers = {"Authorization": f"Bearer {self.token}"}
        # 按创建时间升序，取前100条
        params = {"sort": f'["{date_field_name} ASC"]', "page_size": 100}
        
        resp = requests.get(url_list, headers=headers, params=params)
        data = resp.json().get("data", {}).get("items", [])
        
        if not data:
            return "无数据", 0

        # 获取最早那条数据的日期（用于日志记录）
        oldest_date_val = data[0]["fields"].get(date_field_name, "未知日期")
            
        # 提取记录ID进行删除
        records_to_delete = [item["record_id"] for item in data]
        
        if records_to_delete:
            url_del = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{table_id}/records/batch_delete"
            requests.post(url_del, headers=headers, json={"records": records_to_delete})
        
        return oldest_date_val, len(records_to_delete)

    def log_result(self, status, added, deleted_info, deleted_count, error=""):
        """将运行结果写入日志表"""
        # 获取当前北京时间 (UTC+8)
        beijing_time = (datetime.utcnow() + timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S")
        fields = {
            "运行时间": beijing_time, 
            "执行状态": status,
            "新增条数": added,
            "删除日期": str(deleted_info),
            "删除条数": deleted_count,
            "错误详情": str(error)
        }
        try:
            self.add_records(LOG_TABLE_ID, [fields])
            print("✅ 日志已记录")
        except:
            print("❌ 日志写入失败")

# ================= 浏览器自动化 =================
def download_excel_from_web():
    """使用 Playwright 模拟下载，并绕过 HTTP 安全拦截"""
    
    # 设定时间逻辑：获取昨天的数据
    yesterday = datetime.now() - timedelta(days=1)
    start_str = yesterday.strftime("%Y-%m-%d 00:00:00")
    end_str = yesterday.strftime("%Y-%m-%d 23:59:59")
    
    print(f"📅 准备下载数据区间: {start_str} 到 {end_str}")

    with sync_playwright() as p:
        # 启动浏览器，配置参数以绕过下载拦截
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--safebrowsing-disable-download-protection", # 关键：禁用下载保护
                "--allow-running-insecure-content",           # 关键：允许不安全内容
                "--disable-web-security"
            ]
        )
        # 创建上下文，自动接受下载
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        
        try:
            # 1. 打开登录页
            print("🔄 正在打开网页...")
            page.goto("http://111.230.72.108:8082/orderQuery.htm")
            
            # 2. 登录操作
            print("🔑 正在登录...")
            page.fill('#username', WEB_USER)       
            page.fill('#inputPassword', WEB_PASS)  
            page.click('input[value="登 录"]')
            
            # 等待"全部订单/导出"文字出现，确保登录成功
            # 修改：增加超时时间到60秒，并强制匹配可见元素 (visible=true)，防止匹配到隐藏的移动端菜单
            print("⏳ 等待页面跳转...")
            menu_selector = 'text=全部订单/导出 >> visible=true'
            page.wait_for_selector(menu_selector, timeout=60000)
            
            # 3. 导航到导出页面
            print("📂 进入导出页面...")
            page.click(menu_selector) 
            
            # 4. 强制填入日期 (使用 JS 移除 readonly 属性并赋值)
            print("📅 正在设置日期...")
            js_script = f"""
                document.getElementById('sTime').removeAttribute('readonly');
                document.getElementById('sTime').value = '{start_str}';
                document.getElementById('eTime').removeAttribute('readonly');
                document.getElementById('eTime').value = '{end_str}';
            """
            page.evaluate(js_script)
            
            # 5. 点击下载
            print("⬇️ 点击下载...")
            with page.expect_download() as download_info:
                # 寻找包含"下载"文字的按钮
                page.click('button:has-text("下载")')
            
            download = download_info.value
            # 保存为 result.xls
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
        
        # 1. 爬取数据
        file_path = download_excel_from_web()
        
        # 2. 解析 Excel
        print("📖 正在解析 Excel...")
        # engine='xlrd' 是必须的，因为是 .xls 格式
        df = pd.read_excel(file_path, header=0, engine='xlrd') 
        
        # 去除空行
        df.dropna(how='all', inplace=True)
        
        # 转为字典
        records = df.to_dict(orient="records")
        print(f"📊 解析到 {len(records)} 条数据")
        
        # 3. 写入飞书
        added_count = 0
        if records:
            print("☁️ 正在上传到飞书...")
            added_count = bot.add_records(DATA_TABLE_ID, records)
        else:
            print("⚠️ 没下载到数据，跳过上传")
        
        # 4. 清理旧数据
        print("🗑️ 正在清理旧数据...")
        del_info, del_count = bot.delete_oldest_day(DATA_TABLE_ID)
        
        # 5. 记录成功日志
        bot.log_result("成功", added_count, del_info, del_count)
        print("🎉 任务全部完成！")
        
    except Exception as e:
        print(f"❌ 任务出错: {e}")
        # 记录失败日志
        bot.log_result("失败", 0, "无", 0, str(e))
        # 抛出异常，确保 GitHub Actions 显示红色失败
        raise e
