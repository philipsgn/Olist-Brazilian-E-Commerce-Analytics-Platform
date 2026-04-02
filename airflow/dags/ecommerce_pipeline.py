"""
=============================================================================
FILE: airflow/dags/ecommerce_pipeline.py
=============================================================================
DAG: ecommerce_daily_pipeline
Author: Data Engineering Team

WHAT IS A DAG?
--------------
DAG = Directed Acyclic Graph.
- "Directed"  → tasks chạy theo thứ tự nhất định (có chiều)
- "Acyclic"   → không có vòng lặp (task A không thể chờ task B nếu B đang chờ A)
- "Graph"     → tập hợp các tasks được kết nối với nhau

Hãy tưởng tượng DAG như một công thức nấu ăn:
Step 1: Rửa rau → Step 2: Thái rau → Step 3: Xào → Step 4: Dọn lên đĩa
Bạn không thể xào trước khi rửa rau!

PIPELINE OVERVIEW:
-----------------
[load_csv_to_raw]
      ↓
[dbt_run_staging]    ← Transform raw → staging views
      ↓
[dbt_test_staging]   ← Kiểm tra data quality staging
      ↓
[dbt_run_marts]      ← Transform staging → mart tables
      ↓
[dbt_test_marts]     ← Kiểm tra data quality marts

SCHEDULE: Mỗi ngày lúc 6:00 AM (giờ UTC)

IMPORTANT - DOCKER ARCHITECTURE:
---------------------------------
Khi Airflow chạy trong Docker, mọi đường dẫn file phải là đường dẫn
TRONG container, không phải trên máy Windows của bạn!

Mapping (Windows host → Docker container):
  ./data/           → /opt/airflow/data/
  ./ingestion/      → /opt/airflow/ingestion/
  ./dbt_project/    → /opt/airflow/dbt_project/
=============================================================================
"""

from __future__ import annotations

# --- IMPORTS ---
# datetime: Python's built-in library để làm việc với ngày giờ
from datetime import datetime, timedelta

# pendulum: Library xử lý timezone tốt hơn datetime thuần
# Airflow khuyến nghị dùng pendulum thay vì datetime để tránh lỗi timezone
import pendulum

# Các class cốt lõi của Airflow
from airflow import DAG  # Class để định nghĩa 1 DAG

# Operators: "Loại công việc" mà Airflow có thể thực hiện
from airflow.operators.bash import BashOperator      # Chạy lệnh bash/shell
from airflow.operators.python import PythonOperator  # Chạy hàm Python

# =============================================================================
# SECTION 1: DEFAULT ARGUMENTS
# =============================================================================
# "default_args" là cấu hình mặc định áp dụng cho TẤT CẢ tasks trong DAG.
# Thay vì viết lại retry=2 cho từng task, ta khai báo 1 lần ở đây.
#
# Hãy nghĩ nó như "settings mặc định" của app điện thoại — bạn set 1 lần,
# tất cả app dùng chung.

default_args = {
    # "owner": Ai "sở hữu" DAG này. Dùng để filter trong Airflow UI.
    # Trong team lớn, mỗi team có owner riêng (data-eng, analytics, etc.)
    "owner": "data-engineering",

    # "depends_on_past": Liệu lần chạy hôm nay có phụ thuộc vào
    # lần chạy HÔM QUA không?
    # False = Không. Dù hôm qua fail, hôm nay vẫn chạy bình thường.
    # True  = Nếu hôm qua fail, hôm nay sẽ bị BLOCK, không chạy.
    # → Với daily data pipeline, False thường là lựa chọn đúng.
    "depends_on_past": False,

    # "email_on_failure": Gửi email khi task fail?
    # False vì ta chưa config SMTP server. Trong prod thực tế, set True.
    "email_on_failure": False,

    # "email_on_retry": Gửi email khi task đang retry?
    "email_on_retry": False,

    # "retries": Số lần thử lại khi task bị lỗi.
    # 2 = Thử lại 2 lần. Tổng cộng chạy tối đa 3 lần (1 lần đầu + 2 retry).
    # Tại sao cần retry? Network glitch, DB connection timeout, etc.
    "retries": 2,

    # "retry_delay": Chờ bao lâu giữa các lần retry?
    # timedelta(minutes=5) = Chờ 5 phút trước khi retry.
    # Không retry ngay lập tức vì nguyên nhân lỗi có thể là tạm thời
    # (VD: DB đang bận, chờ 5 phút thường tự giải quyết).
    "retry_delay": timedelta(minutes=5),

    # "execution_timeout": Nếu task chạy quá lâu, Airflow sẽ kill nó.
    # timedelta(hours=2) = Task được phép chạy tối đa 2 tiếng.
    # Điều này ngăn task "treo" vô hạn và block pipeline.
    "execution_timeout": timedelta(hours=2),
}

# =============================================================================
# SECTION 2: CONSTANTS - CÁC ĐƯỜNG DẪN QUAN TRỌNG
# =============================================================================
# CRITICAL: Đây là đường dẫn TRONG Docker container, không phải Windows!
#
# Tại sao? Khi Airflow worker thực thi task, nó chạy TRONG container.
# Container không biết gì về "C:\Users\TanPhat\..." trên máy bạn.
# Container chỉ thấy filesystem của nó, được mount từ docker-compose.yml.
#
# docker-compose.yml mount:
#   ./data          → /opt/airflow/data
#   ./ingestion     → /opt/airflow/ingestion
#   ./dbt_project   → /opt/airflow/dbt_project

# Đường dẫn đến thư mục dbt project TRONG container
DBT_PROJECT_DIR = "/opt/airflow/dbt_project/ecommerce"

# Đường dẫn đến profiles.yml của dbt trong container
# WHY /opt/airflow/dbt_project/profiles.yml?
# Trong container, user home thường không phải /root hoặc ~/.dbt.
# Ta sẽ để profiles.yml cạnh dbt_project và chỉ định rõ path.
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

# Đường dẫn đến script ingestion trong container
INGESTION_SCRIPT = "/opt/airflow/ingestion/load_csv.py"

# Connection string đến PostgreSQL.
# QUAN TRỌNG: Dùng hostname "postgres" (tên service trong docker-compose),
# KHÔNG phải "localhost" hay "127.0.0.1"!
#
# Tại sao? Vì Airflow và PostgreSQL đang chạy trong CÙNG Docker network.
# Trong Docker network, các container nói chuyện với nhau qua SERVICE NAME.
# "localhost" trong container = chính container đó, không phải Postgres!
DB_URI = "postgresql://de_user:de_password@postgres:5432/ecommerce_db"

# =============================================================================
# SECTION 3: CALLBACK FUNCTIONS - HÀM GỌI KHI CÓ SỰ KIỆN
# =============================================================================

def on_failure_callback(context: dict) -> None:
    """
    Hàm này được Airflow tự động gọi khi BẤT KỲ task nào trong DAG bị fail.
    
    Tham số "context" là một dict chứa thông tin về task đang bị lỗi:
    - context['task_instance']    → Thông tin về task instance
    - context['task_instance'].task_id → Tên task bị lỗi
    - context['exception']        → Exception gây ra lỗi
    - context['execution_date']   → Lần chạy nào bị lỗi (ngày nào)
    
    Trong production, hàm này thường gửi alert lên Slack/PagerDuty.
    Ta dùng print() để log ra Airflow logs (đủ cho beginner).
    """
    # Lấy thông tin từ context
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    execution_date = context.get("execution_date")

    # Log ra Airflow task logs. Lên Airflow UI → Task → Logs để xem.
    print("=" * 60)
    print("🚨 PIPELINE FAILURE ALERT 🚨")
    print(f"   DAG       : {task_instance.dag_id}")
    print(f"   Task      : {task_instance.task_id}")
    print(f"   Run date  : {execution_date}")
    print(f"   Error     : {exception}")
    print("=" * 60)
    print("ACTION NEEDED: Check logs above for details!")
    print("To retry manually: dbt run --select <model_name>")


# =============================================================================
# SECTION 4: PYTHON CALLABLE - HÀM ĐƯỢC PYTHON OPERATOR CHẠY
# =============================================================================

def run_load_csv(**kwargs) -> str:
    """
    Task 1: Load CSV files vào PostgreSQL raw schema.
    
    Hàm này sẽ:
    1. Import load_csv.py từ /opt/airflow/ingestion/
    2. Gọi hàm run_ingestion() đã được viết trong Phase 2
    
    **kwargs: Airflow truyền nhiều thông tin hữu ích vào Python callable
    qua keyword arguments. VD: kwargs['execution_date'] là ngày chạy.
    Ta đặt **kwargs để hàm nhận chúng dù không cần dùng.
    
    Returns: String mô tả kết quả (sẽ được lưu vào XCom trong Airflow)
    """
    import importlib.util  # Built-in Python tool để load file .py theo đường dẫn
    import os
    import sys

    # Lấy execution date từ Airflow context để log
    # "ds" = "datestamp" = ngày thực thi dưới dạng "YYYY-MM-DD"
    execution_date = kwargs.get("ds", "unknown")

    print(f"[{execution_date}] Starting CSV ingestion...")
    print(f"Script path: {INGESTION_SCRIPT}")

    # Đặt DB_URI đúng cho container (postgres:5432, không phải localhost:5433!)
    # os.environ cho phép ta override environment variable từ Python code.
    # Nhở rằng load_csv.py đọc DB_URI từ os.getenv("DB_URI", default).
    # Ta override nó ở đây để dùng hostname "postgres" của Docker.
    os.environ["DB_URI"] = DB_URI

    # Đặt DATA_DIR để load_csv.py biết tìm CSV ở đâu
    os.environ["DATA_DIR"] = "/opt/airflow/data"

    # --- Load module load_csv.py theo đường dẫn tuyệt đối ---
    # Cách này tốt hơn "import load_csv" vì ta biết chính xác file ở đâu,
    # không phụ thuộc vào Python PATH.
    spec = importlib.util.spec_from_file_location(
        "load_csv",          # Tên module (có thể đặt tùy ý)
        INGESTION_SCRIPT,    # Đường dẫn đến file .py
    )

    # Kiểm tra file có tồn tại không
    if spec is None:
        raise FileNotFoundError(
            f"Cannot find ingestion script at: {INGESTION_SCRIPT}\n"
            "Check that docker-compose.yml mounts ./ingestion → /opt/airflow/ingestion"
        )

    # Tạo module object từ spec
    module = importlib.util.module_from_spec(spec)

    # Nạp (execute) file Python vào bộ nhớ
    # Sau bước này, module.run_ingestion() có thể gọi được
    spec.loader.exec_module(module)

    # Thêm thư mục ingestion vào Python path để tránh import error
    ingestion_dir = str(INGESTION_SCRIPT.rsplit("/", 1)[0])
    if ingestion_dir not in sys.path:
        sys.path.insert(0, ingestion_dir)

    # Gọi hàm run_ingestion() từ load_csv.py (Phase 2 code!)
    print(f"[{execution_date}] Running ingestion for all 9 CSV files...")
    module.run_ingestion()

    success_msg = f"[{execution_date}] CSV ingestion completed successfully!"
    print(success_msg)
    return success_msg


# =============================================================================
# SECTION 5: DAG DEFINITION
# =============================================================================
# "with DAG(...) as dag:" là cú pháp Python context manager.
# Mọi task được tạo TRONG block "with" sẽ tự động thuộc về DAG này.
# Không cần gán dag=dag cho từng task.

with DAG(
    # dag_id: Tên duy nhất của DAG. Hiển thị trong Airflow UI.
    # Quy tắc đặt tên: lowercase, chỉ dùng chữ, số, dấu gạch dưới/gạch ngang.
    dag_id="ecommerce_daily_pipeline",

    # description: Mô tả ngắn. Hiển thị trong Airflow UI khi hover vào DAG.
    description=(
        "Daily E-commerce pipeline: "
        "CSV ingestion → dbt staging → dbt marts"
    ),

    # default_args: Áp dụng args đã định nghĩa ở Section 1
    default_args=default_args,

    # schedule: Khi nào DAG được chạy?
    # "0 6 * * *" là cron expression:
    #   0  = phút 0
    #   6  = 6 giờ (UTC)
    #   *  = mọi ngày trong tháng
    #   *  = mọi tháng
    #   *  = mọi ngày trong tuần
    # → "Chạy lúc 6:00 AM UTC mỗi ngày"
    # Lưu ý: UTC+7 = Vietnam, nên 6:00 AM UTC = 1:00 PM Vietnam
    # Nếu muốn chạy lúc 6 AM Vietnam → đổi thành "0 23 * * *" (23:00 UTC hôm trước)
    schedule="0 6 * * *",

    # start_date: Từ ngày nào Airflow bắt đầu tính lịch chạy?
    # pendulum.datetime(..., tz="UTC") đảm bảo timezone rõ ràng.
    # Luôn set start_date trong quá khứ để Airflow không bị nhầm lẫn.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),

    # catchup: Nếu True, Airflow sẽ "bắt kịp" tất cả lần chạy bị bỏ qua
    # từ start_date đến hiện tại → có thể tạo hàng trăm runs!
    # False = Chỉ chạy lần tiếp theo theo schedule, bỏ qua quá khứ.
    # Với pipeline mới, LUÔN set catchup=False.
    catchup=False,

    # tags: Nhãn để filter DAGs trong Airflow UI.
    # Hữu ích khi có nhiều DAGs trong 1 project lớn.
    tags=["ecommerce", "daily", "production"],

    # on_failure_callback: Hàm gọi khi DAG-level failure xảy ra
    on_failure_callback=on_failure_callback,

    # doc_md: Tài liệu Markdown hiển thị trong Airflow UI → DAG → Docs tab
    # Giúp đồng nghiệp hiểu DAG này làm gì mà không cần đọc code
    doc_md="""
## E-commerce Daily Pipeline

**Schedule**: 6:00 AM UTC daily (1:00 PM Vietnam time)

### Pipeline Steps:
1. **load_csv_to_raw**: Load 9 Kaggle CSV files → `raw` schema in PostgreSQL
2. **dbt_run_staging**: Create 9 staging views in `staging` schema  
3. **dbt_test_staging**: Run data quality tests on staging layer
4. **dbt_run_marts**: Create 9 mart tables in `marts` schema
5. **dbt_test_marts**: Run data quality tests on marts layer

### On Failure:
- Tasks retry 2 times with 5min delay
- Check task logs in Airflow UI → Task Instance → Log

### Stack:
- PostgreSQL 15 (Docker, port 5433 from host / port 5432 internal)
- dbt-core 1.11.7 with dbt-postgres 1.10.0
- Airflow 2.9.3 with CeleryExecutor
    """,
) as dag:

    # =========================================================================
    # TASK 1: Load CSV → PostgreSQL raw schema
    # =========================================================================
    # PythonOperator: Chạy một hàm Python thuần.
    # Đây là cách đúng để gọi code Python có logic phức tạp.
    #
    # Tại sao không dùng BashOperator để gọi "python load_csv.py"?
    # Vì BashOperator chạy trong subprocess mới, khó truyền biến môi trường
    # và khó handle lỗi. PythonOperator cho phép raise Exception rõ ràng hơn.
    task_load_csv = PythonOperator(
        # task_id: Định danh duy nhất của task TRONG dag này.
        # Hiển thị trong Airflow UI. Đặt tên mô tả hành động.
        task_id="load_csv_to_raw",

        # python_callable: Hàm Python sẽ được gọi khi task chạy.
        # Chú ý: Truyền tên hàm (không có dấu ngoặc!), không phải gọi hàm.
        # ✅ python_callable=run_load_csv     (đúng - truyền hàm)
        # ❌ python_callable=run_load_csv()   (sai - gọi hàm ngay lập tức)
        python_callable=run_load_csv,

        # op_kwargs: Keyword arguments bổ sung truyền vào python_callable.
        # Các args này sẽ được merge vào **kwargs trong hàm.
        # Ta để trống vì đã dùng **kwargs để lấy từ Airflow context.
        op_kwargs={},
    )

    # =========================================================================
    # TASK 2: dbt run staging models
    # =========================================================================
    # BashOperator: Chạy lệnh bash/shell command.
    # Phù hợp để gọi CLI tools như dbt.
    #
    # LỆnh dbt đầy đủ giải thích:
    #   dbt run                    → Chạy các models dbt
    #   --select staging.*         → Chỉ chạy models TRONG thư mục staging/
    #   --project-dir ...          → Đường dẫn đến dbt_project.yml
    #   --profiles-dir ...         → Đường dẫn đến profiles.yml
    #   --no-partial-parse         → Luôn parse lại từ đầu (tránh cache bugs)
    task_dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",

        # bash_command: Lệnh sẽ được chạy trong bash shell.
        # Dùng triple-quote """...""" để viết lệnh dài nhiều dòng dễ đọc.
        # Kết thúc mỗi dòng với \ để nối dòng trong bash.
        bash_command=f"""
            echo "=== [Task 2] Running dbt for STAGING models ===" &&
            dbt run \\
                --select staging.* \\
                --project-dir {DBT_PROJECT_DIR} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --no-partial-parse \\
                --target dev
        """,

        # env: Biến môi trường cho bash process này.
        # Quan trọng: Nếu không set, bash có thể không có PATH đúng.
        env={
            # PATH đảm bảo bash tìm thấy lệnh "dbt"
            # /home/airflow/.local/bin là nơi pip install dbt-core đặt binary
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        # append_env=True: Thêm env ở trên vào environment hiện có,
        # không thay thế hoàn toàn. Giúp không mất các biến ENV của container.
        append_env=True,
    )

    # =========================================================================
    # TASK 3: dbt test staging models
    # =========================================================================
    # Sau khi staging models được tạo, ta kiểm tra data quality.
    # dbt test chạy các tests được định nghĩa trong schema.yml của staging.
    # Nếu test fail → task fail → DAG dừng → mart models KHÔNG được chạy.
    # Đây là "data quality gate" — bảo vệ marts khỏi data xấu!
    task_dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
            echo "=== [Task 3] Testing STAGING data quality ===" &&
            dbt test \\
                --select staging.* \\
                --project-dir {DBT_PROJECT_DIR} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --no-partial-parse
        """,
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    # =========================================================================
    # TASK 4: dbt run mart models
    # =========================================================================
    # Bây giờ data staging đã sạch, ta chạy mart models.
    # Marts = Business Intelligence tables (fact tables, dim tables).
    # Analyst và Superset sẽ query từ đây.
    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
            echo "=== [Task 4] Running dbt for MARTS models ===" &&
            dbt run \\
                --select marts.* \\
                --project-dir {DBT_PROJECT_DIR} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --no-partial-parse \\
                --target dev
        """,
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    # =========================================================================
    # TASK 5: dbt test mart models
    # =========================================================================
    # Kiểm tra lần cuối data ở marts layer.
    # Sau khi pass, data đã sẵn sàng cho Superset dashboard!
    task_dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"""
            echo "=== [Task 5] Testing MARTS data quality ===" &&
            dbt test \\
                --select marts.* \\
                --project-dir {DBT_PROJECT_DIR} \\
                --profiles-dir {DBT_PROFILES_DIR} \\
                --no-partial-parse &&
            echo "=== ✅ Pipeline completed successfully! ==="
        """,
        env={
            "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
        },
        append_env=True,
    )

    # =========================================================================
    # TASK DEPENDENCIES - KẾT NỐI CÁC TASKS
    # =========================================================================
    # Cú pháp ">>" có nghĩa là "task bên trái phải XONG trước task bên phải"
    # Đây gọi là "bitshift operator" trong Airflow.
    #
    # Đọc như sau:
    #   task_a >> task_b  =  "task_a rồi mới task_b"
    #   task_a >> [task_b, task_c]  =  "task_a xong, rồi b VÀ c chạy song song"
    #
    # Pipeline của ta là LINEAR (tuần tự hoàn toàn):
    #   load_csv → dbt_run_staging → dbt_test_staging → dbt_run_marts → dbt_test_marts
    #
    # Nếu bất kỳ task nào fail (sau hết retries), các task sau sẽ bị SKIP.
    # Đây là cơ chế "fail-fast" bảo vệ downstream từ bad data.

    (
        task_load_csv
        >> task_dbt_run_staging
        >> task_dbt_test_staging
        >> task_dbt_run_marts
        >> task_dbt_test_marts
    )
