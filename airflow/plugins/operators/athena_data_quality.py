import logging
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults

class AthenaDataQualityOperator(BaseOperator):
    """
    Operator thực hiện kiểm tra Data Quality trên AWS Athena theo tiêu chuẩn Production.
    Nếu có bất kỳ rule nào vi phạm, task sẽ Fail và raise AirflowException.
    """
    
    @apply_defaults
    def __init__(
        self,
        table_name: str,
        database: str,
        aws_conn_id: str = 'aws_default',
        workgroup: str = 'primary',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.database = database
        self.aws_conn_id = aws_conn_id
        self.workgroup = workgroup

    def execute(self, context):
        from airflow.providers.amazon.aws.hooks.athena import AthenaHook
        hook = AthenaHook(aws_conn_id=self.aws_conn_id)
        
        # 3 Bài kiểm tra chuẩn như Siêu Prompt yêu cầu
        # Đã sửa lại Uniqueness: Kiểm tra xem có order_id nào bị NULL (trống) không
        dq_queries = {
            "Uniqueness": f"""
                SELECT COUNT(*) AS bad_count 
                FROM {self.database}.{self.table_name}
                WHERE order_id IS NULL
            """,
            "Completeness": f"""
                SELECT COUNT(*) AS bad_count 
                FROM {self.database}.{self.table_name} 
                WHERE price IS NULL OR customer_state IS NULL
            """,
            "Validity": f"""
                SELECT COUNT(*) AS bad_count 
                FROM {self.database}.{self.table_name} 
                WHERE total_amount <= 0
            """
        }

        failed_checks = []

        for check_name, query in dq_queries.items():
            self.log.info(f"🚀 Running DQ Check: {check_name}")
            
            try:
                # Chạy query trên Athena
                query_execution_id = hook.run_query(
                    query=query,
                    query_context={"Database": self.database},
                    result_configuration={"OutputLocation": "s3://olist-de-tanphat-2026/athena-results/"},
                    workgroup=self.workgroup
                )
                
                # Đợi và lấy kết quả
                status = hook.poll_query_status(query_execution_id)
                if status != 'SUCCEEDED':
                    raise Exception(f"Query failed with status: {status}")
                
                results = hook.get_query_results(query_execution_id=query_execution_id)
                
                # Trích xuất giá trị bad_count
                rows = results.get('ResultSet', {}).get('Rows', [])
                if len(rows) > 1:
                    bad_records_count = int(rows[1]['Data'][0]['VarCharValue'])
                    
                    if bad_records_count > 0:
                        error_msg = f"❌ {check_name} FAILED: Found {bad_records_count} violating records."
                        self.log.error(error_msg)
                        failed_checks.append(error_msg)
                    else:
                        self.log.info(f"✅ {check_name} PASSED.")
            except Exception as e:
                self.log.error(f"Error executing {check_name}: {str(e)}")
                failed_checks.append(f"Execution error in {check_name}")

        # Fail task nếu có lỗi
        if failed_checks:
            self.log.error("🚨 Data Quality Checks Failed!")
            raise AirflowException(" | ".join(failed_checks))
        
        self.log.info("🎉 All Data Quality checks passed successfully.")
