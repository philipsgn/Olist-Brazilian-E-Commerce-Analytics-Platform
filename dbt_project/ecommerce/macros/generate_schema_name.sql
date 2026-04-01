{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
       Macro này ghi đè logic mặc định của dbt.
       Nếu model có định nghĩa 'schema' (staging/marts), dbt sẽ lấy chính xác tên đó.
       Nếu không có, dbt sẽ dùng schema mặc định trong profile (public).
    #}
    
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}  {# -- CHANGED: Trả về chính xác tên staging/marts mà không có prefix #}
    {%- endif -%}

{%- endmacro %}
