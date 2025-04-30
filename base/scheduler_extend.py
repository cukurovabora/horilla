def sync_database_data():
    import pyodbc
    import json
    import os

    server = os.getenv('SOURCE_MSSQL_SERVER')
    database = os.getenv('SOURCE_MSSQL_DATABASE')
    username = os.getenv('SOURCE_MSSQL_USERNAME')
    password = os.getenv('SOURCE_MSSQL_PASSWORD')

    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password}'
    )
    cursor = conn.cursor()

    # Query data with JSON field
    cursor.execute("""
        SELECT json_data FROM GocturLabsHR.glabshr_base where json_data IS NOT NULL
        """)
    records = cursor.fetchall()

    # Convert records to JSON format for loaddata
    data_base = [json.loads(record.json_data) for record in records]

    cursor.execute("""
        SELECT json_data FROM GocturLabsHR.glabshr_employees
        WHERE json_data IS NOT NULL
        ORDER BY id asc, action_order asc""")
    records = cursor.fetchall()

    data_employee = [json.loads(record.json_data) for record in records]
    data = data_base + data_employee

    cursor.close()
    conn.close()

    return data

def scheduled_sync_task():
    from django.apps import apps

    print("Getting data from MS SQL...")
    data = sync_database_data()  # your function to fetch data
    print("Data retrieved successfully.")

    for entry in data:
        try:
            model_path = entry["model"]  # e.g. "base.jobposition"
            app_label, model_name = model_path.split(".")
            model = apps.get_model(app_label, model_name)

            pk = entry["pk"]
            fields = entry["fields"]

            # 1) Identify many-to-many fields and remove them
            m2m_fields = {}
            for field in model._meta.get_fields():
                if field.many_to_many and field.name in fields:
                    m2m_fields[field.name] = fields.pop(field.name)

            # 2) Identify foreign keys in the model and convert int->instance
            for field in model._meta.get_fields():
                # Check if it's a standard ForeignKey (many-to-one).
                if field.is_relation and (field.many_to_one or field.one_to_one)and not field.auto_created:
                    fk_name = field.name  # e.g. "department_id"
                    if fk_name in fields and isinstance(fields[fk_name], int):
                        related_model = field.related_model  # e.g. Department
                        fk_pk = fields[fk_name]             # the integer, e.g. 236

                        # Convert that integer PK into the related model instance
                        fields[fk_name] = related_model.objects.get(pk=fk_pk)

            # 3) update_or_create the object (no M2M fields here)
            obj, created = model.objects.update_or_create(pk=pk, defaults=fields)

            # 4) Assign M2M fields via .set(...)
            for field_name, ids in m2m_fields.items():
                getattr(obj, field_name).set(ids)
        except Exception as e:
            print(f"Error processing entry {entry}: {e}")
            print(entry)
            print("\n\n")
    raise Exception("Test exception")