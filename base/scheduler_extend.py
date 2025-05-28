def sync_database_data(phase):
    import pyodbc
    import json
    import os

    if phase not in ["p1", "p2"]:
        raise ValueError("Invalid phase. Use 'p1' or 'p2'.")

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

    proc_name = f"[dbo].[p_sfdc_replicate_to_t_glabshr_base_final_{phase}_v3_from_glabshr_base_final_{phase}_v3_2025-05-28]"
    table_name = f"GocturMainETL_HR.dbo.t_glabshr_base_final_{phase}_v3"

    cursor.execute(f"EXEC {proc_name}")
    conn.commit()
    print(f"✅ Stored procedure {proc_name} executed.")

    cursor.execute(f"""
        SELECT sort, EUID AS id, json_data
        FROM {table_name}
        WHERE [isProcessed] = 0
        ORDER BY sort, id
    """)
    records = cursor.fetchall()

    data = []
    order_ids = []

    for record in records:
        try:
            json_data = json.loads(record.json_data)
            data.append((record.sort, json_data))
            order_ids.append(record.id)
        except Exception as e:
            raise Exception(f"❌ Error parsing JSON for ID={record.id}: {e}")

    cursor.close()
    conn.close()

    return data, order_ids


def sync_phase_data(phase):
    from django.apps import apps
    from django.db import transaction
    from dateutil.parser import parse
    import pyodbc
    import os

    data, order_ids = sync_database_data(phase)
    print(f"Phase {phase.upper()} data retrieved: {len(data)} entries.")

    if not data:
        print(f"No new data to process for phase {phase.upper()}.")
        return

    sort_ordered_models = {}
    for sort, entry in data:
        model_path = entry["model"]
        if model_path not in sort_ordered_models:
            sort_ordered_models[model_path] = {"sort": sort, "entries": []}
        sort_ordered_models[model_path]["entries"].append(entry)

    sorted_models = sorted(sort_ordered_models.items(), key=lambda kv: kv[1]["sort"])

    for model_path, grouped in sorted_models:
        app_label, model_name = model_path.split(".")
        model = apps.get_model(app_label, model_name)
        print(f"🔄 Processing model: {model_path}")

        index = 0
        for entry in grouped["entries"]:
            index += 1
            if index % 100 == 0:
                print(f"Processed {index} entries for {model_path} {entry['pk']}")

            pk = entry["pk"]
            fields = entry["fields"].copy()

            # Parse datetime strings
            for field in model._meta.get_fields():
                if field.name in fields:
                    if field.get_internal_type() in ("DateField", "DateTimeField") and isinstance(fields[field.name], str):
                        try:
                            fields[field.name] = parse(fields[field.name])
                        except Exception as e:
                            print(f"❌ Date parse error on {field.name} in {model_path}, pk={pk}: {e}")
                            fields[field.name] = None

            # M2M
            m2m_fields = {
                f.name: fields.pop(f.name)
                for f in model._meta.get_fields()
                if f.many_to_many and f.name in fields
            }

            # FK
            for field in model._meta.get_fields():
                if field.is_relation and (field.many_to_one or field.one_to_one) and not field.auto_created:
                    if field.name in fields and isinstance(fields[field.name], int):
                        try:
                            related_model = field.related_model
                            fields[field.name] = related_model.objects.get(pk=fields[field.name])
                        except related_model.DoesNotExist:
                            print(f"⚠️ FK {field.name} with id={fields[field.name]} missing for {model_path}, pk={pk}")
                            fields[field.name] = None

            # try:
            if True:
                with transaction.atomic():
                    obj, created = model.objects.update_or_create(pk=pk, defaults=fields)
                    for field_name, ids in m2m_fields.items():
                        getattr(obj, field_name).set(ids)
            # except Exception as e:
            #     print(f"❌ Error saving {model_path} pk={pk}: {e}")

    # Update processed rows
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={os.getenv("SOURCE_MSSQL_SERVER")};'
        f'DATABASE={os.getenv("SOURCE_MSSQL_DATABASE")};'
        f'UID={os.getenv("SOURCE_MSSQL_USERNAME")};'
        f'PWD={os.getenv("SOURCE_MSSQL_PASSWORD")}'
    )
    cursor = conn.cursor()

    table_name = f"GocturMainETL_HR.dbo.t_glabshr_base_final_{phase}_v3"
    for order_id in order_ids:
        cursor.execute(f"""
            UPDATE {table_name}
            SET isProcessed = 1
            WHERE EUID = ?
        """, order_id)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ Phase {phase.upper()} marked {len(order_ids)} rows as processed.")

def scheduled_sync_task_bulk():
    print("🔄 Starting bulk sync task...")
    for phase in ["p1", "p2"]:
        print(f"🚀 Starting sync for phase {phase.upper()}...")
        sync_phase_data(phase)
        print(f"✅ Completed sync for phase {phase.upper()}")
