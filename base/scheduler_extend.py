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
        SELECT json_data FROM GocturLabsHR.glabshr_base_final
        """)
    records = cursor.fetchall()

    # Convert records to JSON format for loaddata
    for record in records:
        # Convert the JSON string to a Python dictionary
        try:
            json_data = json.loads(record.json_data)
        except Exception as e:
            raise Exception(f"Error parsing JSON data: {record.json_data}")
    data_base = [json.loads(record.json_data) for record in records]


    # cursor.execute("""
    #     SELECT json_data FROM GocturLabsHR.glabshr_employees_v2
    #     ORDER BY id asc""")
    # records = cursor.fetchall()

    # data_employee = [json.loads(record.json_data) for record in records]

    # cursor.execute("""
    #     SELECT json_data FROM GocturLabsHR.glabshr_attandance
    #     ORDER BY id asc""")
    # records = cursor.fetchall()

    # data_attandance = [json.loads(record.json_data) for record in records]

    # cursor.execute("""
    #     SELECT json_data FROM GocturLabsHR.glabshr_leave
    #     ORDER BY id asc""")
    # records = cursor.fetchall()

    # data_leave = [json.loads(record.json_data) for record in records]
    data = data_base # + data_employee + data_attandance + data_leave

    cursor.close()
    conn.close()

    return data

def scheduled_sync_task():
    from django.apps import apps
    from django.db import transaction

    print("Getting data from MS SQL...")
    data = sync_database_data()
    print("Data retrieved successfully.")

    for entry in data:
        # print(f"Processing entry: {entry['model']} {entry['pk']}")
        try:
            model_path = entry["model"]
            app_label, model_name = model_path.split(".")
            model = apps.get_model(app_label, model_name)

            pk = entry["pk"]
            fields = entry["fields"]

            # Handle many-to-many separately
            m2m_fields = {
                f.name: fields.pop(f.name)
                for f in model._meta.get_fields()
                if f.many_to_many and f.name in fields
            }

            # Convert foreign key IDs to instances
            for field in model._meta.get_fields():
                if field.is_relation and (field.many_to_one or field.one_to_one) and not field.auto_created:
                    if field.name in fields and isinstance(fields[field.name], int):
                        related_model = field.related_model
                        fields[field.name] = related_model.objects.get(pk=fields[field.name])

            with transaction.atomic():
                obj, created = model.objects.update_or_create(pk=pk, defaults=fields)
                for field_name, ids in m2m_fields.items():
                    getattr(obj, field_name).set(ids)

        except Exception as e:
            print(f"Error processing entry {entry['model']} {entry['pk']}: {e}")
            import time
            time.sleep(100000)

from django.apps import apps
from django.db import transaction

def scheduled_sync_task_bulk_dep(test_mode=True, max_per_model=1000000):
    print("⚡ FAST TEST MODE ENABLED" if test_mode else "Running full sync...")
    data = sync_database_data()
    print("Data retrieved.")

    ordered_models = []
    model_entries = {}

    for entry in data:
        model_path = entry["model"]
        if model_path not in model_entries:
            ordered_models.append(model_path)
            model_entries[model_path] = []
        model_entries[model_path].append(entry)

    m2m_buffer = []

    for model_path in ordered_models:
        print(f"🔄 Processing model: {model_path}")
        app_label, model_name = model_path.split(".")
        model = apps.get_model(app_label, model_name)

        entries = model_entries[model_path]
        if test_mode:
            entries = entries[:max_per_model]

        create_list = []
        update_list = []

        for entry in entries:
            print(f"Processing entry: {entry['model']} {entry['pk']}")
            pk = entry["pk"]
            fields = entry["fields"].copy()

            # --- Handle M2M separately ---
            m2m_fields = {}
            for field in model._meta.get_fields():
                if field.many_to_many and field.name in fields:
                    m2m_fields[field.name] = fields.pop(field.name)

            # --- Resolve FK fields ---
            for field in model._meta.get_fields():
                if field.is_relation and (field.many_to_one or field.one_to_one) and not field.auto_created:
                    if field.name in fields and isinstance(fields[field.name], int):
                        try:
                            related_model = field.related_model
                            fields[field.name] = related_model.objects.get(pk=fields[field.name])
                        except Exception as e:
                            print(f"⚠️ FK resolution failed for {model_path}.{field.name}={fields[field.name]}: {e}")
                            fields[field.name] = None  # optional fallback for testing

            # --- Create instance ---
            instance = model(pk=pk, **fields)

            if m2m_fields:
                m2m_buffer.append((instance, m2m_fields))

            # --- Choose create or update ---
            if model.objects.filter(pk=pk).exists():
                update_list.append(instance)
            else:
                create_list.append(instance)

        # --- Bulk create & update ---
        with transaction.atomic():
            if create_list:
                model.objects.bulk_create(create_list, batch_size=100)

            if update_list:
                update_fields = [
                    f.name for f in model._meta.fields
                    if f.name != model._meta.pk.name and not f.auto_created
                ]
                model.objects.bulk_update(update_list, update_fields, batch_size=100)

        print(f"✅ {model_path}: created {len(create_list)}, updated {len(update_list)}")

    # --- M2M assignment ---
    for instance, m2m_fields in m2m_buffer:
        for field_name, ids in m2m_fields.items():
            try:
                getattr(instance, field_name).set(ids)
            except Exception as e:
                print(f"❌ Error setting M2M field {field_name} on {instance}: {e}")

    print("🏁 Sync complete.")

def scheduled_sync_task_bulk():
    from django.apps import apps
    from django.db import transaction

    print("Getting data from MS SQL...")
    data = sync_database_data()
    print("Data retrieved successfully.")

    # Determine execution order from first appearance
    ordered_models = []
    model_entries = {}

    for entry in data:
        model_path = entry["model"]
        if model_path not in model_entries:
            ordered_models.append(model_path)
            model_entries[model_path] = []
        model_entries[model_path].append(entry)

    for model_path in ordered_models:
        app_label, model_name = model_path.split(".")
        model = apps.get_model(app_label, model_name)
        print(f"🔄 Processing model: {app_label}, {model_name}")
        print(model)

        index = 0
        for entry in model_entries[model_path]:
            index += 1
            if index % 100 == 0:
                print(f"Processed {index} entries for {model_path} {entry['pk']}")
            # try:
            if True:
                pk = entry["pk"]
                fields = entry["fields"].copy()

                # Handle many-to-many separately
                m2m_fields = {
                    f.name: fields.pop(f.name)
                    for f in model._meta.get_fields()
                    if f.many_to_many and f.name in fields
                }

                # Convert foreign key IDs to instances
                # for field in model._meta.get_fields():
                #     if field.is_relation and (field.many_to_one or field.one_to_one) and not field.auto_created:
                #         if field.name in fields and isinstance(fields[field.name], int):
                #             related_model = field.related_model
                #             fields[field.name] = related_model.objects.get(pk=fields[field.name])

                for field in model._meta.get_fields():
                    if field.is_relation and (field.many_to_one or field.one_to_one) and not field.auto_created:
                        if field.name in fields and isinstance(fields[field.name], int):
                            try:
                                related_model = field.related_model
                                fields[field.name] = related_model.objects.get(pk=fields[field.name])
                            except related_model.DoesNotExist:
                                print(f"⚠️ FK {field.name} with id={fields[field.name]} does not exist for {model_path} {pk}")
                                fields[field.name] = None  # or `continue` or `raise` depending on your policy

                with transaction.atomic():
                    obj, created = model.objects.update_or_create(pk=pk, defaults=fields)
                    for field_name, ids in m2m_fields.items():
                        getattr(obj, field_name).set(ids)

            # except Exception as e:
            #     print(f"❌ Error processing entry {entry['model']} {entry['pk']}: {e}")
            #     import time
            #     time.sleep(100000)

    print("✅ Sync complete.")
