import psycopg2
import os
import sys

os.chdir("db/migrations")
conn = None


def execute_sql(sql):
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()


def get_schema_version():
    execute_sql("SAVEPOINT schema;")
    try:
        command = """
    SELECT schema_version.version FROM schema_version;
    """
        cur = conn.cursor()
        cur.execute(command)
        result = cur.fetchone()
        cur.close()
        return result[0]
    except psycopg2.errors.UndefinedTable as e:
        execute_sql("ROLLBACK TO schema;")
        f = open("000000_schema_version.sql")
        i = f.read()
        execute_sql(i)
        return get_schema_version()


def update_schema_version(num):
    cur = conn.cursor()
    command = """
  UPDATE schema_version SET version = %s;
  """
    cur.execute(command, (num,))
    cur.close()


def pad_number(num):
    num = str(num)
    while len(num) < 6:
        num = "0" + num
    return num


def get_migration_filename(num, migration_type):
    num = pad_number(num)
    files = os.listdir()
    matched_files = []
    for filename in files:
        suffix = filename[filename.find(".") :]  # .up.sql
        prefix = filename.split("_")[0]  # 000050
        if num in prefix and migration_type in suffix:
            matched_files.append(filename)

    if len(matched_files) != 1:
        raise Exception("invalid files: " + str(matched_files))
    return matched_files[0]


def get_max_migration_num():
    files = os.listdir()
    files = sorted(files)
    last_file = files[-1]
    migration_number_str = last_file.split("_")[0]
    return int(migration_number_str)


def get_migration_sql(num, migration_type):
    filename = get_migration_filename(num, migration_type)
    f = open(filename)
    contents = f.read()
    f.close()
    return contents


def get_up_migration(num):
    return get_migration_sql(num, "up")


def get_down_migration(num):
    return get_migration_sql(num, "down")


def clear_db():
    current_migration = get_schema_version()
    while current_migration > 0:
        sql = get_down_migration(current_migration)
        try:
            execute_sql(sql)
        except Exception as e:
            print(sql)
            print(e)
            exit(1)
        current_migration -= 1
    update_schema_version(current_migration)
    conn.commit()

def undo_current_migration():
    current_migration = get_schema_version()
    sql = get_down_migration(current_migration)
    try:
        execute_sql(sql)
    except Exception as e:
        print(sql)
        print(e)
        exit(1)
    update_schema_version(current_migration - 1)
    conn.commit()


def run_db():
    current_migration = get_schema_version()
    max_migration_num = get_max_migration_num()
    while current_migration < max_migration_num:
        sql = get_up_migration(current_migration + 1)
        try:
            execute_sql(sql)
        except Exception as e:
            print(sql)
            print(e)
            exit(1)
        current_migration += 1
    update_schema_version(current_migration)
    conn.commit()


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 2:
        print(args)
        print("no command")
        exit(1)
    command = args[1]
    db = args[2]
    conn = psycopg2.connect(
        host="localhost", database=db, user="postgres", password="postgres", port="5441"
    )
    if command == "up":
        run_db()
        print("db updated")
    elif command == "down":
        undo_current_migration()
        print("db rolled back to revision", get_schema_version())
    else:
        print("unknown command", command)
