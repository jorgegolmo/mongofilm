# create_user.py
from pymongo import MongoClient

client = MongoClient("mongodb://127.0.0.1:27017/")
admin_db = client.admin

username = "myuser"
password = "MyS3cretPwd"

try:
    admin_db.command("createUser", username,
                     pwd=password,
                     roles=[{"role":"userAdminAnyDatabase","db":"admin"},
                            {"role":"readWriteAnyDatabase","db":"admin"}])
    print("User created:", username)
except Exception as e:
    print("Error creating user:", e)
finally:
    client.close()
