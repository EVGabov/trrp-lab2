# import
from Cryptodome.Cipher import PKCS1_v1_5
from socket import *
from Cryptodome.PublicKey import RSA
from Cryptodome.Cipher import PKCS1_OAEP
import datetime
from Cryptodome.Cipher import DES
import pickle
import pyodbc
from configparser import ConfigParser
import hashlib


def check_data():
    configur = ConfigParser()
    configur.read('imp_config.ini')

    server_name = configur.get('database', 'server_name')
    database_name = configur.get('database', 'database_name')
    connection_string = 'DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + database_name
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    response = """
    select sum(summ)+sum(life_time_m) from credits as a1 
    left join clients as a2 on a1.clients_id=a2.id
    left join cr_type as a3 on a1.cr_type_id=a3.id
    left join employers as a4 on a1.employers_id=a4.id
    left join office as a5 on a4.office_id=a5.id
    """
    chek = 0
    cursor.execute(response)
    for r in cursor:
        chek = r[0]
        print('Проверочная сумма =' + str(r[0]))
    connection.commit()
    connection.close()
    return chek


def connect():
    serv_sock.bind((ip, int(port)))
    print('Подключение произведено')
    serv_sock.listen(1)
    client_conn, client_addr = serv_sock.accept()
    print('Соединение:', client_addr)
    serv_rsa_key = RSA.generate(1024)
    exported_key = serv_rsa_key.publickey()
    exported_key = exported_key.export_key()
    print('Открытый ключ (md5 хеш): ' + str(hashlib.md5(exported_key).hexdigest()))
    data_msg = pickle.dumps(exported_key)
    client_conn.send(data_msg)
    print('Открытый ключ отправлен')
    return client_conn, serv_rsa_key


def recv_key(client_conn, serv_rsa_key):
    data_msg = client_conn.recv(100000)
    enc_msg = pickle.loads(data_msg)
    print('Ключ симметричного шифрования получен:' + str(enc_msg))
    serv_rsa_cipher = PKCS1_OAEP.new(serv_rsa_key)
    des_key = serv_rsa_cipher.decrypt(enc_msg)
    print('Ключ (md5 хеш):' + str(hashlib.md5(des_key).hexdigest()))
    msg = 'Y'
    data_msg = pickle.dumps(msg.encode('utf-8'))
    client_conn.send(data_msg)
    serv_des_cipher = DES.new(des_key, DES.MODE_ECB)
    return serv_des_cipher


def load_data(serv_des_cipher, client_conn):
    server_name = configur.get('database', 'server_name')
    database_name = configur.get('database', 'database_name')
    connection_string = 'DRIVER={SQL Server};SERVER=' + server_name + ';DATABASE=' + database_name
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    load_directory(cursor, client_conn, serv_des_cipher)
    load_directory(cursor, client_conn, serv_des_cipher)
    load_directory(cursor, client_conn, serv_des_cipher)
    load_directory_2(cursor, client_conn, serv_des_cipher)
    load_directory_3(cursor, client_conn, serv_des_cipher)

    connection.commit()
    connection.close()

    chek_v = check_data()
    data_msg = client_conn.recv(100000)
    enc_msg = pickle.loads(data_msg)
    print('Получено зашифрованное сообщение: ' + str(enc_msg))
    dec_msg = serv_des_cipher.decrypt(enc_msg)
    data_ = str(dec_msg.decode('utf-8')).strip()
    print('Сообщение расшифровано: ' + str(dec_msg.decode('utf-8')))
    if (int(data_) == int(chek_v)):
        print('Импорт завершен успешно')
    else:
        print('Возникла ошибка')
    msg = 'Y'
    data_msg = pickle.dumps(msg.encode('utf-8'))
    client_conn.send(data_msg)


def load_directory(cursor, client_conn, serv_des_cipher):
    data_msg = client_conn.recv(100000)
    enc_msg = pickle.loads(data_msg)
    print('Получено зашифрованное сообщение: ' + str(enc_msg))
    dec_msg = serv_des_cipher.decrypt(enc_msg)
    data_ = str(dec_msg.decode('utf-8')).strip()
    print('Сообщение расшифровано: ' + str(dec_msg.decode('utf-8')))
    cursor.execute(data_)
    msg = 'Y'
    data_msg = pickle.dumps(msg.encode('utf-8'))
    client_conn.send(data_msg)


def load_directory_2(cursor, client_conn, serv_des_cipher):
    data_msg = client_conn.recv(100000)
    enc_msg = pickle.loads(data_msg)
    print('Получено зашифрованное сообщение: ' + str(enc_msg))
    dec_msg = serv_des_cipher.decrypt(enc_msg)
    data_ = str(dec_msg.decode('utf-8')).strip().split("|")
    print('Сообщение расшифровано: ' + str(dec_msg.decode('utf-8')))
    index = ""
    for i in range(len(data_) - 1):
        if (i % 2 == 0):
            print(data_[i])
            cursor.execute(str(data_[i]))
            index = cursor.fetchone()
        if (i % 2 == 1):
            cursor.execute(data_[i] + " , " + str(index[0]) + ")")
    msg = 'Countries has been caugth'
    data_msg = pickle.dumps(msg.encode('utf-8'))
    client_conn.send(data_msg)


def load_directory_3(cursor, client_conn, serv_des_cipher):
    data_msg = client_conn.recv(100000)
    enc_msg = pickle.loads(data_msg)
    print('Получено зашифрованное сообщение: ' + str(enc_msg))
    dec_msg = serv_des_cipher.decrypt(enc_msg)
    data_ = str(dec_msg.decode('utf-8')).strip().split("|")
    print('Сообщение расшифровано: ' + str(dec_msg.decode('utf-8')))
    index = ""
    index1 = ""
    index2 = ""
    for i in range(len(data_) - 1):
        if (i % 4 == 0):
            cursor.execute(str(data_[i]))
            index = cursor.fetchone()
        if (i % 4 == 1):
            cursor.execute(str(data_[i]))
            index1 = cursor.fetchone()
        if (i % 4 == 2):
            cursor.execute(str(data_[i]))
            index2 = cursor.fetchone()
        if (i % 4 == 3):
            cursor.execute(data_[i] + " , " + str(index[0]) + " , " + str(index1[0]) + " , " + str(index2[0]) + ")")
    msg = 'Y'
    data_msg = pickle.dumps(msg.encode('utf-8'))
    client_conn.send(data_msg)


configur = ConfigParser()
configur.read('imp_config.ini')
ip = configur.get('network', 'ip')
port = configur.get('network', 'port')
serv_sock = socket(AF_INET, SOCK_STREAM)
client_conn, serv_rsa_key = connect()
serv_des_cipher = recv_key(client_conn, serv_rsa_key)
load_data(serv_des_cipher, client_conn)
print('End')