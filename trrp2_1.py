# export
from socket import *
import time
import datetime
from Cryptodome.PublicKey import RSA
from Cryptodome.Cipher import PKCS1_OAEP
import random
from Cryptodome.Cipher import DES
import pickle
import sqlite3
from configparser import ConfigParser
import hashlib


def pad(text):
    while len(text) % 8 != 0:
        text += b' '
    return text


def check_data():
    configur = ConfigParser()
    configur.read('exp_config.ini')
    path = configur.get('database', 'path')
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    response = 'SELECT sum(Summ)+sum(life_time) FROM cred_inf;'
    rows = cursor.execute(response)
    chek = 0
    for r in rows:
        print(r[0])
        chek = r[0]
    connection.close()
    return chek


def connect():
    client_sock.connect((ip, int(port)))
    print('Подключение произведено')
    data_msg = client_sock.recv(100000)
    exported_key = pickle.loads(data_msg)
    rsa_public_key = RSA.import_key(exported_key)
    client_rsa_cipher = PKCS1_OAEP.new(rsa_public_key)
    print('Получен открытый ключ (md5 хеш): ' + str(hashlib.md5(rsa_public_key.export_key()).hexdigest()))
    return client_rsa_cipher


def send_key(client_rsa_cipher):
    des_key = ''
    for i in range(8):
        letter_num = random.randint(0, 25) + ord('a')
        des_key += chr(letter_num)
    print('Ключ симметричного шифрования (md5 хеш): ' + hashlib.md5(des_key.encode('utf-8')).hexdigest())
    enc_msg = client_rsa_cipher.encrypt(des_key.encode('utf-8'))
    print('Зашифрованный ключ симметричного шифрования: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Ключ симметричного шифрования отправлен')
    data_msg = client_sock.recv(100000)
    print('Y')
    client_des_cipher = DES.new(des_key.encode('utf-8'), DES.MODE_ECB)
    return client_des_cipher


def send_data(client_des_cipher):
    path = configur.get('database', 'path')
    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute("select distinct cr_type_Name , [limit] , Count_m from cred_inf")
    data = cursor.fetchall()
    msg = ''
    for row in data:
        msg += ("insert into cr_type values('" + str(row[0]) + "'," + str(row[1]) + "," + str(row[2]) + ")" + " ")
    print('Сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')
    #####

    cursor.execute("select distinct  office_Name from cred_inf")
    data = cursor.fetchall()
    msg = ''
    for row in data:
        msg += ("insert into office values('" + str(row[0]) + "')" + " ")
    print('Сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')
    #####
    cursor.execute(
        "select distinct client_SecondName,client_FirstName,client_MiddleName, client_BDate, EMP , client_Phone from cred_inf")
    data = cursor.fetchall()
    msg = ''
    for row in data:
        bdate = str(row[3]).split(".")[2] + str(row[3]).split(".")[1] + str(row[3]).split(".")[0]
        msg += ("insert into clients values('" + str(row[0]) + "','" + str(row[1]) + "','" + str(row[2]) + "','" + str(
            bdate) + "','" + str(row[4]) + "','" + str(row[5]) + "')" + " ")
    print('Сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')
    ####
    cursor.execute(
        "select distinct employ_SecondName,employ_FirstName,employ_MiddleName,employ_BDate,Positin,office_Name from cred_inf")
    data = cursor.fetchall()
    msg = ''
    for row in data:
        bdate = str(row[3]).split(".")[2] + str(row[3]).split(".")[1] + str(row[3]).split(".")[0]
        msg += "select top 1 id from office where Name='" + str(row[5]) + "'| "
        msg += "insert into employers values('" + str(row[0]) + "','" + str(row[1]) + "','" + str(row[2]) + "','" + str(
            bdate) + "','" + str(row[4]) + "' |"

    print('Сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')
    ####
    cursor.execute(
        "select  Summ , life_time, percent,datest,cr_type_Name , [limit] , Count_m , client_SecondName,client_FirstName,client_MiddleName, client_BDate, employ_SecondName,employ_FirstName,employ_MiddleName,employ_BDate from cred_inf")
    data = cursor.fetchall()
    msg = ''
    for row in data:
        bdate = str(row[3]).split(".")[2] + str(row[3]).split(".")[1] + str(row[3]).split(".")[0]
        bdate2 = str(row[10]).split(".")[2] + str(row[10]).split(".")[1] + str(row[10]).split(".")[0]
        bdate3 = str(row[14]).split(".")[2] + str(row[14]).split(".")[1] + str(row[14]).split(".")[0]
        datest = str(row[3]).split(".")[2] + str(row[3]).split(".")[1] + str(row[3]).split(".")[0]
        msg += "select top 1 id from cr_type where Name='" + str(row[4]) + "' and limit=" + str(
            row[5]) + " and Count_m=" + str(row[6]) + "|"
        msg += "select top 1 id from clients where SecondName='" + str(row[7]) + "' and FirstName='" + str(
            row[8]) + "' and  MiddleName='" + str(row[9]) + "' and BirthDate='" + bdate2 + "'|"
        msg += "select top 1 id from employers where SecondName='" + str(row[11]) + "' and FirstName='" + str(
            row[12]) + "' and  MiddleName='" + str(row[13]) + "' and BirthDate='" + bdate3 + "'|"
        msg += "insert into credits values(" + str(row[0]) + "," + str(row[1]) + ",'" + datest + "'," + str(
            row[2]) + "|"

    print('Сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')
    connection.close()
    ####
    chek_v = check_data()
    msg = str(chek_v)
    print('Проверочное сообщение: ' + msg)
    enc_msg = client_des_cipher.encrypt(pad(msg.encode('utf-8')))
    print('Зашифрованное сообщение: ' + str(enc_msg))
    data_msg = pickle.dumps(enc_msg)
    client_sock.send(data_msg)
    print('Зашифрованное сообщение отправлено')
    data_msg = client_sock.recv(100000)
    print('Y')


configur = ConfigParser()
configur.read('exp_config.ini')
ip = configur.get('network', 'ip')
port = configur.get('network', 'port')
client_sock = socket(AF_INET, SOCK_STREAM)
client_rsa_cipher = connect()
client_des_cipher = send_key(client_rsa_cipher)
send_data(client_des_cipher)
time.sleep(3)
print('End')